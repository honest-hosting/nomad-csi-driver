package iscsi

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
)

func TestByPath(t *testing.T) {
	assert.Equal(t,
		"/dev/disk/by-path/ip-10.0.0.1:3260-iscsi-iqn.test:tgt-lun-0",
		ByPath("10.0.0.1:3260", "iqn.test:tgt", 0))
}

func TestDeviceGlob(t *testing.T) {
	// Portal-agnostic: matches by IQN + LUN, so a real udev link named with the
	// portal IP (ip-172.16.46.69:3260-...) matches regardless of the configured
	// portal being a hostname.
	pat := DeviceGlob("iqn.2004-04.com.qnap:qutscloud:iscsi.dummy-qnap.ba9bcc", 0)
	assert.Equal(t,
		"/dev/disk/by-path/*-iscsi-iqn.2004-04.com.qnap:qutscloud:iscsi.dummy-qnap.ba9bcc-lun-0",
		pat)
	ok, err := filepath.Match(pat, "/dev/disk/by-path/ip-172.16.46.69:3260-iscsi-iqn.2004-04.com.qnap:qutscloud:iscsi.dummy-qnap.ba9bcc-lun-0")
	require.NoError(t, err)
	assert.True(t, ok, "udev IP-named by-path link must match the glob")
}

func TestConnectorCommands(t *testing.T) {
	fr := &cexec.FakeRunner{}
	c := New(fr)
	ctx := context.Background()

	require.NoError(t, c.Discover(ctx, "10.0.0.1:3260"))
	require.NoError(t, c.Login(ctx, "10.0.0.1:3260", "iqn.test:tgt"))
	require.NoError(t, c.Rescan(ctx, "10.0.0.1:3260", "iqn.test:tgt"))
	require.NoError(t, c.Logout(ctx, "10.0.0.1:3260", "iqn.test:tgt"))

	joined := strings.Join(fr.Commands(), "\n")
	assert.Contains(t, joined, "iscsiadm -m discovery -t sendtargets -p 10.0.0.1:3260")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.test:tgt -p 10.0.0.1:3260 --login")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.test:tgt -p 10.0.0.1:3260 -R")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.test:tgt -p 10.0.0.1:3260 --logout")
}

func TestLoginIdempotent(t *testing.T) {
	// Exit code 15 ("already exists") must be treated as success.
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		return cexec.Output{}, &cexec.Error{Name: "iscsiadm", ExitCode: exitAlreadyExists}
	}}
	require.NoError(t, New(fr).Login(context.Background(), "p", "iqn"))
}

func TestLoginRealError(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		return cexec.Output{}, &cexec.Error{Name: "iscsiadm", ExitCode: 1, Stderr: "connection refused"}
	}}
	require.Error(t, New(fr).Login(context.Background(), "p", "iqn"))
}

// sessionP3Multipath is a realistic `iscsiadm -m session -P 3` capture: one
// target reached over two portals (multipath), LUN 0 on sda/sdb.
const sessionP3Multipath = `Target: iqn.2004-04.com.qnap:tvs-882:iscsi.vol-a.abc123 (non-flash)
	Current Portal: 172.16.46.69:3260,1
	Persistent Portal: 172.16.46.69:3260,1
		**********
		Interface:
		**********
		Iface Name: default
		Iface Transport: tcp
		SID: 1
		iSCSI Connection State: LOGGED IN
		iSCSI Session State: LOGGED_IN
		************************
		Attached SCSI devices:
		************************
		Host Number: 5	State: running
		scsi5 Channel 00 Id 0 Lun: 0
			Attached scsi disk sda		State: running
	Current Portal: 172.16.47.69:3260,1
	Persistent Portal: 172.16.47.69:3260,1
		**********
		Interface:
		**********
		Iface Name: default
		SID: 2
		iSCSI Session State: LOGGED_IN
		************************
		Attached SCSI devices:
		************************
		Host Number: 6	State: running
		scsi6 Channel 00 Id 0 Lun: 0
			Attached scsi disk sdb		State: running
`

// sessionP3TwoLUNsSharedTarget: a single portal, one target (IQN) exposing two
// LUNs — the 1:N shared-target shape the OQ2 refcount cares about.
const sessionP3TwoLUNsSharedTarget = `Target: iqn.2004-04.com.qnap:tvs-882:iscsi.shared.def456 (non-flash)
	Current Portal: 172.16.46.70:3260,1
		************************
		Attached SCSI devices:
		************************
		scsi7 Channel 00 Id 0 Lun: 0
			Attached scsi disk sdc		State: running
		scsi7 Channel 00 Id 0 Lun: 1
			Attached scsi disk sdd		State: running
`

func TestListSessionsMultipath(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		return cexec.Output{Stdout: []byte(sessionP3Multipath)}, nil
	}}
	sess, err := New(fr).ListSessions(context.Background())
	require.NoError(t, err)
	require.Len(t, sess, 2)

	iqn := "iqn.2004-04.com.qnap:tvs-882:iscsi.vol-a.abc123"
	assert.Equal(t, Session{IQN: iqn, Portal: "172.16.46.69:3260", LUN: 0, Device: "/dev/sda"}, sess[0])
	assert.Equal(t, Session{IQN: iqn, Portal: "172.16.47.69:3260", LUN: 0, Device: "/dev/sdb"}, sess[1])

	// De-dupe on (IQN, LUN): a multipathed volume must count once.
	uniq := map[string]struct{}{}
	for _, s := range sess {
		uniq[fmt.Sprintf("%s/%d", s.IQN, s.LUN)] = struct{}{}
	}
	assert.Len(t, uniq, 1, "multipath (IQN,LUN) collapses to one volume")

	assert.Contains(t, strings.Join(fr.Commands(), "\n"), "iscsiadm -m session -P 3")
}

func TestListSessionsSharedTargetTwoLUNs(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		return cexec.Output{Stdout: []byte(sessionP3TwoLUNsSharedTarget)}, nil
	}}
	sess, err := New(fr).ListSessions(context.Background())
	require.NoError(t, err)
	require.Len(t, sess, 2)
	assert.Equal(t, 0, sess[0].LUN)
	assert.Equal(t, 1, sess[1].LUN)
	assert.Equal(t, sess[0].IQN, sess[1].IQN, "both LUNs share one target IQN (1:N)")
	assert.Equal(t, "/dev/sdc", sess[0].Device)
	assert.Equal(t, "/dev/sdd", sess[1].Device)
}

func TestListSessionsNoneIsEmpty(t *testing.T) {
	// `iscsiadm -m session` exits 21 with no active sessions — not an error.
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		return cexec.Output{}, &cexec.Error{Name: "iscsiadm", ExitCode: exitNoObjectsFound, Stderr: "iscsiadm: No active sessions."}
	}}
	sess, err := New(fr).ListSessions(context.Background())
	require.NoError(t, err)
	assert.Empty(t, sess)
}

func TestListSessionsRealError(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		return cexec.Output{}, &cexec.Error{Name: "iscsiadm", ExitCode: 6, Stderr: "iscsid is not running"}
	}}
	_, err := New(fr).ListSessions(context.Background())
	require.Error(t, err)
}
