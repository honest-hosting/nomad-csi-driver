package iscsi

import (
	"context"
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
