package qnap

import (
	"context"
	"strings"
	"testing"
	"time"

	goqnap "github.com/honest-hosting/go-qnap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
	"github.com/honest-hosting/nomad-csi-driver/internal/iscsi"
	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

func idleStatsRegistry(t *testing.T) *stats.Registry {
	t.Helper()
	r := stats.NewRegistry(stats.Config{
		Enabled: true, Interval: time.Hour, WalkInterval: time.Hour, StatfsTimeout: time.Second,
	}, "n1", zap.NewNop())
	t.Cleanup(r.Close)
	return r
}

func TestValidateNodeConfig(t *testing.T) {
	require.Error(t, validateNodeConfig(&config.QNAPConfig{}), "no creds must be refused")
	require.Error(t, validateNodeConfig(&config.QNAPConfig{BaseURL: "http://san"}), "no user/pass must be refused")
	require.Error(t, validateNodeConfig(&config.QNAPConfig{BaseURL: "http://san", Username: "u"}), "no password must be refused")
	require.NoError(t, validateNodeConfig(&config.QNAPConfig{BaseURL: "http://san", Username: "u", Password: "p"}))
}

// TestStagedSessions_MultipathAndBlock proves the host enumeration de-dups a
// multipathed fs volume to one entry (mapper device → staging mount), and reports
// a session with no mount as a presence-only block volume.
func TestStagedSessions_MultipathAndBlock(t *testing.T) {
	const sess = `Target: iqn.qnap:vol-a (non-flash)
	Current Portal: 10.0.0.1:3260,1
		Attached SCSI devices:
		scsi5 Channel 00 Id 0 Lun: 0
			Attached scsi disk sdg		State: running
	Current Portal: 10.0.1.1:3260,1
		Attached SCSI devices:
		scsi6 Channel 00 Id 0 Lun: 0
			Attached scsi disk sdh		State: running
Target: iqn.qnap:vol-b (non-flash)
	Current Portal: 10.0.0.1:3260,1
		Attached SCSI devices:
		scsi7 Channel 00 Id 0 Lun: 0
			Attached scsi disk sdi		State: running
`
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		joined := c.Name + " " + strings.Join(c.Args, " ")
		switch {
		case c.Name == "iscsiadm" && len(c.Args) >= 2 && c.Args[1] == "session":
			return cexec.Output{Stdout: []byte(sess)}, nil
		case c.Name == "multipathd" && strings.Contains(joined, "show paths"):
			// vol-a's two paths map to one WWID; vol-b's sdi is not multipathed.
			return cexec.Output{Stdout: []byte("sdg 3600aaa\nsdh 3600aaa\n")}, nil
		case c.Name == "findmnt" && strings.Contains(joined, "TARGET,SOURCE"):
			return cexec.Output{Stdout: []byte("/opt/nomad/staging/vol-a/rw /dev/mapper/3600aaa\n")}, nil
		}
		return cexec.Output{}, nil
	}}
	n := newTestNode(fr, newMemMetaStore())
	n.cfg = &config.QNAPConfig{} // no portals → all sessions count

	sessions, err := n.stagedSessions(context.Background())
	require.NoError(t, err)
	byIQN := map[string]stagedSession{}
	for _, s := range sessions {
		byIQN[s.IQN] = s
	}
	require.Len(t, byIQN, 2, "multipathed vol-a de-dups to one; vol-b is separate")

	a := byIQN["iqn.qnap:vol-a"]
	assert.Equal(t, 0, a.LUN)
	assert.Equal(t, stats.AccessMount, a.AccessType)
	assert.Equal(t, "/opt/nomad/staging/vol-a/rw", a.StagingPath, "mapper device → staging mount target")

	b := byIQN["iqn.qnap:vol-b"]
	assert.Equal(t, stats.AccessBlock, b.AccessType, "a session with no mount is a presence-only block volume")
	assert.Empty(t, b.StagingPath)
}

// reconcilerNode builds a node whose iSCSI/mount enumeration is faked and whose SAN
// identity cache is backed by a counting fake client (single 1:1 vol-a at LUN 0).
func reconcilerNode(t *testing.T, now *time.Time) (*node, *countingClient) {
	t.Helper()
	const sess = `Target: iqn.qnap:vol-a (non-flash)
	Current Portal: 10.0.0.1:3260,1
		Attached SCSI devices:
		scsi5 Channel 00 Id 0 Lun: 0
			Attached scsi disk sda		State: running
`
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		joined := c.Name + " " + strings.Join(c.Args, " ")
		switch {
		case c.Name == "iscsiadm" && len(c.Args) >= 2 && c.Args[1] == "session":
			return cexec.Output{Stdout: []byte(sess)}, nil
		case c.Name == "findmnt" && strings.Contains(joined, "TARGET,SOURCE"):
			return cexec.Output{Stdout: []byte("/opt/nomad/staging/vol-a /dev/sda\n")}, nil
		}
		return cexec.Output{}, nil
	}}
	fc := newFakeClient()
	fc.targets[0] = goqnap.Target{Index: 0, IQN: "iqn.qnap:vol-a", Alias: "vol-a", LUNs: []int{42}}
	fc.luns[42] = goqnap.LUN{Index: 42, Name: "vol-a"}
	cc := &countingClient{Client: fc}
	c := newSANIdentityCache(cc, newSessionManager(cc, "u", "p"), 30*time.Second, zap.NewNop())
	c.nowFn = func() time.Time { return *now }

	n := newTestNode(fr, newMemMetaStore())
	n.cfg = &config.QNAPConfig{}
	n.useMultipath = false // vol-a is a single raw path here; multipath join covered elsewhere
	n.san = c
	return n, cc
}

func TestStatsReconciler_RehydratesAndMemoizes(t *testing.T) {
	now := time.Unix(1000, 0)
	n, cc := reconcilerNode(t, &now)
	reg := idleStatsRegistry(t)
	sr := newStatsReconciler(n, reg, time.Hour)

	// First sweep after a restart: empty registry → rehydrate from the session,
	// resolving the external id via the SAN (one coalesced hydration).
	require.Empty(t, reg.Dump())
	sr.sweep(context.Background())
	s, ok := reg.Get("qnap/v1/42/0/t/vol-a")
	require.True(t, ok, "volume rehydrated with no re-stage")
	assert.Equal(t, stats.AccessMount, s.AccessType)
	require.Equal(t, 1, cc.listTargets, "one SAN hydration on the cold sweep")

	// Advance well past the SAN cache TTL. A second sweep must NOT touch the SAN —
	// the reconciler memoizes (IQN,LUN)→external id for the process lifetime, so
	// steady-state sweeps are host-only (cold-only SAN posture).
	now = time.Unix(9000, 0)
	sr.sweep(context.Background())
	assert.Equal(t, 1, cc.listTargets, "steady-state sweep must make zero SAN calls (memoized)")
	_, ok = reg.Get("qnap/v1/42/0/t/vol-a")
	assert.True(t, ok, "still tracked after the second sweep")
}

func TestStatsReconciler_AddOnlyWhenSANUnresolved(t *testing.T) {
	now := time.Unix(1000, 0)
	n, _ := reconcilerNode(t, &now)
	// Point the only session at an IQN the SAN doesn't know → resolution fails.
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if c.Name == "iscsiadm" && len(c.Args) >= 2 && c.Args[1] == "session" {
			return cexec.Output{Stdout: []byte("Target: iqn.qnap:unknown (non-flash)\n" +
				"\tCurrent Portal: 10.0.0.1:3260,1\n\t\tAttached SCSI devices:\n" +
				"\t\tscsi5 Channel 00 Id 0 Lun: 0\n\t\t\tAttached scsi disk sda\t\tState: running\n")}, nil
		}
		return cexec.Output{}, nil
	}}
	n.iscsi = iscsi.New(fr)
	n.mounter = mountutil.New(fr, zap.NewNop())

	reg := idleStatsRegistry(t)
	const seeded = "qnap/v1/99/9/t/seeded"
	reg.Track(seeded, "/s/seeded", stats.AccessMount)
	sr := newStatsReconciler(n, reg, time.Hour)

	sr.sweep(context.Background())
	_, ok := reg.Get(seeded)
	assert.True(t, ok, "an unresolved session leaves desired incomplete → add-only, no eviction")
}
