package qnap

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
	"github.com/honest-hosting/nomad-csi-driver/internal/iscsi"
	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
	"github.com/honest-hosting/nomad-csi-driver/internal/multipath"
)

// reconRunner answers the reconciler's commands: session list, mount list,
// multipathd (device→wwid), and records logouts.
func reconRunner(sessions, mounts, paths string) *cexec.FakeRunner {
	return &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		switch {
		case c.Name == "iscsiadm" && len(c.Args) >= 2 && c.Args[1] == "session":
			return cexec.Output{Stdout: []byte(sessions)}, nil
		case c.Name == "findmnt":
			return cexec.Output{Stdout: []byte(mounts)}, nil
		case c.Name == "multipathd":
			return cexec.Output{Stdout: []byte(paths)}, nil
		}
		return cexec.Output{}, nil
	}}
}

func newTestReconciler(fr cexec.Runner, cached func() map[string]struct{}, ours ...string) *reconciler {
	return &reconciler{
		iscsi:        iscsi.New(fr),
		mounter:      mountutil.New(fr, zap.NewNop()),
		mpath:        multipath.New(fr, ""),
		useMultipath: true,
		ourPortals: func() map[string]struct{} {
			m := map[string]struct{}{}
			for _, p := range ours {
				m[normalizePortal(p)] = struct{}{}
			}
			return m
		},
		cachedIQNs:  cached,
		grace:       5 * time.Minute,
		resolve:     func(s string) (string, error) { return s, nil }, // no-op by default; overridden per-test
		log:         zap.NewNop(),
		orphanSince: map[string]time.Time{},
	}
}

func noCache() map[string]struct{} { return nil }

const orphanSession = `Target: iqn.q:orphan (non-flash)
	Current Portal: 10.0.0.1:3260,1
		Attached SCSI devices:
		scsi5 Channel 00 Id 0 Lun: 0
			Attached scsi disk sda		State: running
`

func TestReconcile_OrphanLoggedOutAfterGrace(t *testing.T) {
	fr := reconRunner(orphanSession, "", "sda somewwid\n") // nothing mounted → orphan
	r := newTestReconciler(fr, noCache, "10.0.0.1")
	now := time.Unix(1000, 0)
	r.nowFn = func() time.Time { return now }

	r.sweep(context.Background()) // first sighting: hold for grace, no logout
	assert.NotContains(t, strings.Join(fr.Commands(), "\n"), "--logout", "not logged out on first orphan sighting")

	now = now.Add(6 * time.Minute) // past grace
	r.sweep(context.Background())
	assert.Contains(t, strings.Join(fr.Commands(), "\n"),
		"iscsiadm -m node -T iqn.q:orphan -p 10.0.0.1:3260 --logout", "leaked target logged out after grace")
}

func TestReconcile_LiveMountedNeverLoggedOut(t *testing.T) {
	// The target's multipath mapper is mounted → live, even past grace.
	fr := reconRunner(orphanSession, "/stage /dev/mapper/somewwid\n", "sda somewwid\n")
	r := newTestReconciler(fr, noCache, "10.0.0.1")
	now := time.Unix(1000, 0)
	r.nowFn = func() time.Time { return now }

	r.sweep(context.Background())
	now = now.Add(6 * time.Minute)
	r.sweep(context.Background())
	assert.NotContains(t, strings.Join(fr.Commands(), "\n"), "--logout")
}

func TestReconcile_CachedIQNKeptLive(t *testing.T) {
	// The IQN is in the tier-1 cache (staged this lifetime) → live, no logout.
	cached := func() map[string]struct{} { return map[string]struct{}{"iqn.q:orphan": {}} }
	fr := reconRunner(orphanSession, "", "sda somewwid\n")
	r := newTestReconciler(fr, cached, "10.0.0.1")
	now := time.Unix(1000, 0)
	r.nowFn = func() time.Time { return now }

	r.sweep(context.Background())
	now = now.Add(6 * time.Minute)
	r.sweep(context.Background())
	assert.NotContains(t, strings.Join(fr.Commands(), "\n"), "--logout")
}

func TestReconcile_ForeignPortalIgnored(t *testing.T) {
	// Session belongs to another plugin's SAN (portal not ours) → never touched.
	const foreign = `Target: iqn.other:vol (non-flash)
	Current Portal: 192.168.9.9:3260,1
		Attached SCSI devices:
		scsi9 Channel 00 Id 0 Lun: 0
			Attached scsi disk sdz		State: running
`
	fr := reconRunner(foreign, "", "sdz otherwwid\n")
	r := newTestReconciler(fr, noCache, "10.0.0.1") // we own 10.0.0.1, not 192.168.9.9
	now := time.Unix(1000, 0)
	r.nowFn = func() time.Time { return now }

	r.sweep(context.Background())
	now = now.Add(6 * time.Minute)
	r.sweep(context.Background())
	assert.NotContains(t, strings.Join(fr.Commands(), "\n"), "--logout", "another plugin's session must never be logged out")
}

// Regression: raw findmnt reports the resolved /dev/dm-N, not /dev/mapper/<wwid>.
// A live multipath session must be recognized via symlink resolution and NOT
// logged out. Without the resolve step this test's session would be torn down —
// data loss on a running volume.
func TestReconcile_LiveMultipathResolvedDmName(t *testing.T) {
	fr := reconRunner(orphanSession, "/stage /dev/dm-0\n", "sda somewwid\n") // mount source is /dev/dm-0
	r := newTestReconciler(fr, noCache, "10.0.0.1")
	r.resolve = func(s string) (string, error) {
		if s == "/dev/mapper/somewwid" {
			return "/dev/dm-0", nil // the real EvalSymlinks would resolve the mapper symlink here
		}
		return s, nil
	}
	now := time.Unix(1000, 0)
	r.nowFn = func() time.Time { return now }

	r.sweep(context.Background())
	now = now.Add(6 * time.Minute)
	r.sweep(context.Background())
	assert.NotContains(t, strings.Join(fr.Commands(), "\n"), "--logout",
		"live multipath session whose mount source is /dev/dm-N must not be logged out")
}

func TestReconcile_OrphanBecomesLiveResetsTimer(t *testing.T) {
	// Orphan at first sweep, then mounted before grace → timer cleared, no logout.
	r := newTestReconciler(reconRunner(orphanSession, "", "sda somewwid\n"), noCache, "10.0.0.1")
	now := time.Unix(1000, 0)
	r.nowFn = func() time.Time { return now }
	r.sweep(context.Background()) // records orphanSince

	// Now it's mounted (published) and well past grace.
	r.iscsi = iscsi.New(reconRunner(orphanSession, "/stage /dev/mapper/somewwid\n", "sda somewwid\n"))
	r.mounter = mountutil.New(reconRunner(orphanSession, "/stage /dev/mapper/somewwid\n", "sda somewwid\n"), zap.NewNop())
	r.mpath = multipath.New(reconRunner(orphanSession, "/stage /dev/mapper/somewwid\n", "sda somewwid\n"), "")
	now = now.Add(10 * time.Minute)
	r.sweep(context.Background())
	assert.Empty(t, r.orphanSince, "timer cleared once the target is live again")
}

func TestReconcileEnabled_DefaultOff(t *testing.T) {
	assert.False(t, reconcileEnabled(&config.QNAPConfig{}), "reconciler is off by default")
	on := true
	assert.True(t, reconcileEnabled(&config.QNAPConfig{ReconcileEnabled: &on}))
	off := false
	assert.False(t, reconcileEnabled(&config.QNAPConfig{ReconcileEnabled: &off}))
}

func TestReconcileTimings_DefaultsAndOverrides(t *testing.T) {
	i, g := reconcileTimings(&config.QNAPConfig{}, zap.NewNop())
	assert.Equal(t, defaultReconcileInterval, i)
	assert.Equal(t, defaultReconcileGrace, g)

	i, g = reconcileTimings(&config.QNAPConfig{ReconcileInterval: "30s", ReconcileGrace: "90s"}, zap.NewNop())
	assert.Equal(t, 30*time.Second, i)
	assert.Equal(t, 90*time.Second, g)

	// Invalid values fall back to the defaults.
	i, g = reconcileTimings(&config.QNAPConfig{ReconcileInterval: "bogus", ReconcileGrace: ""}, zap.NewNop())
	assert.Equal(t, defaultReconcileInterval, i)
	assert.Equal(t, defaultReconcileGrace, g)
}
