package local

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

// reconcileFindmnt mirrors the StagedCount fixture: v1 by its /dev/zvol symlink,
// v2 by its resolved /dev/zd0 device, plus v1's publish bind-mount (source = the
// staging dir, not the zvol), the root fs, and a foreign plugin's zvol.
const reconcileFindmnt = `/ /dev/vda1
/opt/nomad/staging/v1/rw-file-system /dev/zvol/tank/csi/v1
/opt/nomad/per-alloc/a/v1/rw /opt/nomad/staging/v1/rw-file-system
/opt/nomad/staging/v2/rw-file-system /dev/zd0
/opt/nomad/staging/other/rw-file-system /dev/zvol/tank/other-plugin/z9
`

func findmntNode(t *testing.T) *node {
	t.Helper()
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if c.Name == "findmnt" {
			return cexec.Output{Stdout: []byte(reconcileFindmnt)}, nil
		}
		return cexec.Output{}, nil
	}}
	n := newTestNode(fr)
	n.zvolDatasets = func() map[string]string {
		return map[string]string{
			"/dev/zvol/tank/csi/v1": "tank/csi/v1",
			"/dev/zd0":              "tank/csi/v2",
		}
	}
	return n
}

func idleRegistry(t *testing.T) *stats.Registry {
	t.Helper()
	r := stats.NewRegistry(stats.Config{
		Enabled: true, Interval: time.Hour, WalkInterval: time.Hour, StatfsTimeout: time.Second,
	}, "A", zap.NewNop())
	t.Cleanup(r.Close)
	return r
}

func TestStagedVolumes_ReconstructsSpecs(t *testing.T) {
	n := findmntNode(t)
	specs, err := n.stagedVolumes(context.Background())
	require.NoError(t, err)

	got := map[string]stats.TrackSpec{}
	for _, s := range specs {
		got[s.VolumeID] = s
	}
	require.Len(t, got, 2, "two of this plugin's staged fs volumes; exclude bind-mount, root fs, foreign zvol")

	v1 := externalID{Node: "A", Dataset: "tank/csi/v1"}.String()
	v2 := externalID{Node: "A", Dataset: "tank/csi/v2"}.String()
	require.Contains(t, got, v1)
	require.Contains(t, got, v2, "the resolved /dev/zd0 form maps back to its dataset")
	assert.Equal(t, "/opt/nomad/staging/v1/rw-file-system", got[v1].StagingPath)
	assert.Equal(t, "/opt/nomad/staging/v2/rw-file-system", got[v2].StagingPath)
	assert.Equal(t, stats.AccessMount, got[v1].AccessType)
}

func TestStatsReconciler_RehydratesRegistryAcrossRestart(t *testing.T) {
	n := findmntNode(t)
	// A fresh (post-restart) registry: empty, and Nomad has NOT re-issued Stage.
	reg := idleRegistry(t)
	n.stats = reg
	sr := &statsReconciler{nd: n, reg: reg, interval: time.Hour, log: zap.NewNop()}

	require.Empty(t, reg.Dump(), "registry starts empty (mirrors a plugin restart)")
	sr.sweep(context.Background())

	ids := map[string]bool{}
	for _, s := range reg.Dump() {
		ids[s.VolumeID] = true
	}
	require.Len(t, ids, 2, "both host-staged volumes rehydrated with no re-stage")
	assert.True(t, ids[externalID{Node: "A", Dataset: "tank/csi/v1"}.String()])
	assert.True(t, ids[externalID{Node: "A", Dataset: "tank/csi/v2"}.String()])
}

func TestStatsReconciler_AddOnlyOnEnumerationError(t *testing.T) {
	// findmnt hard-errors → the sweep must skip (add-only) and not evict a live
	// volume that a prior successful sweep tracked.
	boom := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if c.Name == "findmnt" {
			return cexec.Output{}, &cexec.Error{Name: "findmnt", ExitCode: 127}
		}
		return cexec.Output{}, nil
	}}
	n := newTestNode(boom)
	n.zvolDatasets = func() map[string]string { return nil }
	reg := idleRegistry(t)
	n.stats = reg
	const keep = "local/v1/A/tank/csi/keep"
	reg.Track(keep, "/s/keep", stats.AccessMount)

	sr := &statsReconciler{nd: n, reg: reg, interval: time.Hour, log: zap.NewNop()}
	sr.sweep(context.Background())

	_, ok := reg.Get(keep)
	assert.True(t, ok, "enumeration error must not evict live volumes")
}
