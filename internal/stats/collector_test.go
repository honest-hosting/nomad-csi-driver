package stats

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func fixedNow(base time.Time) func() time.Time { return func() time.Time { return base } }

func TestCollector_PerVolumeGauges(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	items := []PublicVolumeStats{{
		ID: "local-data", Namespace: "default", Node: "nodeA", AccessType: AccessMount,
		TotalBytes: 100, UsedBytes: 40, AvailableBytes: 60,
		TotalInodes: 10, UsedInodes: 4, FreeInodes: 6,
		StatfsAt:  base.Add(-30 * time.Second), // age 30s
		FileCount: 7, DirCount: 3, OtherCount: 1,
		WalkComplete: true, WalkAt: base.Add(-10 * time.Second),
	}}
	c := newCollector(func() []PublicVolumeStats { return items }, true, 5*time.Second, fixedNow(base))

	expected := `
# HELP nomad_csi_volume_used_bytes Filesystem used bytes.
# TYPE nomad_csi_volume_used_bytes gauge
nomad_csi_volume_used_bytes{id="local-data",namespace="default",node="nodeA"} 40
# HELP nomad_csi_volume_files File count (tree walk).
# TYPE nomad_csi_volume_files gauge
nomad_csi_volume_files{id="local-data",namespace="default",node="nodeA"} 7
# HELP nomad_csi_volume_statfs_age_seconds Seconds since the last successful statfs.
# TYPE nomad_csi_volume_statfs_age_seconds gauge
nomad_csi_volume_statfs_age_seconds{id="local-data",namespace="default",node="nodeA"} 30
# HELP nomad_csi_volume_stale 1 if the reading is older than stale_after, else 0.
# TYPE nomad_csi_volume_stale gauge
nomad_csi_volume_stale{id="local-data",namespace="default",node="nodeA"} 1
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(expected),
		"nomad_csi_volume_used_bytes", "nomad_csi_volume_files",
		"nomad_csi_volume_statfs_age_seconds", "nomad_csi_volume_stale"); err != nil {
		t.Fatal(err)
	}
}

func TestCollector_PartialAndBlockSuppressed(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	items := []PublicVolumeStats{
		// walk not complete → no file/dir gauges
		{ID: "partial", Namespace: "default", Node: "n", AccessType: AccessMount, UsedBytes: 5, StatfsAt: base, WalkComplete: false},
		// block volume → entirely suppressed
		{ID: "blk", Namespace: "default", Node: "n", AccessType: AccessBlock, StatfsAt: base},
	}
	c := newCollector(func() []PublicVolumeStats { return items }, true, 0, fixedNow(base))

	if n := testutil.CollectAndCount(c, "nomad_csi_volume_files"); n != 0 {
		t.Fatalf("partial/block should emit no file gauges; got %d", n)
	}
	if n := testutil.CollectAndCount(c, "nomad_csi_volume_used_bytes"); n != 1 {
		t.Fatalf("used_bytes series = %d; want 1 (partial only)", n)
	}
}

func TestCollector_AggregateMode(t *testing.T) {
	base := time.Unix(1_700_000_000, 0)
	items := []PublicVolumeStats{
		{ID: "a", Namespace: "default", AccessType: AccessMount, UsedBytes: 10, TotalBytes: 100, StatfsAt: base},
		{ID: "b", Namespace: "default", AccessType: AccessMount, UsedBytes: 20, TotalBytes: 200, StatfsAt: base},
		{ID: "blk", Namespace: "default", AccessType: AccessBlock, StatfsAt: base}, // excluded
	}
	c := newCollector(func() []PublicVolumeStats { return items }, false, 0, fixedNow(base))

	expected := `
# HELP nomad_csi_volume_count Number of measured volumes.
# TYPE nomad_csi_volume_count gauge
nomad_csi_volume_count 2
# HELP nomad_csi_volume_used_bytes_sum Sum of used bytes across measured volumes.
# TYPE nomad_csi_volume_used_bytes_sum gauge
nomad_csi_volume_used_bytes_sum 30
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(expected),
		"nomad_csi_volume_count", "nomad_csi_volume_used_bytes_sum"); err != nil {
		t.Fatal(err)
	}
	if n := testutil.CollectAndCount(c, "nomad_csi_volume_total_bytes"); n != 0 {
		t.Fatalf("aggregate mode should not emit per-volume series; got %d", n)
	}
}

func TestCollector_RegisterNilSafe(t *testing.T) {
	if err := RegisterCollector(nil, func() []PublicVolumeStats { return nil }, true, 0); err != nil {
		t.Fatalf("nil registry: %v", err)
	}
	reg := prometheus.NewRegistry()
	if err := RegisterCollector(reg, nil, true, 0); err != nil {
		t.Fatalf("nil provider: %v", err)
	}
}
