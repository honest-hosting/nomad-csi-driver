package stats

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Per-volume gauge descriptors, labeled by the **Nomad volume id** + namespace +
// node (consistent with Nomad-scraped target metadata). This is a deliberate,
// documented exception to the driver's otherwise no-per-volume-label rule
// (bounded by volume count); set metrics_per_volume=false to collapse to the
// aggregate gauges below.
var (
	volLabels = []string{"id", "namespace", "node"}

	descTotalBytes = prometheus.NewDesc("nomad_csi_volume_total_bytes", "Filesystem size in bytes.", volLabels, nil)
	descUsedBytes  = prometheus.NewDesc("nomad_csi_volume_used_bytes", "Filesystem used bytes.", volLabels, nil)
	descAvailBytes = prometheus.NewDesc("nomad_csi_volume_available_bytes", "Filesystem available bytes.", volLabels, nil)
	descInodesTot  = prometheus.NewDesc("nomad_csi_volume_inodes_total", "Total inodes.", volLabels, nil)
	descInodesUsed = prometheus.NewDesc("nomad_csi_volume_inodes_used", "Used inodes.", volLabels, nil)
	descInodesFree = prometheus.NewDesc("nomad_csi_volume_inodes_free", "Free inodes.", volLabels, nil)
	descFiles      = prometheus.NewDesc("nomad_csi_volume_files", "File count (tree walk).", volLabels, nil)
	descDirs       = prometheus.NewDesc("nomad_csi_volume_dirs", "Directory count (tree walk).", volLabels, nil)
	descOther      = prometheus.NewDesc("nomad_csi_volume_other", "Other-object count: symlinks/sockets/devices (tree walk).", volLabels, nil)
	descStatfsAge  = prometheus.NewDesc("nomad_csi_volume_statfs_age_seconds", "Seconds since the last successful statfs.", volLabels, nil)
	descWalkAge    = prometheus.NewDesc("nomad_csi_volume_walk_age_seconds", "Seconds since the last completed walk.", volLabels, nil)
	descWalkDur    = prometheus.NewDesc("nomad_csi_volume_walk_duration_seconds", "Duration of the last completed walk.", volLabels, nil)
	descStale      = prometheus.NewDesc("nomad_csi_volume_stale", "1 if the reading is older than stale_after, else 0.", volLabels, nil)

	// Aggregate gauges (used when metrics_per_volume=false).
	descAggCount = prometheus.NewDesc("nomad_csi_volume_count", "Number of measured volumes.", nil, nil)
	descAggUsed  = prometheus.NewDesc("nomad_csi_volume_used_bytes_sum", "Sum of used bytes across measured volumes.", nil, nil)
	descAggTotal = prometheus.NewDesc("nomad_csi_volume_total_bytes_sum", "Sum of total bytes across measured volumes.", nil, nil)
)

// collector emits per-volume usage gauges from a snapshot provider on each scrape
// (no statfs/walk/fan-out on the scrape path — it reads relabeled cached data).
type collector struct {
	provider   func() []PublicVolumeStats
	perVolume  bool
	staleAfter time.Duration
	nowFn      func() time.Time
}

func newCollector(provider func() []PublicVolumeStats, perVolume bool, staleAfter time.Duration, nowFn func() time.Time) *collector {
	return &collector{provider: provider, perVolume: perVolume, staleAfter: staleAfter, nowFn: nowFn}
}

// RegisterCollector registers a per-volume usage collector on reg (the
// identity-wrapping Registerer, so the series inherit the constant
// driver/mode/node_id/plugin_id labels), backed by provider (a Source's relabeled
// snapshot). No-op if reg or provider is nil.
func RegisterCollector(reg prometheus.Registerer, provider func() []PublicVolumeStats, perVolume bool, staleAfter time.Duration) error {
	if reg == nil || provider == nil {
		return nil
	}
	return reg.Register(newCollector(provider, perVolume, staleAfter, time.Now))
}

func (c *collector) Describe(ch chan<- *prometheus.Desc) {
	if c.perVolume {
		for _, d := range []*prometheus.Desc{
			descTotalBytes, descUsedBytes, descAvailBytes, descInodesTot, descInodesUsed, descInodesFree,
			descFiles, descDirs, descOther, descStatfsAge, descWalkAge, descWalkDur, descStale,
		} {
			ch <- d
		}
		return
	}
	ch <- descAggCount
	ch <- descAggUsed
	ch <- descAggTotal
}

func (c *collector) Collect(ch chan<- prometheus.Metric) {
	items := c.provider()
	now := c.nowFn()

	if !c.perVolume {
		var count, used, total float64
		for _, s := range items {
			if s.AccessType == AccessBlock || s.StatfsAt.IsZero() {
				continue
			}
			count++
			used += float64(s.UsedBytes)
			total += float64(s.TotalBytes)
		}
		ch <- prometheus.MustNewConstMetric(descAggCount, prometheus.GaugeValue, count)
		ch <- prometheus.MustNewConstMetric(descAggUsed, prometheus.GaugeValue, used)
		ch <- prometheus.MustNewConstMetric(descAggTotal, prometheus.GaugeValue, total)
		return
	}

	for _, s := range items {
		if s.AccessType == AccessBlock { // no filesystem to measure
			continue
		}
		lv := []string{s.ID, s.Namespace, s.Node}
		if !s.StatfsAt.IsZero() {
			g(ch, descTotalBytes, float64(s.TotalBytes), lv)
			g(ch, descUsedBytes, float64(s.UsedBytes), lv)
			g(ch, descAvailBytes, float64(s.AvailableBytes), lv)
			g(ch, descInodesTot, float64(s.TotalInodes), lv)
			g(ch, descInodesUsed, float64(s.UsedInodes), lv)
			g(ch, descInodesFree, float64(s.FreeInodes), lv)
			g(ch, descStatfsAge, now.Sub(s.StatfsAt).Seconds(), lv)
			stale := 0.0
			if c.staleAfter > 0 && now.Sub(s.StatfsAt) > c.staleAfter {
				stale = 1
			}
			g(ch, descStale, stale, lv)
		}
		if s.WalkComplete {
			g(ch, descFiles, float64(s.FileCount), lv)
			g(ch, descDirs, float64(s.DirCount), lv)
			g(ch, descOther, float64(s.OtherCount), lv)
			g(ch, descWalkAge, now.Sub(s.WalkAt).Seconds(), lv)
			g(ch, descWalkDur, s.WalkDuration.Seconds(), lv)
		}
	}
}

func g(ch chan<- prometheus.Metric, d *prometheus.Desc, v float64, labels []string) {
	ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, labels...)
}
