package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// NodeMetrics holds the shared node mount-layer collectors (both backends'
// nodes use the same mountutil seam). Registered on the shared registry in
// backend.New and attached to the Mounter via mountutil.(*Mounter).WithMetrics.
// It satisfies the mountutil.Metrics interface structurally.
type NodeMetrics struct {
	mountTotal    *prometheus.CounterVec   // {op, outcome}
	mountDuration *prometheus.HistogramVec // {op}
	formatSkipped prometheus.Counter
	ctxRebuilt    *prometheus.CounterVec // {field}
}

// NewNodeMetrics registers the node collectors on reg (the identity-wrapping
// Registerer, so they inherit the constant driver/mode/node_id/plugin_id labels).
func NewNodeMetrics(reg prometheus.Registerer) *NodeMetrics {
	m := &NodeMetrics{
		mountTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "nomad_csi", Subsystem: "node", Name: "mount_total",
			Help: "Node mount-layer operations by op (format|mount|unmount|bind|resize) and outcome.",
		}, []string{"op", "outcome"}),
		mountDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "nomad_csi", Subsystem: "node", Name: "mount_duration_seconds",
			Help: "Node mount-layer operation duration by op.", Buckets: prometheus.DefBuckets,
		}, []string{"op"}),
		formatSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "nomad_csi", Subsystem: "node", Name: "format_skipped_total",
			Help: "Times an existing filesystem was found and mkfs was skipped (idempotency safety signal).",
		}),
		ctxRebuilt: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "nomad_csi", Subsystem: "node", Name: "volume_context_reconstructed_total",
			Help: "Volume-context fields rebuilt from the volume's external id because Nomad delivered the context empty. Non-zero means `nomad volume create` was run against an ALREADY-EXISTING volume: Nomad skips the plugin on that path and (before 1.9.6 / hashicorp/nomad#24922) erases the stored context. Should stay at zero on Nomad >= 1.9.6. A block volume can rebuild at both stage and publish, so counts are per field-resolution, not per volume.",
		}, []string{"field"}),
	}
	reg.MustRegister(m.mountTotal, m.mountDuration, m.formatSkipped, m.ctxRebuilt)
	return m
}

// All methods are nil-safe so a node without metrics (tests, or a typed-nil
// *NodeMetrics passed as the mountutil.Metrics interface) is a clean no-op.

// MountOp records one mount-layer operation outcome + duration.
func (m *NodeMetrics) MountOp(op, outcome string, dur time.Duration) {
	if m == nil {
		return
	}
	m.mountTotal.WithLabelValues(op, outcome).Inc()
	m.mountDuration.WithLabelValues(op).Observe(dur.Seconds())
}

// FormatSkipped records an idempotent format-skip (existing filesystem reused).
func (m *NodeMetrics) FormatSkipped() {
	if m != nil {
		m.formatSkipped.Inc()
	}
}

// VolumeContextReconstructed records one volume-context field that had to be
// rebuilt from the volume's external id. Callers pass the context key they
// recovered (e.g. "dataset", "node", "iqn"), so the label says which part of the
// context Nomad dropped.
func (m *NodeMetrics) VolumeContextReconstructed(field string) {
	if m != nil {
		m.ctxRebuilt.WithLabelValues(field).Inc()
	}
}

// NOTE: the staged-volume count is no longer an Inc/Dec gauge here. It is
// derived from live host state at scrape time via metrics.RegisterStagedGauge +
// each backend's StagedCounter, so it survives plugin restarts and can never go
// negative. See internal/metrics/staged.go.
