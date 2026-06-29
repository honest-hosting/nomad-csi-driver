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
	staged        prometheus.Gauge // volumes currently staged on this node
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
		staged: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "nomad_csi", Subsystem: "node", Name: "staged_volumes",
			Help: "Volumes currently staged (mounted) on this node.",
		}),
	}
	reg.MustRegister(m.mountTotal, m.mountDuration, m.formatSkipped, m.staged)
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

// StagedInc records a volume becoming staged (mounted) on this node.
func (m *NodeMetrics) StagedInc() {
	if m != nil {
		m.staged.Inc()
	}
}

// StagedDec records a volume being unstaged from this node.
func (m *NodeMetrics) StagedDec() {
	if m != nil {
		m.staged.Dec()
	}
}
