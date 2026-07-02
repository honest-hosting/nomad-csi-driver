package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// ClusterMetrics holds the shared controller↔peer forwarding collectors, used by
// BOTH backends: the local controller-to-controller forwarding path and the qnap
// controller's fan-out to node daemons. It is registered on the identity-wrapping
// Registerer, so every series carries the constant driver/mode/node_id/plugin_id
// labels. All record methods are nil-safe so a backend without metrics (tests,
// node-only) is a clean no-op.
type ClusterMetrics struct {
	forwardTotal *prometheus.CounterVec // {method, outcome=ok|error|unreachable}
	resolveTotal *prometheus.CounterVec // {outcome=ok|error}
	peers        prometheus.Gauge
}

// NewClusterMetrics registers the cluster-forwarding collectors on reg.
func NewClusterMetrics(reg prometheus.Registerer) *ClusterMetrics {
	m := &ClusterMetrics{
		forwardTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "nomad_csi", Subsystem: "cluster", Name: "forward_total",
			Help: "Controller→peer forwards by method and outcome (ok|error|unreachable). local: controller-to-controller; qnap: controller fan-out to node daemons.",
		}, []string{"method", "outcome"}),
		resolveTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "nomad_csi", Subsystem: "cluster", Name: "resolve_total",
			Help: "Peer/node roster resolutions by outcome (ok|error).",
		}, []string{"outcome"}),
		peers: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "nomad_csi", Subsystem: "cluster", Name: "peers",
			Help: "Cluster members discovered at the last successful resolve (local: peer controllers; qnap: node daemons targeted by fan-out).",
		}),
	}
	reg.MustRegister(m.forwardTotal, m.resolveTotal, m.peers)
	return m
}

// Forward records one forward attempt outcome for method.
func (m *ClusterMetrics) Forward(method, outcome string) {
	if m != nil {
		m.forwardTotal.WithLabelValues(method, outcome).Inc()
	}
}

// Resolve records one roster-resolution outcome (ok on nil err, else error).
func (m *ClusterMetrics) Resolve(err error) {
	if m == nil {
		return
	}
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	m.resolveTotal.WithLabelValues(outcome).Inc()
}

// SetPeers records the count of cluster members from the last successful resolve.
func (m *ClusterMetrics) SetPeers(n int) {
	if m != nil {
		m.peers.Set(float64(n))
	}
}

// ForwardCounter and ResolveCounter expose the underlying child counters as
// prometheus.Collectors so their current values can be read (e.g. tests via
// testutil.ToFloat64). Lazily creates a zero child.
func (m *ClusterMetrics) ForwardCounter(method, outcome string) prometheus.Counter {
	return m.forwardTotal.WithLabelValues(method, outcome)
}

// ResolveCounter exposes the roster-resolution child counter (see ForwardCounter).
func (m *ClusterMetrics) ResolveCounter(outcome string) prometheus.Counter {
	return m.resolveTotal.WithLabelValues(outcome)
}
