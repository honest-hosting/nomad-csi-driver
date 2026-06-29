// Package metrics owns the Prometheus registry and the shared collectors. It is
// injected into the gRPC interceptors and the backends. It intentionally does
// not import any backend package; backend-specific adapters (e.g. the go-qnap
// hook adapter) live in the backend that needs them and call ObserveRPC / use
// the registry exposed here.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/honest-hosting/nomad-csi-driver/internal/version"
)

// Metrics holds the registry and the standard CSI-RPC collectors. The deployment
// identity (driver/mode/node_id/plugin_id) is attached as constant labels to
// EVERY series via a wrapped Registerer, so it is not repeated in any metric's
// own label set. Per-metric labels stay bounded (e.g. rpc = method/code only).
type Metrics struct {
	reg         *prometheus.Registry
	registerer  prometheus.Registerer
	rpcDuration *prometheus.HistogramVec
	rpcTotal    *prometheus.CounterVec
}

// New builds a Metrics with a fresh registry whose Registerer stamps the
// deployment-identity constant labels (driver/mode/node_id/plugin_id) onto every
// collector registered through it — these RPC metrics, the Go/process collectors,
// build_info, and all backend collectors registered via Registerer(). This bakes
// identity into the series (a deliberate reversal of the scrape-relabel posture in
// ARCHITECTURE §9) without per-call-site plumbing.
func New(driver, mode, nodeID, pluginID string) *Metrics {
	reg := prometheus.NewRegistry()
	registerer := prometheus.WrapRegistererWith(prometheus.Labels{
		"driver":    driver,
		"mode":      mode,
		"node_id":   nodeID,
		"plugin_id": pluginID,
	}, reg)

	registerer.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		reg:        reg,
		registerer: registerer,
		rpcDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "nomad_csi",
			Subsystem: "rpc",
			Name:      "duration_seconds",
			Help:      "Duration of CSI RPCs by method and resulting gRPC code.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "code"}),
		rpcTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "nomad_csi",
			Subsystem: "rpc",
			Name:      "total",
			Help:      "Count of CSI RPCs by method and resulting gRPC code.",
		}, []string{"method", "code"}),
	}
	registerer.MustRegister(m.rpcDuration, m.rpcTotal)

	// build_info: a constant gauge=1 carrying the build stamp. The deployment
	// identity (driver/mode/node_id/plugin_id) is supplied by the wrapped
	// Registerer's constant labels, so it doubles as the metrics "source"
	// descriptor (which process/listener served the scrape).
	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "nomad_csi",
		Name:      "build_info",
		Help:      "Build/deployment identity; always 1. Carries version/commit/build_date plus the constant driver/mode/node_id/plugin_id labels.",
	}, []string{"version", "commit", "build_date"})
	buildInfo.WithLabelValues(version.Version, version.CommitSHA, version.BuildDate).Set(1)
	registerer.MustRegister(buildInfo)

	return m
}

// Registry returns the underlying registry for the HTTP handler / scrape.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Registerer returns the identity-wrapping Registerer that backends MUST use to
// register their own collectors, so each inherits the constant deployment-identity
// labels (driver/mode/node_id/plugin_id).
func (m *Metrics) Registerer() prometheus.Registerer { return m.registerer }

// ObserveRPC records one CSI RPC outcome.
func (m *Metrics) ObserveRPC(method, code string, dur time.Duration) {
	m.rpcDuration.WithLabelValues(method, code).Observe(dur.Seconds())
	m.rpcTotal.WithLabelValues(method, code).Inc()
}

// Handler returns the HTTP handler that serves the registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
