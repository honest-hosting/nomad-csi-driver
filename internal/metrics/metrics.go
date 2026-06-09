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
)

// Metrics holds the registry and the standard CSI-RPC collectors. Cardinality
// is kept bounded: labels are driver/method/code only — never per-volume.
type Metrics struct {
	reg         *prometheus.Registry
	driver      string
	rpcDuration *prometheus.HistogramVec
	rpcTotal    *prometheus.CounterVec
}

// New builds a Metrics with a fresh registry, labeled by driver name.
func New(driver string) *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		reg:    reg,
		driver: driver,
		rpcDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "nomad_csi",
			Subsystem: "rpc",
			Name:      "duration_seconds",
			Help:      "Duration of CSI RPCs by method and resulting gRPC code.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"driver", "method", "code"}),
		rpcTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "nomad_csi",
			Subsystem: "rpc",
			Name:      "total",
			Help:      "Count of CSI RPCs by method and resulting gRPC code.",
		}, []string{"driver", "method", "code"}),
	}
	reg.MustRegister(m.rpcDuration, m.rpcTotal)
	return m
}

// Registry returns the underlying registry so backends can register their own
// collectors (e.g. pool capacity, multipath state).
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// ObserveRPC records one CSI RPC outcome.
func (m *Metrics) ObserveRPC(method, code string, dur time.Duration) {
	m.rpcDuration.WithLabelValues(m.driver, method, code).Observe(dur.Seconds())
	m.rpcTotal.WithLabelValues(m.driver, method, code).Inc()
}

// Handler returns the HTTP handler that serves the registry.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}
