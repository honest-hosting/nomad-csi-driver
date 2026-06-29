// Package qnap is the --driver=qnap backend: an iSCSI SAN backend that
// provisions block LUNs on a QNAP appliance via github.com/honest-hosting/go-qnap
// (controller) and attaches them over iSCSI + multipath (node). It registers
// itself with the driver registry in init().
package qnap

import (
	"context"
	"errors"
	"net/http"
	"time"

	goqnap "github.com/honest-hosting/go-qnap"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/cluster"
	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/iscsi"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
	"github.com/honest-hosting/nomad-csi-driver/internal/multipath"
	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

// nodeMetrics builds the shared node mount-layer metrics, or nil when metrics
// are unavailable. *NodeMetrics methods are nil-safe, so the nil case is a clean
// no-op both as the mountutil.Metrics sink and for the node's staged counter.
func nodeMetrics(d driver.Deps) *metrics.NodeMetrics {
	if d.Metrics == nil {
		return nil
	}
	return metrics.NewNodeMetrics(d.Metrics.Registerer())
}

func init() { driver.Register("qnap", New) }

const (
	pluginName         = "io.honesthosting.csi.qnap"
	defaultStateDir    = "/var/lib/nomad-csi-driver/qnap"
	devAppearTimeout   = 60 * time.Second
	defaultForwardAddr = ":9602"
)

// backend implements driver.Backend for the qnap SAN backend.
type backend struct {
	caps       driver.Capabilities
	ctrl       *controller
	nd         *node
	statsReg   *stats.Registry    // node-side per-volume usage stats; nil for controller-only
	forwardSrv *http.Server       // node-side stats forwarding server
	source     *qnapSource        // controller-side fan-out aggregate
	queryS     *stats.QueryServer // controller-side public query API
	log        *zap.Logger
}

// New constructs the qnap backend for the given mode.
func New(_ context.Context, d driver.Deps) (driver.Backend, error) {
	if d.Config == nil || d.Config.QNAP == nil {
		return nil, driver.InvalidArgument("qnap backend requires a 'qnap' config block")
	}
	cfg := d.Config.QNAP
	log := d.Logger
	if log == nil {
		log = zap.NewNop()
	}
	statsCfg, err := d.Config.ResolveStats()
	if err != nil {
		return nil, err
	}

	b := &backend{
		caps: driver.Capabilities{
			CreateDelete:     true,
			PublishUnpublish: false, // LUN is mapped at create time (map-at-create)
			Expand:           true,
			ExpandOnline:     true,
			Snapshot:         true,
			Clone:            true,
			GetCapacity:      true,
			ListVolumes:      true,
			ListSnapshots:    true,
			NodeStage:        true,
			NodeExpand:       true,
			NodeVolumeStats:  true,
			Topology:         false, // reachable from any node
		},
		log: log,
	}

	if d.Mode.HasController() {
		if err := validateControllerConfig(cfg); err != nil {
			return nil, err
		}
		cl, err := newQNAPClient(cfg, d.Metrics.Registerer(), log)
		if err != nil {
			return nil, driver.Internal("creating qnap client: %v", err)
		}
		sm := newSessionManager(cl, cfg.Username, cfg.Password)
		b.ctrl = newController(cl, sm, cfg, log)
		// Echo the multipath portal config at startup so the deploy is verifiable
		// (e.g. confirm two portals were picked up).
		log.Info("qnap controller configured",
			zap.Int("portals", len(cfg.PortalList())), zap.Strings("portal_list", cfg.PortalList()))
	}

	if d.Mode.HasNode() {
		stateDir := cfg.NodeStateDir
		if stateDir == "" {
			stateDir = defaultStateDir
		}
		mpath := multipath.New(d.Runner, cfg.MultipathConfigDir)
		var qnm *qnapNodeMetrics
		if d.Metrics != nil {
			qnm = newQNAPNodeMetrics(d.Metrics.Registerer())
		}
		nm := nodeMetrics(d)
		b.statsReg = stats.NewRegistry(statsCfg, d.NodeID, log)
		b.nd = &node{
			cfg:          cfg,
			nodeID:       d.NodeID,
			iscsi:        iscsi.New(d.Runner),
			mpath:        mpath,
			mounter:      mountutil.New(d.Runner, log).WithMetrics(nm),
			meta:         newFileMetaStore(stateDir),
			useMultipath: !cfg.DisableMultipath,
			log:          log,
			metrics:      qnm,
			nodeM:        nm,
			stats:        b.statsReg,
			waitForPath:  osWaitForPath(devAppearTimeout),
		}
		if b.nd.useMultipath {
			b.installMultipathDropin(context.Background(), mpath)
		}
	}

	if err := b.setupStats(d, cfg, statsCfg, log); err != nil {
		return nil, err
	}
	return b, nil
}

// setupStats wires the per-volume usage stats transport. The node runs a
// forwarding server (so the controller can pull its readings); the controller
// fans out to all nodes, aggregates, and exposes the query API + /metrics. Both
// require a shared forward_secret — without it, qnap stats hydrate node-locally
// but are not centrally queryable.
func (b *backend) setupStats(d driver.Deps, cfg *config.QNAPConfig, statsCfg stats.Config, log *zap.Logger) error {
	if !statsCfg.Enabled {
		return nil
	}
	if cfg.ForwardSecret == "" {
		if d.Mode.HasController() {
			log.Warn("qnap stats: forward_secret not set; central query API and /metrics disabled (node-local hydration still runs)")
		}
		return nil
	}
	forwardAddr := cfg.ForwardAddr
	if forwardAddr == "" {
		forwardAddr = defaultForwardAddr
	}

	// Node: serve its readings to the controller.
	if b.nd != nil {
		b.forwardSrv = &http.Server{
			Addr:              forwardAddr,
			Handler:           cluster.NewServer(cfg.ForwardSecret, b.nd.dispatchForward),
			ReadHeaderTimeout: 5 * time.Second,
		}
		go func() {
			log.Info("qnap stats forwarding server listening", zap.String("address", forwardAddr))
			if err := b.forwardSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Error("qnap stats forwarding server error", zap.Error(err))
			}
		}()
	}

	// Controller: fan out to nodes, aggregate, and expose query API + metrics.
	if d.Mode.HasController() {
		nopts, err := nomadOptions(cfg, d.NodeID, forwardAddr, log)
		if err != nil {
			return err
		}
		res, err := cluster.NewNomadResolver(nopts)
		if err != nil {
			return err
		}
		mapper, err := cluster.NewNomadVolumes(nopts) // Nomad-id ↔ external-id for the stats API
		if err != nil {
			return err
		}
		var cm *metrics.ClusterMetrics
		if d.Metrics != nil {
			cm = metrics.NewClusterMetrics(d.Metrics.Registerer()) // shared forward/resolve/peers
		}
		b.source = newQNAPSource(res, cluster.NewClient(cfg.ForwardSecret), mapper, statsCfg.Namespace, statsCfg.AggregateInterval, cm, log)
		if d.Metrics != nil {
			if err := stats.RegisterCollector(d.Metrics.Registerer(), b.source.metricsSnapshot, statsCfg.MetricsPerVolume, statsCfg.StaleAfter); err != nil {
				return driver.Internal("registering stats collector: %v", err)
			}
		}
		qs, err := stats.NewQueryServer(statsCfg.QueryAddr, b.source, statsCfg.QueryToken, statsCfg.QueryTokenHeader, log)
		if err != nil {
			return driver.Internal("starting stats query server: %v", err)
		}
		if qs != nil {
			qs.Serve()
			b.queryS = qs
			log.Info("stats query API listening", zap.String("address", qs.Addr()), zap.Bool("auth", statsCfg.QueryToken != ""))
		}
	}
	return nil
}

func (b *backend) Name() string                      { return "qnap" }
func (b *backend) PluginName() string                { return pluginName }
func (b *backend) Capabilities() driver.Capabilities { return b.caps }

// Shutdown stops the stats subsystem: node-side workers/walk pool + forwarding
// server, and controller-side fan-out collector + query server. All are
// non-blocking and nil-safe (controller-only / node-only processes skip the
// halves they never built).
func (b *backend) Shutdown(ctx context.Context) error {
	b.statsReg.Close()
	b.source.Close()
	if b.queryS != nil {
		_ = b.queryS.Close(ctx)
	}
	if b.forwardSrv != nil {
		return b.forwardSrv.Shutdown(ctx)
	}
	return nil
}

func (b *backend) Controller() driver.ControllerBackend {
	if b.ctrl == nil {
		return nil
	}
	return b.ctrl
}

func (b *backend) Node() driver.NodeBackend {
	if b.nd == nil {
		return nil
	}
	return b.nd
}

// Probe checks controller readiness by ensuring a live session. Node-only
// processes have nothing remote to probe.
func (b *backend) Probe(ctx context.Context) error {
	if b.ctrl == nil {
		return nil
	}
	if _, err := b.ctrl.sm.get(ctx); err != nil {
		return mapQNAPError("Probe", err)
	}
	return nil
}

// installMultipathDropin renders the QNAP drop-in and reloads multipathd. It is
// best-effort: failures are logged, not fatal, so a node without multipath
// configured can still attach raw devices.
func (b *backend) installMultipathDropin(ctx context.Context, m *multipath.Manager) {
	if err := m.WriteDropin(multipath.DefaultDropin()); err != nil {
		b.log.Warn("writing multipath drop-in", zap.Error(err))
		return
	}
	if err := m.Reload(ctx); err != nil {
		b.log.Warn("reloading multipathd", zap.Error(err))
	}
}

func validateControllerConfig(cfg *config.QNAPConfig) error {
	switch {
	case cfg.BaseURL == "":
		return driver.InvalidArgument("qnap.base_url is required for controller mode")
	case cfg.Username == "" || cfg.Password == "":
		return driver.InvalidArgument("qnap.username and qnap.password are required for controller mode")
	case len(cfg.Interfaces) == 0:
		return driver.InvalidArgument("qnap.interfaces is required for controller mode (used to create 1:1 iSCSI targets)")
	case len(cfg.PortalList()) == 0:
		// Without a portal the volume context carries no iSCSI target host, so
		// NodeStageVolume fails later with "missing portal/iqn". Fail fast here.
		return driver.InvalidArgument("qnap.portal (or qnap.portals) is required for controller mode (the iSCSI portal(s) nodes connect to)")
	}
	return nil
}

// qnapHooks adapts go-qnap observability hooks to Prometheus collectors
// registered on the shared registry (so every appliance call is measured) AND
// to structured zap logging: each request is logged at debug, and a failed
// operation at info, so go-qnap calls can be traced from the plugin's logs.
func qnapHooks(reg prometheus.Registerer, log *zap.Logger) goqnap.Hooks {
	opDur := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "nomad_csi", Subsystem: "qnap", Name: "op_duration_seconds",
		Help: "Duration of go-qnap operations by op and success.", Buckets: prometheus.DefBuckets,
	}, []string{"op", "success"})
	reqDur := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "nomad_csi", Subsystem: "qnap", Name: "request_duration_seconds",
		Help: "Duration of QNAP HTTP attempts by op and method.", Buckets: prometheus.DefBuckets,
	}, []string{"op", "method"})
	// op_total adds the categorized failure outcome (ok|auth|busy|notfound|
	// conflict|unsupported|ratelimit|transport|other) that the duration's bool
	// success label can't express — the primary "why did the appliance call fail".
	opTotal := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "nomad_csi", Subsystem: "qnap", Name: "op_total",
		Help: "go-qnap operations by op and categorized outcome.",
	}, []string{"op", "outcome"})
	reg.MustRegister(opDur, reqDur, opTotal)

	return goqnap.Hooks{
		OnOperation: func(s goqnap.OperationStat) {
			opDur.WithLabelValues(s.Op, boolLabel(s.Err == nil)).Observe(s.Duration.Seconds())
			opTotal.WithLabelValues(s.Op, qnapOutcome(s.Err)).Inc()
			if s.Err != nil {
				log.Info("qnap op failed",
					zap.String("op", s.Op), zap.Int("attempts", s.Attempts),
					zap.Int64("duration_ms", s.Duration.Milliseconds()),
					zap.Int("api_code", s.APICode), zap.Error(s.Err))
				return
			}
			log.Debug("qnap op",
				zap.String("op", s.Op), zap.Int("attempts", s.Attempts),
				zap.Int64("duration_ms", s.Duration.Milliseconds()),
				zap.Int("api_code", s.APICode))
		},
		OnRequest: func(s goqnap.RequestStat) {
			reqDur.WithLabelValues(s.Op, s.Method).Observe(s.Duration.Seconds())
			log.Debug("qnap request",
				zap.String("op", s.Op), zap.String("method", s.Method), zap.String("path", s.Path),
				zap.Int("status", s.StatusCode), zap.Int("attempt", s.Attempt),
				zap.Int64("duration_ms", s.Duration.Milliseconds()),
				zap.Int64("bytes_in", s.BytesIn), zap.Int64("bytes_out", s.BytesOut),
				zap.Error(s.Err))
		},
	}
}

// qnapNodeMetrics holds the qnap node-side collectors. All record* methods are
// nil-safe so a node without metrics (tests) is a no-op.
type qnapNodeMetrics struct {
	iscsiLogin *prometheus.CounterVec // {outcome=ok|fail}
	stage      *prometheus.CounterVec // {outcome=ok|degraded|failed}
	rescan     *prometheus.CounterVec // {outcome=ok|error}
	deviceWait *prometheus.CounterVec // {outcome=ok|timeout}
}

func newQNAPNodeMetrics(reg prometheus.Registerer) *qnapNodeMetrics {
	m := &qnapNodeMetrics{
		iscsiLogin: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "nomad_csi", Subsystem: "qnap", Name: "iscsi_login_total",
			Help: "iSCSI portal logins by outcome (ok|fail).",
		}, []string{"outcome"}),
		stage: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "nomad_csi", Subsystem: "qnap", Name: "node_stage_total",
			Help: "Node stage outcomes; degraded = fewer active paths than configured portals.",
		}, []string{"outcome"}),
		rescan: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "nomad_csi", Subsystem: "qnap", Name: "iscsi_rescan_total",
			Help: "iSCSI session rescans (node expand) by outcome.",
		}, []string{"outcome"}),
		deviceWait: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "nomad_csi", Subsystem: "qnap", Name: "device_wait_total",
			Help: "Waits for the iSCSI/multipath device to appear, by outcome (ok|timeout).",
		}, []string{"outcome"}),
	}
	reg.MustRegister(m.iscsiLogin, m.stage, m.rescan, m.deviceWait)
	return m
}

func (m *qnapNodeMetrics) recordLogin(outcome string) {
	if m != nil {
		m.iscsiLogin.WithLabelValues(outcome).Inc()
	}
}
func (m *qnapNodeMetrics) recordStage(outcome string) {
	if m != nil {
		m.stage.WithLabelValues(outcome).Inc()
	}
}
func (m *qnapNodeMetrics) recordRescan(outcome string) {
	if m != nil {
		m.rescan.WithLabelValues(outcome).Inc()
	}
}
func (m *qnapNodeMetrics) recordDeviceWait(outcome string) {
	if m != nil {
		m.deviceWait.WithLabelValues(outcome).Inc()
	}
}

func boolLabel(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// qnapOutcome categorizes a go-qnap operation error into a bounded label for
// op_total, using the library's sentinel taxonomy + HTTP status. Unknown API
// errors are "other"; non-API errors (no APIError in the chain) are "transport".
func qnapOutcome(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, goqnap.ErrAuthFailed), errors.Is(err, goqnap.ErrSessionInvalid):
		return "auth"
	case errors.Is(err, goqnap.ErrResourceBusy):
		return "busy"
	case errors.Is(err, goqnap.ErrNameConflict):
		return "conflict"
	case errors.Is(err, goqnap.ErrPoolMissing):
		return "notfound"
	case errors.Is(err, goqnap.ErrTimeout):
		return "timeout"
	case errors.Is(err, goqnap.ErrUnsupported):
		return "unsupported"
	}
	var ae *goqnap.APIError
	if errors.As(err, &ae) {
		if ae.HTTPStatus == http.StatusTooManyRequests {
			return "ratelimit"
		}
		return "other"
	}
	return "transport"
}
