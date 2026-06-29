package cmd

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/csi"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
	"github.com/honest-hosting/nomad-csi-driver/internal/version"

	// Backends register themselves via init(). They are imported here for side
	// effects so driver.New can resolve --driver.
	_ "github.com/honest-hosting/nomad-csi-driver/internal/driver/local"
	_ "github.com/honest-hosting/nomad-csi-driver/internal/driver/qnap"
)

var runFlags struct {
	mode          string
	driver        string
	endpoint      string
	nodeID        string
	pluginID      string
	parentDataset string
	config        string
	logLevel      string
	logFormat     string
}

// defaultParentDataset is the ZFS dataset under each pool that holds provisioned
// zvols (--driver=local) when neither --parent-dataset nor a pool's
// parent_dataset override is set.
const defaultParentDataset = "nomad-csi"

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the CSI plugin server",
	Long: "Run starts the CSI gRPC server on the Unix socket Nomad provides. " +
		"--mode selects the process role and --driver the storage backend.",
	RunE: runServer,
}

func init() {
	f := runCmd.Flags()
	f.StringVar(&runFlags.mode, "mode", "", "process role: controller|node|monolith")
	f.StringVar(&runFlags.driver, "driver", "", "storage backend: qnap|local")
	f.StringVar(&runFlags.endpoint, "endpoint", envOr("CSI_ENDPOINT", ""), "CSI Unix socket endpoint (unix://...); defaults to $CSI_ENDPOINT")
	f.StringVar(&runFlags.nodeID, "node-id", envOr("CSI_NODE_ID", ""), "this node's unique ID (e.g. ${node.unique.name}); defaults to $CSI_NODE_ID; required in all modes (also a metric label)")
	f.StringVar(&runFlags.pluginID, "plugin-id", envOr("CSI_PLUGIN_ID", ""), "the Nomad csi_plugin id this deployment runs as (e.g. ${var.plugin_id}); defaults to $CSI_PLUGIN_ID; required (also a metric label)")
	f.StringVar(&runFlags.parentDataset, "parent-dataset", defaultParentDataset, "(--driver=local) ZFS dataset under each pool that holds provisioned zvols: <pool>/<parent-dataset>/<volume-id>. A pool's parent_dataset config overrides it. Set to your csi_plugin id (e.g. --parent-dataset=${var.plugin_id}) to namespace per deployment")
	f.StringVar(&runFlags.config, "config", "", "path to the deployment config file (HCL or JSON)")
	f.StringVar(&runFlags.logLevel, "log-level", "info", "log level: debug|info|warn|error")
	f.StringVar(&runFlags.logFormat, "log-format", "json", "log format: json|console")
}

func runServer(cmd *cobra.Command, _ []string) error {
	if runFlags.driver == "" {
		return fmt.Errorf("--driver is required (one of: %v)", driver.Registered())
	}
	if runFlags.endpoint == "" {
		return fmt.Errorf("--endpoint (or $CSI_ENDPOINT) is required")
	}
	mode := driver.Mode(runFlags.mode)
	if err := driver.ValidateModeForDriver(runFlags.driver, mode); err != nil {
		return err
	}
	if runFlags.nodeID == "" {
		return fmt.Errorf("--node-id (or $CSI_NODE_ID) is required")
	}
	if runFlags.pluginID == "" {
		return fmt.Errorf("--plugin-id (or $CSI_PLUGIN_ID) is required")
	}

	log, err := buildLogger(runFlags.logLevel, runFlags.logFormat)
	if err != nil {
		return err
	}
	defer func() { _ = log.Sync() }()
	log.Info("starting nomad-csi-driver",
		zap.String("version", version.Version),
		zap.String("driver", runFlags.driver), zap.String("mode", runFlags.mode))

	cfg, err := config.Load(runFlags.config)
	if err != nil {
		return err
	}

	// SIGINT/SIGTERM trigger graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	m := metrics.New(runFlags.driver, string(mode), runFlags.nodeID, runFlags.pluginID)

	backend, err := driver.New(ctx, runFlags.driver, driver.Deps{
		Mode:          mode,
		NodeID:        runFlags.nodeID,
		ParentDataset: runFlags.parentDataset,
		Config:        cfg,
		Runner:        cexec.NewOSRunner(),
		Logger:        log,
		Metrics:       m,
	})
	if err != nil {
		return fmt.Errorf("initializing %s backend: %w", runFlags.driver, err)
	}

	// Start the metrics endpoint first so the readiness wait is observable, then
	// gate on backend readiness BEFORE creating/serving the CSI socket. If the
	// backend never becomes ready we tear everything down and exit non-zero — we
	// must not advertise a CSI socket Nomad would mark healthy while the backing
	// store is unusable; Nomad's rescheduler takes over instead.
	metricsSrv := startMetricsServer(cfg, m, log)

	if err := awaitBackendReady(ctx, backend, cfg, log); err != nil {
		shutdownBackend(backend, log)
		shutdownMetricsServer(metricsSrv, log)
		return err
	}

	srv, err := csi.NewServer(backend, mode, runFlags.endpoint, log, m)
	if err != nil {
		shutdownBackend(backend, log)
		shutdownMetricsServer(metricsSrv, log)
		return err
	}

	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve() }()

	select {
	case <-ctx.Done():
		log.Info("shutdown signal received; stopping gracefully")
		srv.GracefulStop()
	case err := <-serveErr:
		if err != nil {
			log.Error("csi server stopped with error", zap.Error(err))
		}
		srv.Stop()
	}
	shutdownBackend(backend, log)
	shutdownMetricsServer(metricsSrv, log)
	return nil
}

// prober is the readiness surface of a backend (driver.Backend satisfies it).
type prober interface {
	Probe(ctx context.Context) error
}

// awaitBackendReady blocks until the backend reports ready (Probe returns nil)
// or the configured readiness timeout elapses, retrying every interval. It
// returns an error — so the caller exits non-zero and Nomad reschedules — when
// the backend never becomes ready or ctx is cancelled. A zero timeout means a
// single attempt (fail fast); a non-zero timeout retries in-process.
func awaitBackendReady(ctx context.Context, b prober, cfg *config.Config, log *zap.Logger) error {
	timeout, interval, err := cfg.ResolveReadiness()
	if err != nil {
		return err
	}

	start := time.Now()
	deadline := start.Add(timeout)
	for attempt := 1; ; attempt++ {
		if perr := b.Probe(ctx); perr == nil {
			if attempt > 1 {
				log.Info("backend ready", zap.Int("attempts", attempt),
					zap.Duration("waited", time.Since(start)))
			} else {
				log.Info("backend ready")
			}
			return nil
		} else if timeout <= 0 || !time.Now().Before(deadline) {
			// Out of attempts: exit non-zero so Nomad's rescheduler takes over
			// rather than serving a CSI socket the backing store can't support.
			log.Error("backend not ready; exiting non-zero so Nomad reschedules",
				zap.Int("attempts", attempt), zap.Duration("waited", time.Since(start)),
				zap.Duration("timeout", timeout), zap.Error(perr))
			return fmt.Errorf("backend not ready after %d attempt(s) over %s: %w", attempt, time.Since(start).Round(time.Second), perr)
		} else {
			log.Warn("backend not ready; retrying",
				zap.Int("attempt", attempt), zap.Duration("retry_in", interval),
				zap.Duration("deadline_in", time.Until(deadline).Round(time.Second)), zap.Error(perr))
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("readiness wait cancelled: %w", ctx.Err())
		case <-time.After(interval):
		}
	}
}

// shutdownBackend invokes the backend's optional Shutdown hook (e.g. to stop a
// forwarding server and deregister from service discovery).
func shutdownBackend(backend driver.Backend, log *zap.Logger) {
	sd, ok := backend.(driver.Shutdowner)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := sd.Shutdown(ctx); err != nil {
		log.Warn("backend shutdown error", zap.Error(err))
	}
}

// startMetricsServer launches the Prometheus endpoint if configured, returning
// the server (or nil) for later shutdown.
func startMetricsServer(cfg *config.Config, m *metrics.Metrics, log *zap.Logger) *http.Server {
	if cfg.Metrics == nil || !cfg.Metrics.Enabled {
		return nil // metrics off by default
	}
	addr, path := cfg.Metrics.EffectiveAddress(), cfg.Metrics.EffectivePath()
	mux := http.NewServeMux()
	mux.Handle(path, m.Handler())
	hs := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("metrics endpoint listening", zap.String("address", addr), zap.String("path", path))
		if err := hs.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server error", zap.Error(err))
		}
	}()
	return hs
}

func shutdownMetricsServer(hs *http.Server, log *zap.Logger) {
	if hs == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := hs.Shutdown(ctx); err != nil {
		log.Warn("metrics server shutdown error", zap.Error(err))
	}
}

func buildLogger(level, format string) (*zap.Logger, error) {
	var lvl zapcore.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid --log-level %q: %w", level, err)
	}
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(lvl)
	switch format {
	case "json":
		cfg.Encoding = "json"
	case "console":
		cfg.Encoding = "console"
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	default:
		return nil, fmt.Errorf("invalid --log-format %q (want json|console)", format)
	}
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	return cfg.Build()
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
