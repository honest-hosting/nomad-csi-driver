// Package local is the --driver=local backend: node-local thick ZFS zvols
// exposed as CSI volumes. It runs as a monolith on every node under a single
// plugin_id; because Nomad routes controller RPCs topology-blind, each
// controller forwards operations to the node that owns the target volume via
// the cluster forwarding transport (peer discovery via Nomad's /v1/nodes API
// over the task API socket). It registers itself with the driver registry in
// init().
package local

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/cluster"
	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
	"github.com/honest-hosting/nomad-csi-driver/internal/zfs"
)

// nodeMetrics builds the shared node mount-layer metrics, or nil when metrics
// are unavailable. *NodeMetrics methods are nil-safe, so the nil case is a clean
// no-op both as the mountutil.Metrics sink and for the node's staged counter.
func nodeMetrics(d driver.Deps) *metrics.NodeMetrics {
	if d.Metrics == nil {
		return nil
	}
	return metrics.NewNodeMetrics(d.Metrics.Registry())
}

func init() { driver.Register("local", New) }

const (
	pluginName         = "io.honesthosting.csi.local"
	defaultForwardAddr = ":9602"
	devAppearTimeout   = 30 * time.Second
)

type backend struct {
	caps       driver.Capabilities
	ctrl       *controller
	nd         *node
	z          *zfs.ZFS
	cfg        *config.LocalConfig
	forwardSrv *http.Server
	log        *zap.Logger
}

// New constructs the local backend. local is monolith-only, so both controller
// and node halves are always present, and a forwarding server is started.
func New(_ context.Context, d driver.Deps) (driver.Backend, error) {
	if d.Config == nil || d.Config.Local == nil {
		return nil, driver.InvalidArgument("local backend requires a 'local' config block")
	}
	cfg := d.Config.Local
	log := d.Logger
	if log == nil {
		log = zap.NewNop()
	}
	if err := validatePools(cfg); err != nil {
		return nil, err
	}
	if cfg.ForwardSecret == "" {
		return nil, driver.InvalidArgument("local.forward_secret is required (shared secret for controller-to-controller forwarding)")
	}
	if d.NodeID == "" {
		return nil, driver.InvalidArgument("node id is required for the local backend")
	}

	forwardAddr := cfg.ForwardAddr
	if forwardAddr == "" {
		forwardAddr = defaultForwardAddr
	}

	res, err := buildResolver(cfg, d.NodeID, forwardAddr, log)
	if err != nil {
		return nil, err
	}
	z := zfs.New(d.Runner)
	fwd := cluster.NewClient(cfg.ForwardSecret)
	// Default ZFS parent dataset under each pool (a pool may still override it via
	// parent_dataset). Defend against an empty value reaching us programmatically;
	// the CLI flag already defaults it.
	parentDataset := d.ParentDataset
	if parentDataset == "" {
		parentDataset = "nomad-csi"
	}
	log.Info("local: volumes provisioned under ZFS parent dataset",
		zap.String("parent_dataset", parentDataset),
		zap.String("layout", "<pool>/"+parentDataset+"/<volume-id>"))
	ctrl := newController(z, cfg, parentDataset, res, fwd, log)
	if d.Metrics != nil {
		reg := d.Metrics.Registry()
		ctrl.metrics = newLocalMetrics(reg)
		reg.MustRegister(newPoolCollector(z, cfg, parentDataset, log)) // pool gauges, computed on scrape
	}
	nm := nodeMetrics(d) // shared node metrics (mount + staged), also the mounter's sink

	b := &backend{
		caps: driver.Capabilities{
			CreateDelete:    true,
			Expand:          true,
			ExpandOnline:    true,
			Snapshot:        true,
			Clone:           true,
			GetCapacity:     true,
			ListVolumes:     true,
			ListSnapshots:   true,
			NodeStage:       true,
			NodeExpand:      true,
			NodeVolumeStats: true,
			Topology:        true, // node-pinned
		},
		ctrl: ctrl,
		nd: &node{
			cfg:         cfg,
			z:           z,
			nodeID:      d.NodeID,
			mounter:     mountutil.New(d.Runner, log).WithMetrics(nm),
			log:         log,
			nodeM:       nm,
			waitForPath: osWaitForPath(devAppearTimeout),
		},
		z:   z,
		cfg: cfg,
		log: log,
	}

	// Start the forwarding server so peers can route owner-node operations here.
	b.forwardSrv = &http.Server{
		Addr:              forwardAddr,
		Handler:           cluster.NewServer(cfg.ForwardSecret, ctrl.dispatchForward),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("local forwarding server listening", zap.String("address", forwardAddr))
		if err := b.forwardSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("forwarding server error", zap.Error(err))
		}
	}()

	// Log the discovered peer count once at startup (best-effort; non-fatal).
	// Discovery is read-only — nothing to register — so there is no background
	// registration loop.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if peers, err := res.List(ctx); err != nil {
			log.Warn("peer discovery: initial roster read failed; will retry on demand", zap.Error(err))
		} else {
			log.Info("peer discovery ready", zap.Int("peers", len(peers)))
		}
	}()

	return b, nil
}

// Shutdown stops the forwarding server. Peer discovery is read-only (nothing
// was registered), so there is nothing to deregister.
func (b *backend) Shutdown(ctx context.Context) error {
	if b.forwardSrv != nil {
		return b.forwardSrv.Shutdown(ctx)
	}
	return nil
}

func (b *backend) Name() string                      { return "local" }
func (b *backend) PluginName() string                { return pluginName }
func (b *backend) Capabilities() driver.Capabilities { return b.caps }
func (b *backend) Controller() driver.ControllerBackend {
	return b.ctrl
}
func (b *backend) Node() driver.NodeBackend { return b.nd }

// Probe validates that ZFS is present and at least one allowlisted pool is
// ONLINE on this node. A node may legitimately have only a subset of the
// configured pools; it is unhealthy only if ZFS is unreachable or none of its
// pools are usable.
func (b *backend) Probe(ctx context.Context) error {
	usable := 0
	for _, p := range b.cfg.Pools {
		present, online, err := b.z.PoolStatus(ctx, p.Name)
		if err != nil {
			return driver.Unavailable("zfs/zpool unavailable: %v", err)
		}
		if present && online {
			usable++
		}
	}
	if usable == 0 {
		return driver.FailedPrecondition("no usable zpool ONLINE on this node (configured: %v)", b.cfg.PoolNames())
	}
	return nil
}

// validatePools enforces the multi-pool config invariants at startup: at least
// one pool, bare (slash-free) unique names, and a default_pool that is a member.
func validatePools(cfg *config.LocalConfig) error {
	if len(cfg.Pools) == 0 {
		return driver.InvalidArgument("local requires at least one 'pool' block")
	}
	seen := make(map[string]bool, len(cfg.Pools))
	for _, p := range cfg.Pools {
		if p.Name == "" || strings.ContainsRune(p.Name, '/') {
			return driver.InvalidArgument("local pool name %q must be a bare zpool name (no '/')", p.Name)
		}
		if seen[p.Name] {
			return driver.InvalidArgument("duplicate local pool %q", p.Name)
		}
		seen[p.Name] = true
	}
	if cfg.DefaultPool == "" {
		return driver.InvalidArgument("local.default_pool is required")
	}
	if !seen[cfg.DefaultPool] {
		return driver.InvalidArgument("local.default_pool %q is not one of the configured pools %v", cfg.DefaultPool, cfg.PoolNames())
	}
	return nil
}

// buildResolver picks the peer-discovery strategy: an explicit static peer
// table is the opt-in override; otherwise discovery runs through Nomad's
// /v1/nodes API over the task API socket. There is no single-node fallback
// (Nomad discovery covers N=1) and no Consul. A NomadResolver that cannot reach
// the task API (no socket / no token source) returns an error, so New exits
// non-zero and Nomad reschedules with a corrected identity block.
func buildResolver(cfg *config.LocalConfig, nodeID, forwardAddr string, log *zap.Logger) (cluster.Resolver, error) {
	if len(cfg.Peers) > 0 {
		peers := make([]cluster.NodeInfo, 0, len(cfg.Peers))
		selfPresent := false
		for _, p := range cfg.Peers {
			peers = append(peers, cluster.NodeInfo{Node: p.Node, Addr: p.Addr})
			if p.Node == nodeID {
				selfPresent = true
			}
		}
		// Fail fast: if this node isn't in its own static peer table, every
		// "owned by me" comparison misses and the node silently owns nothing,
		// forwarding operations meant for itself. Catch the misconfiguration at
		// startup instead.
		if !selfPresent {
			names := make([]string, 0, len(cfg.Peers))
			for _, p := range cfg.Peers {
				names = append(names, p.Node)
			}
			return nil, driver.InvalidArgument("--node-id %q is not present in the static peer table (peers: %v); each node's own id must appear in its peer list", nodeID, names)
		}
		return &cluster.StaticResolver{Self: nodeID, Peers: peers}, nil
	}

	nc := cfg.Nomad
	if nc == nil {
		nc = &config.NomadConfig{}
	}
	var ttl time.Duration
	if s := strings.TrimSpace(nc.CacheTTL); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return nil, driver.InvalidArgument("local.nomad.cache_ttl %q: %v", s, err)
		}
		ttl = d
	}
	secretsDir := os.Getenv("NOMAD_SECRETS_DIR")
	return cluster.NewNomadResolver(cluster.NomadOptions{
		Self:        nodeID,
		SocketPath:  orDefaultPath(nc.SocketPath, secretsDir, "api.sock"),
		TokenPath:   orDefaultPath(nc.TokenPath, secretsDir, "nomad_token"),
		Token:       nc.Token,
		Datacenter:  orDefault(nc.Datacenter, os.Getenv("NOMAD_DC")),
		NodeFilter:  nc.NodeFilter,
		ForwardPort: portOf(forwardAddr),
		CacheTTL:    ttl,
		Logger:      log,
	})
}

// orDefaultPath returns override if set, else dir/name when dir is non-empty,
// else "" (no NOMAD_SECRETS_DIR -> NewNomadResolver fails fast).
func orDefaultPath(override, dir, name string) string {
	if override != "" {
		return override
	}
	if dir == "" {
		return ""
	}
	return filepath.Join(dir, name)
}

func orDefault(v, def string) string {
	if v != "" {
		return v
	}
	return def
}

// portOf extracts the numeric port from a listen address like ":9602".
func portOf(addr string) int {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(p)
	return n
}
