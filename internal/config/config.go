// Package config loads the controller/node deployment configuration — the file
// the operator hands the plugin via --config. This is NOT the per-volume CSI
// volume spec (that is the standard Nomad CSI surface and is never redefined
// here). The format is HCL with a JSON fallback (HCL's native JSON variant);
// hclsimple selects the parser by file extension (.json -> JSON, else HCL).
package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/hcl/v2/hclsimple"

	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

// Config is the top-level deployment config. Both backend blocks are optional;
// only the one matching the active --driver is required (validated by the
// backend at startup).
type Config struct {
	QNAP      *QNAPConfig      `hcl:"qnap,block"`
	Local     *LocalConfig     `hcl:"local,block"`
	Metrics   *MetricsConfig   `hcl:"metrics,block"`
	Readiness *ReadinessConfig `hcl:"readiness,block"`
	Stats     *StatsConfig     `hcl:"stats,block"`
}

// QNAPConfig holds the --driver=qnap controller/node settings. Credentials live
// here (sourced from a file Nomad mounts), never on argv.
type QNAPConfig struct {
	// BaseURL is required for controller mode (validated at startup); node mode
	// reads everything it needs from the CSI volume context, so its config may
	// omit it.
	BaseURL  string `hcl:"base_url,optional"`
	Username string `hcl:"username,optional"`
	Password string `hcl:"password,optional"`
	Insecure bool   `hcl:"insecure,optional"`
	// Portal is the iSCSI portal (host or host:port) nodes run discovery
	// against. Port defaults to 3260 when omitted. For multipath, prefer Portals.
	Portal string `hcl:"portal,optional"`
	// Portals lists the iSCSI portals for multipath: nodes log into ALL of them,
	// so the LUN is reached over one path per portal (two NICs/subnets → two
	// paths). Takes precedence over Portal; a single Portal is an equivalent
	// one-element list (single path, still multipath-ready).
	Portals       []string `hcl:"portals,optional"`
	DefaultPoolID int      `hcl:"default_pool_id,optional"`
	Interfaces    []string `hcl:"interfaces,optional"`
	// DefaultSectorSize is the LUN sector size when a volume omits it (512;
	// 4096 is Windows-only and unsupported).
	DefaultSectorSize int `hcl:"default_sector_size,optional"`
	// DisableMultipath skips multipath assembly on the node (raw device only).
	DisableMultipath bool `hcl:"disable_multipath,optional"`
	// MultipathConfigDir overrides the multipath drop-in directory.
	MultipathConfigDir string `hcl:"multipath_config_dir,optional"`
	// NodeStateDir holds per-volume stage metadata the node writes at
	// NodeStageVolume and reads at NodeUnstageVolume (which carries no volume
	// context). Defaults to /var/lib/nomad-csi-driver/qnap.
	NodeStateDir string `hcl:"node_state_dir,optional"`
	// DebugHTTP wraps the QNAP transport to log every raw request path +
	// response body at debug level (the in-driver equivalent of
	// `qnapctl --debug-http`). Verbose and may include appliance data — enable
	// only for troubleshooting, and run with --log-level=debug to see it.
	DebugHTTP bool `hcl:"debug_http,optional"`

	// --- per-volume stats forwarding (controller fan-out → node daemons) ---
	// These mirror the local backend's forwarding transport. They are only used
	// by the usage-stats subsystem: the node runs a forwarding server, and the
	// controller fans out to all nodes to aggregate readings. Unset = the stats
	// query API / central /metrics for qnap are disabled (node-local hydration
	// still runs).

	// ForwardSecret is the shared secret authenticating stats forwards between the
	// controller and node daemons. Required to enable the qnap stats fan-out.
	ForwardSecret string `hcl:"forward_secret,optional"`
	// ForwardAddr is the address a node's stats forwarding server listens on
	// (host:port), reachable by the controller. Cluster-uniform. Defaults to ":9602".
	ForwardAddr string `hcl:"forward_addr,optional"`
	// Nomad tunes node discovery via the Nomad task API (api.sock) on the
	// controller. Optional; the zero value resolves api.sock + token from
	// NOMAD_SECRETS_DIR and scopes to $NOMAD_DC.
	Nomad *NomadConfig `hcl:"nomad,block"`
}

// PortalList returns the effective iSCSI portals: Portals if set, otherwise the
// single Portal, otherwise empty. Entries are trimmed; ports are NOT normalized
// here (the node/controller appends :3260 as needed).
func (q *QNAPConfig) PortalList() []string {
	raw := q.Portals
	if len(raw) == 0 && q.Portal != "" {
		raw = []string{q.Portal}
	}
	out := make([]string, 0, len(raw))
	for _, p := range raw {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// LocalConfig holds the --driver=local settings.
type LocalConfig struct {
	// Pools is the allowlist of zpools this node may carve zvols from (each a
	// "pool <name> {}" block). Required (>=1). Pools are referenced by NAME; the
	// driver never creates them (backing — device/file/RAID — is operator owned)
	// A node may legitimately have only a subset imported.
	Pools []PoolConfig `hcl:"pool,block"`
	// DefaultPool is used when a volume omits parameters.pool. Required and MUST
	// be a member of Pools (validated at startup; fatal otherwise).
	DefaultPool string `hcl:"default_pool"`
	// DefaultVolblocksize applies when a volume omits volblocksize (e.g. "16K").
	DefaultVolblocksize string `hcl:"default_volblocksize,optional"`
	// ReservePercent is the global free-space safety margin: refuse a create that
	// would push a pool below this fraction free (a per-pool override lives on
	// PoolConfig). 0 = no reserve. This is agent config, never a volume parameter.
	ReservePercent int `hcl:"reserve_percent,optional"`
	// Nomad tunes peer discovery via the Nomad task API (api.sock). It is
	// optional: when omitted, discovery runs with defaults. Discovery via the
	// task API + workload identity is the ONLY peer-discovery mechanism for the
	// L4 forwarding layer (the plugin task therefore requires an `identity`
	// block, and `node:read` with ACLs on).
	Nomad *NomadConfig `hcl:"nomad,block"`
	// ForwardAddr is the address this node's forwarding server listens on
	// (host:port), reachable by peer controllers. Defaults to ":9602".
	ForwardAddr string `hcl:"forward_addr,optional"`
	// ForwardSecret is the shared secret peers use to authenticate forwards
	// (v1; mTLS is later hardening).
	ForwardSecret string `hcl:"forward_secret,optional"`
}

// PoolConfig is one usable zpool in the local allowlist. The block label is the
// bare zpool name; the backing (device/file/RAID) is opaque to the driver.
type PoolConfig struct {
	Name string `hcl:"name,label"`
	// ParentDataset under Name that holds provisioned zvols. Defaults to
	// "nomad-csi" (i.e. <name>/nomad-csi) when omitted.
	ParentDataset string `hcl:"parent_dataset,optional"`
	// ReservePercent overrides LocalConfig.ReservePercent for this pool. nil =
	// inherit the global default.
	ReservePercent *int `hcl:"reserve_percent,optional"`
}

// PoolByName returns the configured pool of the given name, and whether it
// exists in the allowlist.
func (c *LocalConfig) PoolByName(name string) (PoolConfig, bool) {
	for _, p := range c.Pools {
		if p.Name == name {
			return p, true
		}
	}
	return PoolConfig{}, false
}

// PoolNames returns the configured pool names, for error messages.
func (c *LocalConfig) PoolNames() []string {
	names := make([]string, 0, len(c.Pools))
	for _, p := range c.Pools {
		names = append(names, p.Name)
	}
	return names
}

// ReserveFor returns the reserve percent for a pool: its per-pool override if
// set, else the global default.
func (c *LocalConfig) ReserveFor(name string) int {
	if p, ok := c.PoolByName(name); ok && p.ReservePercent != nil {
		return *p.ReservePercent
	}
	return c.ReservePercent
}

// NomadConfig tunes peer discovery via the Nomad task API unix socket. Every
// field is optional; the zero value resolves api.sock and the workload-identity
// token from the task's NOMAD_SECRETS_DIR and scopes to $NOMAD_DC. The plugin
// task must carry an `identity` block (and, with ACLs on, a node:read policy)
// for the socket to be usable.
type NomadConfig struct {
	// SocketPath overrides the task API socket. Default ${NOMAD_SECRETS_DIR}/api.sock.
	SocketPath string `hcl:"socket_path,optional"`
	// TokenPath is the workload-identity token file, re-read on each refresh so
	// rotation is picked up. Default ${NOMAD_SECRETS_DIR}/nomad_token.
	TokenPath string `hcl:"token_path,optional"`
	// Token is a literal bearer token override (rare; prefer the file). Used only
	// if the file is absent.
	Token string `hcl:"token,optional"`
	// Datacenter scopes the node roster. Default $NOMAD_DC.
	Datacenter string `hcl:"datacenter,optional"`
	// NodeFilter is an optional Nomad filter expression applied server-side to
	// /v1/nodes (e.g. `NodeClass == "storage"`), for jobs constrained to a subset
	// of clients. Empty = all ready nodes in the datacenter.
	NodeFilter string `hcl:"node_filter,optional"`
	// CacheTTL is how long a fetched roster is reused before refresh. Default 5m.
	CacheTTL string `hcl:"cache_ttl,optional"`
}

// Default Prometheus endpoint settings, applied when metrics are enabled but the
// field is omitted. Controller and node configs are independent (separate jobs),
// so each can override the address (required when co-located under host
// networking, since they'd otherwise share a port).
const (
	DefaultMetricsAddress = "0.0.0.0:9090"
	DefaultMetricsPath    = "/metrics"
)

// MetricsConfig configures the plugin's own Prometheus endpoint (scraped
// directly, not via Nomad telemetry). Metrics are OFF unless Enabled is true.
type MetricsConfig struct {
	Enabled bool   `hcl:"enabled,optional"` // default false — metrics off
	Address string `hcl:"address,optional"` // default 0.0.0.0:9090 when enabled
	Path    string `hcl:"path,optional"`    // default /metrics
}

// EffectiveAddress returns the listen address, defaulting when omitted.
func (m *MetricsConfig) EffectiveAddress() string {
	if m.Address != "" {
		return m.Address
	}
	return DefaultMetricsAddress
}

// EffectivePath returns the scrape path, defaulting when omitted.
func (m *MetricsConfig) EffectivePath() string {
	if m.Path != "" {
		return m.Path
	}
	return DefaultMetricsPath
}

// DefaultReadinessInterval is the delay between startup readiness probes when
// not overridden.
const DefaultReadinessInterval = 5 * time.Second

// ReadinessConfig gates startup: before serving the CSI socket, the plugin
// probes the backend (for local: at least one allowlisted zpool imported +
// ONLINE; for the qnap controller: a live appliance session) and retries until
// ready or Timeout elapses. If it never becomes ready the process exits non-zero
// so Nomad reschedules it — it never serves a socket it cannot actually back.
//
// Timeout = 0 (the default) means a single attempt: fail fast and let Nomad's
// reschedule/backoff retry the whole alloc. A non-zero Timeout retries in-process
// (gentler than alloc churn) — useful where the backing store can take a while to
// come up, e.g. a node where the zpool is still being created/formatted.
type ReadinessConfig struct {
	Timeout  string `hcl:"timeout,optional"`  // total wait, e.g. "20m"; "" / "0" = single attempt
	Interval string `hcl:"interval,optional"` // delay between attempts; default 5s
}

// ResolveReadiness returns the parsed (timeout, interval) for the startup gate,
// applying defaults. It is nil-safe (no readiness block → timeout 0, interval
// 5s). A malformed duration is a hard config error.
func (c *Config) ResolveReadiness() (timeout, interval time.Duration, err error) {
	interval = DefaultReadinessInterval
	if c == nil || c.Readiness == nil {
		return 0, interval, nil
	}
	if s := strings.TrimSpace(c.Readiness.Timeout); s != "" {
		if timeout, err = time.ParseDuration(s); err != nil {
			return 0, 0, fmt.Errorf("config: readiness.timeout %q: %w", s, err)
		}
	}
	if s := strings.TrimSpace(c.Readiness.Interval); s != "" {
		if interval, err = time.ParseDuration(s); err != nil {
			return 0, 0, fmt.Errorf("config: readiness.interval %q: %w", s, err)
		}
	}
	if interval <= 0 {
		interval = DefaultReadinessInterval
	}
	return timeout, interval, nil
}

// StatsConfig is the HCL form of the per-volume usage-stats subsystem (see
// internal/stats). All fields are optional; ResolveStats applies defaults. The
// three toggles are *bool so an omitted value defaults ON (nil) while an explicit
// false disables. QueryAddr is *string so an omitted value defaults to :9610
// while an explicit "" disables the query endpoint.
type StatsConfig struct {
	Enabled           *bool   `hcl:"enabled,optional"`             // default ON
	Interval          string  `hcl:"interval,optional"`            // statfs cadence; default 60s
	StatfsTimeout     string  `hcl:"statfs_timeout,optional"`      // hung-mount watchdog; default 30s
	WalkEnabled       *bool   `hcl:"walk_enabled,optional"`        // default ON
	WalkInterval      string  `hcl:"walk_interval,optional"`       // tree-walk cadence; default 5m
	WalkWorkers       int     `hcl:"walk_workers,optional"`        // shared pool size; default 4
	WalkBuffer        int     `hcl:"walk_buffer,optional"`         // backlog hint; default 4096
	WalkTimeout       string  `hcl:"walk_timeout,optional"`        // per-volume walk deadline; default 10m
	StaleAfter        string  `hcl:"stale_after,optional"`         // staleness threshold; default 5m
	MaxFailureBackoff string  `hcl:"max_failure_backoff,optional"` // backoff cap; default 30m
	AggregateInterval string  `hcl:"aggregate_interval,optional"`  // qnap controller fan-out; default 60s
	MetricsPerVolume  *bool   `hcl:"metrics_per_volume,optional"`  // default ON
	QueryAddr         *string `hcl:"query_addr,optional"`          // default :9610; "" disables
	QueryToken        string  `hcl:"query_token,optional"`         // "" / unset = open (no auth)
	QueryTokenHeader  string  `hcl:"query_token_header,optional"`  // default X-NCD-Query-Token
	Namespace         string  `hcl:"namespace,optional"`           // Nomad namespace for id resolution; default "default"
}

// ResolveStats returns the parsed, defaulted runtime stats config. It is
// nil-safe (no stats block → all defaults: enabled, walk on, metrics per-volume
// on). A malformed or non-positive duration is a hard config error.
func (c *Config) ResolveStats() (stats.Config, error) {
	out := stats.Config{
		Enabled:           true,
		Interval:          stats.DefaultInterval,
		StatfsTimeout:     stats.DefaultStatfsTimeout,
		WalkEnabled:       true,
		WalkInterval:      stats.DefaultWalkInterval,
		WalkWorkers:       stats.DefaultWalkWorkers,
		WalkBuffer:        stats.DefaultWalkBuffer,
		WalkTimeout:       stats.DefaultWalkTimeout,
		StaleAfter:        stats.DefaultStaleAfter,
		MaxFailureBackoff: stats.DefaultMaxFailureBackoff,
		AggregateInterval: stats.DefaultAggregateInterval,
		MetricsPerVolume:  true,
		QueryAddr:         stats.DefaultQueryAddr,
		QueryTokenHeader:  stats.DefaultQueryTokenHeader,
		Namespace:         stats.DefaultNamespace,
	}
	if c == nil || c.Stats == nil {
		return out, nil
	}
	s := c.Stats

	dur := func(name, v string, dst *time.Duration) error {
		if v = strings.TrimSpace(v); v == "" {
			return nil
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			return fmt.Errorf("config: stats.%s %q: %w", name, v, err)
		}
		if d <= 0 {
			return fmt.Errorf("config: stats.%s must be > 0, got %q", name, v)
		}
		*dst = d
		return nil
	}
	for _, f := range []struct {
		name string
		val  string
		dst  *time.Duration
	}{
		{"interval", s.Interval, &out.Interval},
		{"statfs_timeout", s.StatfsTimeout, &out.StatfsTimeout},
		{"walk_interval", s.WalkInterval, &out.WalkInterval},
		{"walk_timeout", s.WalkTimeout, &out.WalkTimeout},
		{"stale_after", s.StaleAfter, &out.StaleAfter},
		{"max_failure_backoff", s.MaxFailureBackoff, &out.MaxFailureBackoff},
		{"aggregate_interval", s.AggregateInterval, &out.AggregateInterval},
	} {
		if err := dur(f.name, f.val, f.dst); err != nil {
			return stats.Config{}, err
		}
	}

	if s.Enabled != nil {
		out.Enabled = *s.Enabled
	}
	if s.WalkEnabled != nil {
		out.WalkEnabled = *s.WalkEnabled
	}
	if s.MetricsPerVolume != nil {
		out.MetricsPerVolume = *s.MetricsPerVolume
	}
	if s.WalkWorkers > 0 {
		out.WalkWorkers = s.WalkWorkers
	}
	if s.WalkBuffer > 0 {
		out.WalkBuffer = s.WalkBuffer
	}
	if s.QueryAddr != nil { // explicit "" disables the endpoint
		out.QueryAddr = *s.QueryAddr
	}
	out.QueryToken = s.QueryToken
	if s.Namespace != "" {
		out.Namespace = s.Namespace
	}
	if s.QueryTokenHeader != "" {
		out.QueryTokenHeader = s.QueryTokenHeader
	}
	return out, nil
}

// Load reads and decodes the config at path. An empty path yields a zero Config
// (all settings then come from flags/env/defaults).
func Load(path string) (*Config, error) {
	cfg := &Config{}
	if path == "" {
		return cfg, nil
	}
	if err := hclsimple.DecodeFile(path, nil, cfg); err != nil {
		return nil, fmt.Errorf("config: decoding %s: %w", path, err)
	}
	return cfg, nil
}
