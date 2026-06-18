package stats

import "time"

// Default cadences and limits, applied by withDefaults for any unset field. The
// HCL form and its parsing live in internal/config (ResolveStats); these are the
// resolved-runtime defaults so a Config built directly (e.g. in tests) is always
// usable.
const (
	DefaultInterval          = 60 * time.Second
	DefaultStatfsTimeout     = 30 * time.Second
	DefaultWalkInterval      = 5 * time.Minute
	DefaultWalkTimeout       = 10 * time.Minute
	DefaultStaleAfter        = 5 * time.Minute
	DefaultMaxFailureBackoff = 30 * time.Minute
	DefaultWalkWorkers       = 4
	DefaultWalkBuffer        = 4096
	DefaultAggregateInterval = 60 * time.Second
	DefaultQueryAddr         = ":9610"
	DefaultQueryTokenHeader  = "X-NCD-Query-Token"
	DefaultNamespace         = "default"
)

// Config is the resolved (parsed, defaulted) runtime configuration for the stats
// subsystem. The node half uses the cadence/walk fields; the controller half
// (Phase 2+) uses the Aggregate*/Query*/Metrics fields.
type Config struct {
	// Enabled is the master switch. When false the Registry is a complete no-op.
	Enabled bool

	// --- node side ---
	Interval          time.Duration // statfs cadence
	StatfsTimeout     time.Duration // watchdog per statfs call (hung-mount guard)
	WalkEnabled       bool          // run the file/dir tree walk
	WalkInterval      time.Duration // tree-walk cadence
	WalkWorkers       int           // shared walk pool size (the IO ceiling)
	WalkBuffer        int           // shared pending-directory backlog hint
	WalkTimeout       time.Duration // per-volume deadline on one full walk
	StaleAfter        time.Duration // a reading older than this is "stale"
	MaxFailureBackoff time.Duration // cap for exponential backoff after errors

	// --- controller side (used by the source/query/metrics layers) ---
	AggregateInterval time.Duration // qnap controller fan-out cadence
	MetricsPerVolume  bool          // emit per-volume gauges
	QueryAddr         string        // public query endpoint ("" disables)
	QueryToken        string        // bearer token; "" = open (no auth)
	QueryTokenHeader  string        // header carrying the token
	Namespace         string        // Nomad namespace for id resolution / metrics (default "default")
}

// withDefaults returns a copy with every zero/invalid field replaced by its
// default. Safe on a zero-value Config.
func (c Config) withDefaults() Config {
	if c.Interval <= 0 {
		c.Interval = DefaultInterval
	}
	if c.StatfsTimeout <= 0 {
		c.StatfsTimeout = DefaultStatfsTimeout
	}
	if c.WalkInterval <= 0 {
		c.WalkInterval = DefaultWalkInterval
	}
	if c.WalkWorkers <= 0 {
		c.WalkWorkers = DefaultWalkWorkers
	}
	if c.WalkBuffer <= 0 {
		c.WalkBuffer = DefaultWalkBuffer
	}
	if c.WalkTimeout <= 0 {
		c.WalkTimeout = DefaultWalkTimeout
	}
	if c.StaleAfter <= 0 {
		c.StaleAfter = DefaultStaleAfter
	}
	if c.MaxFailureBackoff <= 0 {
		c.MaxFailureBackoff = DefaultMaxFailureBackoff
	}
	if c.AggregateInterval <= 0 {
		c.AggregateInterval = DefaultAggregateInterval
	}
	if c.QueryTokenHeader == "" {
		c.QueryTokenHeader = DefaultQueryTokenHeader
	}
	if c.Namespace == "" {
		c.Namespace = DefaultNamespace
	}
	return c
}
