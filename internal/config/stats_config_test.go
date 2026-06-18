package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

// TestLoad_RenderedJobspecConfigs parses configs shaped exactly like what the
// localdev jobspec templates render, so a drift between the HCL field names and
// the struct tags (stats / qnap forward / nomad blocks) fails here rather than at
// deploy time.
func TestLoad_RenderedJobspecConfigs(t *testing.T) {
	cases := map[string]string{
		"local": `
local {
  default_pool   = "tank"
  pool "tank" {}
  forward_addr   = ":9602"
  forward_secret = "e2e-secret"
  nomad { cache_ttl = "30s" }
}
readiness { timeout = "20m" }
stats {
  interval      = "5s"
  walk_interval = "10s"
  walk_timeout  = "2m"
  stale_after   = "30s"
}
`,
		"qnap": `
qnap {
  base_url       = "https://nas.example:443"
  username       = "admin"
  password       = "pw"
  interfaces     = ["eth0"]
  portal         = "10.0.0.5"
  forward_secret = "e2e-secret-qnap"
  forward_addr   = ":9612"
  nomad { cache_ttl = "30s" }
}
metrics {
  enabled = true
  address = "0.0.0.0:9501"
}
readiness { timeout = "5m" }
stats {
  query_addr         = ":9611"
  aggregate_interval = "5s"
  interval           = "5s"
  walk_interval      = "10s"
  walk_timeout       = "2m"
  stale_after        = "30s"
}
`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.hcl")
			if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
			c, err := Load(p)
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			sc, err := c.ResolveStats()
			if err != nil {
				t.Fatalf("ResolveStats: %v", err)
			}
			if !sc.Enabled || sc.Interval != 5*time.Second || sc.WalkInterval != 10*time.Second {
				t.Fatalf("stats not resolved as expected: %+v", sc)
			}
			if name == "qnap" {
				if sc.QueryAddr != ":9611" || sc.AggregateInterval != 5*time.Second {
					t.Fatalf("qnap stats query/aggregate wrong: %+v", sc)
				}
				if c.QNAP.ForwardSecret != "e2e-secret-qnap" || c.QNAP.ForwardAddr != ":9612" {
					t.Fatalf("qnap forward config not parsed: %+v", c.QNAP)
				}
				if c.QNAP.Nomad == nil || c.QNAP.Nomad.CacheTTL != "30s" {
					t.Fatalf("qnap nomad block not parsed: %+v", c.QNAP.Nomad)
				}
			}
		})
	}
}

func TestResolveStats_DefaultsWhenAbsent(t *testing.T) {
	for _, c := range []*Config{nil, {}, {Stats: nil}} {
		got, err := c.ResolveStats()
		if err != nil {
			t.Fatalf("ResolveStats(%v): %v", c, err)
		}
		if !got.Enabled || !got.WalkEnabled || !got.MetricsPerVolume {
			t.Fatalf("defaults should be ON: %+v", got)
		}
		if got.Interval != stats.DefaultInterval || got.WalkInterval != stats.DefaultWalkInterval {
			t.Fatalf("default cadences wrong: %+v", got)
		}
		if got.QueryAddr != stats.DefaultQueryAddr || got.QueryTokenHeader != stats.DefaultQueryTokenHeader {
			t.Fatalf("default query settings wrong: %+v", got)
		}
	}
}

func TestResolveStats_ExplicitOverrides(t *testing.T) {
	got, err := (&Config{Stats: &StatsConfig{
		Enabled:          boolPtr(false),
		WalkEnabled:      boolPtr(false),
		MetricsPerVolume: boolPtr(false),
		Interval:         "10s",
		WalkInterval:     "2m",
		WalkWorkers:      8,
		WalkBuffer:       2048,
		QueryToken:       "secret",
		QueryTokenHeader: "X-Custom",
	}}).ResolveStats()
	if err != nil {
		t.Fatal(err)
	}
	if got.Enabled || got.WalkEnabled || got.MetricsPerVolume {
		t.Fatalf("explicit false toggles not honored: %+v", got)
	}
	if got.Interval != 10*time.Second || got.WalkInterval != 2*time.Minute {
		t.Fatalf("duration overrides wrong: %+v", got)
	}
	if got.WalkWorkers != 8 || got.WalkBuffer != 2048 {
		t.Fatalf("int overrides wrong: %+v", got)
	}
	if got.QueryToken != "secret" || got.QueryTokenHeader != "X-Custom" {
		t.Fatalf("query auth overrides wrong: %+v", got)
	}
}

func TestResolveStats_QueryAddrUnsetVsEmpty(t *testing.T) {
	// nil (unset) -> default listener
	got, _ := (&Config{Stats: &StatsConfig{}}).ResolveStats()
	if got.QueryAddr != stats.DefaultQueryAddr {
		t.Fatalf("unset query_addr = %q; want default %q", got.QueryAddr, stats.DefaultQueryAddr)
	}
	// explicit "" -> disabled
	got, _ = (&Config{Stats: &StatsConfig{QueryAddr: strPtr("")}}).ResolveStats()
	if got.QueryAddr != "" {
		t.Fatalf(`explicit empty query_addr should disable (""); got %q`, got.QueryAddr)
	}
	// explicit value -> honored
	got, _ = (&Config{Stats: &StatsConfig{QueryAddr: strPtr(":9999")}}).ResolveStats()
	if got.QueryAddr != ":9999" {
		t.Fatalf("explicit query_addr = %q; want :9999", got.QueryAddr)
	}
}

func TestResolveStats_BadDurationIsError(t *testing.T) {
	if _, err := (&Config{Stats: &StatsConfig{Interval: "nope"}}).ResolveStats(); err == nil {
		t.Fatal("expected error for malformed duration")
	}
	if _, err := (&Config{Stats: &StatsConfig{WalkTimeout: "-5m"}}).ResolveStats(); err == nil {
		t.Fatal("expected error for non-positive duration")
	}
}

// TestLoad_ExampleConfigs parses the committed examples/*-config.hcl through the
// real loader, so a doc example that no longer matches the config schema fails
// here. Paths are relative to this package dir (internal/config).
func TestLoad_ExampleConfigs(t *testing.T) {
	for _, rel := range []string{
		"../../examples/local-config.hcl",
		"../../examples/qnap-config.hcl",
	} {
		if _, err := os.Stat(rel); err != nil {
			t.Skipf("example %s not present: %v", rel, err)
		}
		c, err := Load(rel)
		if err != nil {
			t.Fatalf("Load(%s): %v", rel, err)
		}
		if _, err := c.ResolveStats(); err != nil {
			t.Fatalf("ResolveStats(%s): %v", rel, err)
		}
	}
}

func boolPtr(b bool) *bool    { return &b }
func strPtr(s string) *string { return &s }
