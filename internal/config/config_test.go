package config

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetricsConfigDefaults(t *testing.T) {
	// Omitted address/path fall back to the documented defaults.
	empty := &MetricsConfig{Enabled: true}
	assert.Equal(t, "0.0.0.0:9090", empty.EffectiveAddress())
	assert.Equal(t, "/metrics", empty.EffectivePath())

	// Explicit values win (independent per controller/node config).
	custom := &MetricsConfig{Enabled: true, Address: ":9501", Path: "/m"}
	assert.Equal(t, ":9501", custom.EffectiveAddress())
	assert.Equal(t, "/m", custom.EffectivePath())
}

func TestResolveReadiness(t *testing.T) {
	// nil config / nil block → fail-fast default (timeout 0, interval 5s).
	to, iv, err := (*Config)(nil).ResolveReadiness()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), to)
	assert.Equal(t, DefaultReadinessInterval, iv)

	to, iv, err = (&Config{}).ResolveReadiness()
	require.NoError(t, err)
	assert.Equal(t, time.Duration(0), to)
	assert.Equal(t, DefaultReadinessInterval, iv)

	// Explicit durations parse.
	to, iv, err = (&Config{Readiness: &ReadinessConfig{Timeout: "20m", Interval: "10s"}}).ResolveReadiness()
	require.NoError(t, err)
	assert.Equal(t, 20*time.Minute, to)
	assert.Equal(t, 10*time.Second, iv)

	// A non-positive interval falls back to the default.
	_, iv, err = (&Config{Readiness: &ReadinessConfig{Interval: "0s"}}).ResolveReadiness()
	require.NoError(t, err)
	assert.Equal(t, DefaultReadinessInterval, iv)

	// Malformed durations are hard errors.
	_, _, err = (&Config{Readiness: &ReadinessConfig{Timeout: "soon"}}).ResolveReadiness()
	require.Error(t, err)
	_, _, err = (&Config{Readiness: &ReadinessConfig{Interval: "5"}}).ResolveReadiness()
	require.Error(t, err)
}

func TestLoadEmptyPath(t *testing.T) {
	cfg, err := Load("")
	require.NoError(t, err)
	assert.NotNil(t, cfg)
	assert.Nil(t, cfg.QNAP)
	assert.Nil(t, cfg.Local)
}

// TestLoadExampleConfigs guards the examples/*.hcl configs against schema drift:
// they must decode against the current config structs.
func TestLoadExampleConfigs(t *testing.T) {
	t.Run("qnap", func(t *testing.T) {
		cfg, err := Load(filepath.Join("..", "..", "examples", "qnap-config.hcl"))
		require.NoError(t, err)
		require.NotNil(t, cfg.QNAP)
		assert.Equal(t, "https://qnap.example.com", cfg.QNAP.BaseURL)
		assert.Equal(t, []string{"eth0"}, cfg.QNAP.Interfaces)
	})
	t.Run("local", func(t *testing.T) {
		cfg, err := Load(filepath.Join("..", "..", "examples", "local-config.hcl"))
		require.NoError(t, err)
		require.NotNil(t, cfg.Local)
		assert.Equal(t, "tank", cfg.Local.DefaultPool)
		_, ok := cfg.Local.PoolByName("tank")
		assert.True(t, ok)
		require.NotNil(t, cfg.Local.Nomad)
		assert.Equal(t, "5m", cfg.Local.Nomad.CacheTTL)
	})
}
