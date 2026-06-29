package cmd

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
)

// fakeProber fails its first failUntil probes, then succeeds.
type fakeProber struct {
	calls     int
	failUntil int
	err       error
}

func (p *fakeProber) Probe(context.Context) error {
	p.calls++
	if p.calls <= p.failUntil {
		if p.err != nil {
			return p.err
		}
		return errors.New("not ready")
	}
	return nil
}

func TestAwaitBackendReady(t *testing.T) {
	log := zap.NewNop()

	t.Run("ready on first attempt (single-attempt default)", func(t *testing.T) {
		p := &fakeProber{}
		require.NoError(t, awaitBackendReady(context.Background(), p, &config.Config{}, log))
		assert.Equal(t, 1, p.calls)
	})

	t.Run("zero timeout fails fast after one attempt", func(t *testing.T) {
		p := &fakeProber{failUntil: 100}
		err := awaitBackendReady(context.Background(), p, &config.Config{}, log)
		require.Error(t, err)
		assert.Equal(t, 1, p.calls, "zero timeout must not retry")
	})

	t.Run("retries until ready within timeout", func(t *testing.T) {
		p := &fakeProber{failUntil: 3}
		cfg := &config.Config{Readiness: &config.ReadinessConfig{Timeout: "5s", Interval: "1ms"}}
		require.NoError(t, awaitBackendReady(context.Background(), p, cfg, log))
		assert.Equal(t, 4, p.calls, "3 failures then a success")
	})

	t.Run("surfaces the last probe error when the timeout elapses", func(t *testing.T) {
		sentinel := errors.New("zpool unavailable")
		p := &fakeProber{failUntil: 100, err: sentinel}
		cfg := &config.Config{Readiness: &config.ReadinessConfig{Timeout: "10ms", Interval: "1ms"}}
		err := awaitBackendReady(context.Background(), p, cfg, log)
		require.Error(t, err)
		assert.ErrorIs(t, err, sentinel)
		assert.Greater(t, p.calls, 1, "a non-zero timeout must retry")
	})

	t.Run("aborts when the context is cancelled mid-wait", func(t *testing.T) {
		p := &fakeProber{failUntil: 100}
		cfg := &config.Config{Readiness: &config.ReadinessConfig{Timeout: "1h", Interval: "50ms"}}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := awaitBackendReady(ctx, p, cfg, log)
		require.Error(t, err)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("rejects a malformed readiness duration", func(t *testing.T) {
		p := &fakeProber{}
		bad := &config.Config{Readiness: &config.ReadinessConfig{Timeout: "not-a-duration"}}
		require.Error(t, awaitBackendReady(context.Background(), p, bad, log))
		assert.Equal(t, 0, p.calls, "config error must short-circuit before probing")
	})
}

// startMetricsServer must be OFF unless explicitly enabled, and serve on the
// configured (or default) address when enabled.
func TestStartMetricsServer_Gating(t *testing.T) {
	m := metrics.New("test", "monolith", "node-test", "plugin-test")
	log := zap.NewNop()

	// No metrics block → off.
	assert.Nil(t, startMetricsServer(&config.Config{}, m, log), "no metrics block → off")

	// Block present but disabled → off (default).
	off := &config.Config{Metrics: &config.MetricsConfig{Enabled: false}}
	assert.Nil(t, startMetricsServer(off, m, log), "enabled=false → off")

	// Enabled → a server bound to the configured address (ephemeral port for the test).
	on := &config.Config{Metrics: &config.MetricsConfig{Enabled: true, Address: "127.0.0.1:0"}}
	hs := startMetricsServer(on, m, log)
	require.NotNil(t, hs, "enabled=true → server")
	assert.Equal(t, "127.0.0.1:0", hs.Addr)
	shutdownMetricsServer(hs, log)
}
