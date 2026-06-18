package local

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/cluster"
	"github.com/honest-hosting/nomad-csi-driver/internal/config"
)

// TestNomadOptionsAndResolver covers the (now sole) Nomad workload-identity
// discovery path: there is no static peer table anymore.
func TestNomadOptionsAndResolver(t *testing.T) {
	t.Run("resolver built from options", func(t *testing.T) {
		cfg := &config.LocalConfig{Nomad: &config.NomadConfig{SocketPath: "/x/api.sock", Token: "t"}}
		opts, err := nomadOptions(cfg, "n1", ":9602", zap.NewNop())
		require.NoError(t, err)
		res, err := cluster.NewNomadResolver(opts)
		require.NoError(t, err)
		assert.Equal(t, "n1", res.LocalNode())
	})

	t.Run("fatal when no token source (missing identity)", func(t *testing.T) {
		t.Setenv("NOMAD_SECRETS_DIR", "")
		t.Setenv("NOMAD_TOKEN", "")
		opts, err := nomadOptions(&config.LocalConfig{}, "n1", ":9602", zap.NewNop())
		require.NoError(t, err)
		_, err = cluster.NewNomadResolver(opts)
		require.Error(t, err, "missing identity/socket must fail fast so Nomad reschedules")
	})

	t.Run("malformed cache_ttl is a config error", func(t *testing.T) {
		cfg := &config.LocalConfig{Nomad: &config.NomadConfig{SocketPath: "/x/api.sock", Token: "t", CacheTTL: "soon"}}
		_, err := nomadOptions(cfg, "n1", ":9602", zap.NewNop())
		require.Error(t, err)
	})
}
