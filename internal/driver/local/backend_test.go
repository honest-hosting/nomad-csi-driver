package local

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
)

func TestBuildResolver_Precedence(t *testing.T) {
	t.Run("static peer table is the override", func(t *testing.T) {
		cfg := &config.LocalConfig{
			Nomad: &config.NomadConfig{SocketPath: "/x/api.sock", Token: "t"},
			Peers: []config.PeerConfig{
				{Node: "n1", Addr: "127.0.0.1:9602"},
				{Node: "n2", Addr: "127.0.0.1:9603"},
			},
		}
		res, err := buildResolver(cfg, "n1", ":9602", zap.NewNop())
		require.NoError(t, err)
		addr, err := res.Resolve(context.Background(), "n2")
		require.NoError(t, err)
		assert.Equal(t, "127.0.0.1:9603", addr)
	})

	t.Run("self absent from static peer table is a config error", func(t *testing.T) {
		cfg := &config.LocalConfig{
			Peers: []config.PeerConfig{
				{Node: "n1", Addr: "127.0.0.1:9602"},
				{Node: "n2", Addr: "127.0.0.1:9603"},
			},
		}
		_, err := buildResolver(cfg, "n3", ":9602", zap.NewNop())
		require.Error(t, err, "node-id not in its own peer table must fail fast")
	})

	t.Run("nomad resolver when no peers", func(t *testing.T) {
		cfg := &config.LocalConfig{Nomad: &config.NomadConfig{SocketPath: "/x/api.sock", Token: "t"}}
		res, err := buildResolver(cfg, "n1", ":9602", zap.NewNop())
		require.NoError(t, err)
		assert.Equal(t, "n1", res.LocalNode())
	})

	t.Run("fatal when no peers and no token source", func(t *testing.T) {
		t.Setenv("NOMAD_SECRETS_DIR", "")
		t.Setenv("NOMAD_TOKEN", "")
		_, err := buildResolver(&config.LocalConfig{}, "n1", ":9602", zap.NewNop())
		require.Error(t, err, "missing identity/socket must fail fast for reschedule")
	})

	t.Run("malformed cache_ttl is a config error", func(t *testing.T) {
		cfg := &config.LocalConfig{Nomad: &config.NomadConfig{SocketPath: "/x/api.sock", Token: "t", CacheTTL: "soon"}}
		_, err := buildResolver(cfg, "n1", ":9602", zap.NewNop())
		require.Error(t, err)
	})
}
