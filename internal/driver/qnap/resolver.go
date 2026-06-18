package qnap

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/cluster"
	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// nomadOptions builds the shared Nomad task-API options for the controller's
// stats fan-out: peer discovery (NomadResolver) and volume-id resolution
// (NomadVolumes) over the task API socket + workload identity. There is no static
// peer table — discovery via the task API is the only mechanism.
func nomadOptions(cfg *config.QNAPConfig, nodeID, forwardAddr string, log *zap.Logger) (cluster.NomadOptions, error) {
	nc := cfg.Nomad
	if nc == nil {
		nc = &config.NomadConfig{}
	}
	var ttl time.Duration
	if s := strings.TrimSpace(nc.CacheTTL); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return cluster.NomadOptions{}, driver.InvalidArgument("qnap.nomad.cache_ttl %q: %v", s, err)
		}
		ttl = d
	}
	secretsDir := os.Getenv("NOMAD_SECRETS_DIR")
	return cluster.NomadOptions{
		Self:        nodeID,
		SocketPath:  orDefaultPath(nc.SocketPath, secretsDir, "api.sock"),
		TokenPath:   orDefaultPath(nc.TokenPath, secretsDir, "nomad_token"),
		Token:       nc.Token,
		Datacenter:  orDefault(nc.Datacenter, os.Getenv("NOMAD_DC")),
		NodeFilter:  nc.NodeFilter,
		ForwardPort: portOf(forwardAddr),
		CacheTTL:    ttl,
		Logger:      log,
	}, nil
}

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

func portOf(addr string) int {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(p)
	return n
}
