// Package csi is the shared, backend-agnostic CSI gRPC scaffolding. It builds
// the gRPC server, registers the Identity service plus whichever of
// Controller/Node the process mode calls for, and dispatches every RPC to the
// active driver.Backend. Protobuf translation lives in conv.go; error mapping
// in errors.go; cross-cutting concerns in interceptors.go.
package csi

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
)

// Server wraps a gRPC server bound to a Unix socket, serving the CSI services
// appropriate for the backend and mode.
type Server struct {
	grpc     *grpc.Server
	lis      net.Listener
	sockPath string
	log      *zap.Logger
}

// NewServer constructs a CSI server for the backend in the given mode, bound to
// endpoint (a unix:// URI). It registers Identity always, Controller when the
// mode runs the controller half, and Node when the mode runs the node half.
func NewServer(b driver.Backend, mode driver.Mode, endpoint string, log *zap.Logger, m *metrics.Metrics) (*Server, error) {
	if log == nil {
		log = zap.NewNop()
	}
	sockPath, err := parseUnixEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	// Ensure the socket's parent directory exists. Under containerized task
	// drivers Nomad bind-mounts it in; under raw_exec it may not pre-exist.
	if err := os.MkdirAll(filepath.Dir(sockPath), 0o750); err != nil {
		return nil, fmt.Errorf("creating socket directory for %s: %w", sockPath, err)
	}
	// A stale socket from a previous run would make Listen fail with EADDRINUSE.
	if err := os.Remove(sockPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("removing stale socket %s: %w", sockPath, err)
	}
	gs, err := newGRPCServer(b, mode, log, m)
	if err != nil {
		return nil, err
	}

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return nil, fmt.Errorf("listening on %s: %w", sockPath, err)
	}

	log.Info("csi server configured",
		zap.String("driver", b.Name()), zap.String("mode", string(mode)),
		zap.String("endpoint", sockPath))
	return &Server{grpc: gs, lis: lis, sockPath: sockPath, log: log}, nil
}

// newGRPCServer builds the gRPC server and registers Identity plus whichever of
// Controller/Node the mode requires. Shared by NewServer and tests (which serve
// it over an in-memory listener).
func newGRPCServer(b driver.Backend, mode driver.Mode, log *zap.Logger, m *metrics.Metrics) (*grpc.Server, error) {
	gs := grpc.NewServer(grpc.UnaryInterceptor(unaryInterceptor(log, m)))

	// Identity is always served.
	csipb.RegisterIdentityServer(gs, newIdentityServer(b))

	if mode.HasController() {
		if b.Controller() == nil {
			return nil, fmt.Errorf("mode %q needs a controller but backend %q has none", mode, b.Name())
		}
		csipb.RegisterControllerServer(gs, newControllerServer(b))
	}
	if mode.HasNode() {
		if b.Node() == nil {
			return nil, fmt.Errorf("mode %q needs a node but backend %q has none", mode, b.Name())
		}
		csipb.RegisterNodeServer(gs, newNodeServer(b))
	}
	return gs, nil
}

// Serve blocks serving RPCs until the server is stopped.
func (s *Server) Serve() error { return s.grpc.Serve(s.lis) }

// GracefulStop stops accepting new RPCs and waits for in-flight ones to finish.
func (s *Server) GracefulStop() {
	s.grpc.GracefulStop()
	_ = os.Remove(s.sockPath)
}

// Stop forcefully stops the server.
func (s *Server) Stop() {
	s.grpc.Stop()
	_ = os.Remove(s.sockPath)
}

// parseUnixEndpoint extracts the socket path from a unix:// endpoint URI.
// Accepts unix:///abs/path and unix://./rel/path forms.
func parseUnixEndpoint(endpoint string) (string, error) {
	const scheme = "unix://"
	if !strings.HasPrefix(endpoint, scheme) {
		return "", fmt.Errorf("endpoint %q must be a unix:// URI", endpoint)
	}
	path := strings.TrimPrefix(endpoint, scheme)
	if path == "" {
		return "", fmt.Errorf("endpoint %q has empty socket path", endpoint)
	}
	return path, nil
}
