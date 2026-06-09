// Package cluster provides the L4 coordination primitives for the local
// backend: peer discovery (so a controller can find the node that owns a
// volume) and an authenticated forwarding transport (so a controller that
// receives a topology-blind RPC can forward it to the owning node). Discovery
// reads Nomad's own /v1/nodes API over the task API unix socket (see
// NomadResolver) — no external service. A StaticResolver covers fixed tables
// and tests.
package cluster

import (
	"context"
	"fmt"
)

// NodeInfo is a peer driver instance: the Nomad node name and the address of
// its forwarding server.
type NodeInfo struct {
	Node string
	Addr string // host:port of the peer's forward server
}

// Resolver discovers peer driver instances and the local node's identity.
type Resolver interface {
	// LocalNode returns this process's Nomad node name.
	LocalNode() string
	// List returns all known peer instances (including self).
	List(ctx context.Context) ([]NodeInfo, error)
	// Resolve returns the forward address for a specific node.
	Resolve(ctx context.Context, node string) (string, error)
}

// StaticResolver is a fixed peer table — used as the opt-in discovery override
// (hard-coded addresses / running outside Nomad) and in tests.
type StaticResolver struct {
	Self  string
	Peers []NodeInfo
}

// LocalNode returns the configured local node name.
func (s *StaticResolver) LocalNode() string { return s.Self }

// List returns the fixed peer table.
func (s *StaticResolver) List(context.Context) ([]NodeInfo, error) { return s.Peers, nil }

// Resolve returns the forward address for node, or an error if unknown.
func (s *StaticResolver) Resolve(_ context.Context, node string) (string, error) {
	for _, p := range s.Peers {
		if p.Node == node {
			return p.Addr, nil
		}
	}
	return "", fmt.Errorf("cluster: no peer for node %q", node)
}
