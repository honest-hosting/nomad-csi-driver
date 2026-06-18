package cluster

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"

	"go.uber.org/zap"
)

// defaultCacheTTL is how long a fetched node roster is reused before refresh.
const defaultCacheTTL = 5 * time.Minute

// NomadResolver discovers peers from Nomad's own /v1/nodes API over the task
// API unix socket (api.sock). It is read-only: the node name embedded in a
// volume's external_id is matched against /v1/nodes[].Name, and the forward
// address is that node's advertised Address joined with the (cluster-uniform)
// forward port. No registration, no node metadata — only node:read.
//
// Results are cached for CacheTTL with refresh-on-miss (an unknown node forces
// one fresh fetch before erroring) and stale-serve (a refresh error falls back
// to the last good roster), so a joining/leaving node is picked up without any
// long-poll machinery.
type NomadResolver struct {
	self        string
	datacenter  string
	nodeFilter  string
	forwardPort int
	ttl         time.Duration
	h           *nomadHTTP
	log         *zap.Logger

	mu        sync.Mutex
	cache     []NodeInfo
	fetchedAt time.Time
}

// NomadOptions configures a NomadResolver. SocketPath and a token source
// (TokenPath file, Token literal, or the NOMAD_TOKEN env) are required —
// NewNomadResolver fails fast otherwise, since that means the plugin task is
// missing its `identity` block and could never reach api.sock.
type NomadOptions struct {
	Self        string        // this node's name (== /v1/nodes[].Name, == --node-id)
	SocketPath  string        // ${NOMAD_SECRETS_DIR}/api.sock
	TokenPath   string        // ${NOMAD_SECRETS_DIR}/nomad_token (re-read per refresh)
	Token       string        // literal bearer override (rare)
	Datacenter  string        // $NOMAD_DC
	NodeFilter  string        // optional server-side /v1/nodes filter expression
	ForwardPort int           // cluster-uniform forward port, joined to each peer's Address
	CacheTTL    time.Duration // default 5m
	Logger      *zap.Logger
}

// NewNomadResolver builds a NomadResolver and validates that it can actually
// reach the task API: a socket path plus a resolvable bearer token. A missing
// token source (no file, no literal, no env) is a hard error — the local
// backend turns it into a non-zero exit so Nomad reschedules with a corrected
// `identity` block.
func NewNomadResolver(opts NomadOptions) (*NomadResolver, error) {
	log := opts.Logger
	if log == nil {
		log = zap.NewNop()
	}
	ttl := opts.CacheTTL
	if ttl <= 0 {
		ttl = defaultCacheTTL
	}
	h, err := newNomadHTTP(opts.SocketPath, opts.TokenPath, opts.Token, log)
	if err != nil {
		return nil, err
	}
	r := &NomadResolver{
		self:        opts.Self,
		datacenter:  opts.Datacenter,
		nodeFilter:  opts.NodeFilter,
		forwardPort: opts.ForwardPort,
		ttl:         ttl,
		h:           h,
		log:         log,
	}
	log.Debug("nomad: resolver configured",
		zap.String("socket", opts.SocketPath), zap.String("datacenter", r.datacenter),
		zap.Int("forward_port", r.forwardPort), zap.Bool("node_filter", r.nodeFilter != ""),
		zap.Duration("cache_ttl", r.ttl))
	return r, nil
}

// LocalNode returns this node's name.
func (r *NomadResolver) LocalNode() string { return r.self }

// List returns the peer roster, served from cache when fresh; a stale cache is
// refreshed, and a refresh error falls back to the last good roster.
func (r *NomadResolver) List(ctx context.Context) ([]NodeInfo, error) {
	r.mu.Lock()
	cache, fetchedAt := r.cache, r.fetchedAt
	r.mu.Unlock()
	if cache != nil && time.Since(fetchedAt) < r.ttl {
		return cache, nil
	}
	nodes, err := r.refresh(ctx)
	if err != nil {
		if cache != nil {
			r.log.Warn("nomad: roster refresh failed; serving stale cache",
				zap.Int("peers", len(cache)), zap.Error(err))
			return cache, nil
		}
		return nil, err
	}
	return nodes, nil
}

// Resolve returns the forward address for node. On a cache miss it forces one
// fresh fetch (a just-joined node) before giving up.
func (r *NomadResolver) Resolve(ctx context.Context, node string) (string, error) {
	peers, err := r.List(ctx)
	if err != nil {
		return "", err
	}
	if addr, ok := findPeer(peers, node); ok {
		return addr, nil
	}
	// Miss: the node may have just joined and we served a fresh-but-stale cache.
	peers, err = r.refresh(ctx)
	if err != nil {
		return "", err
	}
	if addr, ok := findPeer(peers, node); ok {
		return addr, nil
	}
	return "", fmt.Errorf("cluster: node %q not in datacenter %q roster", node, r.datacenter)
}

func findPeer(peers []NodeInfo, node string) (string, bool) {
	for _, p := range peers {
		if p.Node == node {
			return p.Addr, true
		}
	}
	return "", false
}

// nodeStub is the subset of a /v1/nodes list entry we read.
type nodeStub struct {
	ID         string
	Name       string
	Address    string
	Status     string
	Datacenter string
}

// refresh fetches the roster from /v1/nodes and replaces the cache on success.
func (r *NomadResolver) refresh(ctx context.Context) ([]NodeInfo, error) {
	path := "/v1/nodes"
	if r.nodeFilter != "" {
		path += "?filter=" + url.QueryEscape(r.nodeFilter)
	}
	r.log.Debug("nomad: listing nodes", zap.String("datacenter", r.datacenter))
	var stubs []nodeStub
	if err := r.h.getJSON(ctx, path, &stubs); err != nil {
		return nil, err
	}

	// Apply the well-supported filters client-side (the Address field on the
	// stub is not server-filterable, and we want them enforced regardless of
	// whether an operator NodeFilter was sent): same datacenter, ready, with a
	// usable address.
	out := make([]NodeInfo, 0, len(stubs))
	for _, s := range stubs {
		if r.datacenter != "" && s.Datacenter != r.datacenter {
			continue
		}
		if s.Status != "ready" {
			continue
		}
		if s.Address == "" {
			r.log.Warn("nomad: node has no address; skipping", zap.String("node", s.Name))
			continue
		}
		out = append(out, NodeInfo{Node: s.Name, Addr: net.JoinHostPort(s.Address, fmt.Sprint(r.forwardPort))})
	}

	r.mu.Lock()
	r.cache, r.fetchedAt = out, time.Now()
	r.mu.Unlock()
	r.log.Debug("nomad: roster refreshed", zap.Int("peers", len(out)))
	return out, nil
}
