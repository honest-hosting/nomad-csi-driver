package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
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
	socketPath  string
	tokenPath   string
	tokenLit    string
	datacenter  string
	nodeFilter  string
	forwardPort int
	ttl         time.Duration
	http        *http.Client
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
	r := &NomadResolver{
		self:        opts.Self,
		socketPath:  opts.SocketPath,
		tokenPath:   opts.TokenPath,
		tokenLit:    opts.Token,
		datacenter:  opts.Datacenter,
		nodeFilter:  opts.NodeFilter,
		forwardPort: opts.ForwardPort,
		ttl:         ttl,
		log:         log,
	}
	if r.socketPath == "" {
		return nil, fmt.Errorf("cluster: nomad discovery needs the task API socket " +
			"(NOMAD_SECRETS_DIR/api.sock); is this running under Nomad with an identity block?")
	}
	if _, err := r.token(); err != nil {
		return nil, fmt.Errorf("cluster: nomad discovery has no usable token "+
			"(set an `identity { env = true, file = true }` block on the plugin task): %w", err)
	}
	socket := r.socketPath
	r.http = &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socket)
			},
		},
	}
	log.Debug("nomad: resolver configured",
		zap.String("socket", r.socketPath), zap.String("datacenter", r.datacenter),
		zap.Int("forward_port", r.forwardPort), zap.Bool("node_filter", r.nodeFilter != ""),
		zap.Duration("cache_ttl", r.ttl))
	return r, nil
}

// LocalNode returns this node's name.
func (r *NomadResolver) LocalNode() string { return r.self }

// token resolves the bearer token: the TokenPath file first (re-read each call
// so workload-identity rotation is picked up), then the literal override, then
// the NOMAD_TOKEN env. Returns an error if none yields a non-empty token.
func (r *NomadResolver) token() (string, error) {
	if r.tokenPath != "" {
		if b, err := os.ReadFile(r.tokenPath); err == nil {
			if tok := strings.TrimSpace(string(b)); tok != "" {
				return tok, nil
			}
		}
	}
	if r.tokenLit != "" {
		return r.tokenLit, nil
	}
	if tok := strings.TrimSpace(os.Getenv("NOMAD_TOKEN")); tok != "" {
		return tok, nil
	}
	return "", fmt.Errorf("no token in file %q, literal, or $NOMAD_TOKEN", r.tokenPath)
}

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
	tok, err := r.token()
	if err != nil {
		return nil, err
	}
	endpoint := "http://localhost/v1/nodes"
	if r.nodeFilter != "" {
		endpoint += "?filter=" + url.QueryEscape(r.nodeFilter)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	r.log.Debug("nomad: listing nodes", zap.String("datacenter", r.datacenter))
	resp, err := r.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cluster: querying nomad api.sock: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("cluster: nomad /v1/nodes returned %s", resp.Status)
	}
	var stubs []nodeStub
	if err := json.NewDecoder(resp.Body).Decode(&stubs); err != nil {
		return nil, fmt.Errorf("cluster: decoding /v1/nodes: %w", err)
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
