package cluster

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// nodesHandler serves a /v1/nodes response and counts requests, with a
// swappable handler so a test can flip behavior mid-run.
type nodesHandler struct {
	calls   atomic.Int32
	lastReq atomic.Pointer[http.Request]
	fn      atomic.Pointer[func(w http.ResponseWriter, r *http.Request)]
}

func (h *nodesHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.calls.Add(1)
	h.lastReq.Store(r)
	(*h.fn.Load())(w, r)
}

func (h *nodesHandler) set(fn func(w http.ResponseWriter, r *http.Request)) { h.fn.Store(&fn) }

// startUnixNomad spins an httptest server on a unix socket (mimicking api.sock)
// and returns the socket path plus the request handler.
func startUnixNomad(t *testing.T) (string, *nodesHandler) {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "api.sock")
	ln, err := net.Listen("unix", sock)
	require.NoError(t, err)
	h := &nodesHandler{}
	h.set(func(http.ResponseWriter, *http.Request) {})
	srv := httptest.NewUnstartedServer(h)
	srv.Listener = ln
	srv.Start()
	t.Cleanup(srv.Close)
	return sock, h
}

func writeNodes(w http.ResponseWriter, stubs []nodeStub) {
	_ = json.NewEncoder(w).Encode(stubs)
}

func newTestResolver(t *testing.T, sock string, opts ...func(*NomadOptions)) *NomadResolver {
	t.Helper()
	o := NomadOptions{
		Self: "n1", SocketPath: sock, Token: "test-token",
		Datacenter: "kitchen", ForwardPort: 9602, CacheTTL: time.Hour,
	}
	for _, f := range opts {
		f(&o)
	}
	r, err := NewNomadResolver(o)
	require.NoError(t, err)
	return r
}

func TestNomadResolver_ListFiltersAndJoinsPort(t *testing.T) {
	sock, h := startUnixNomad(t)
	h.set(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/v1/nodes", r.URL.Path)
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		writeNodes(w, []nodeStub{
			{Name: "n1", Address: "192.168.56.51", Status: "ready", Datacenter: "kitchen"},
			{Name: "n2", Address: "192.168.56.52", Status: "ready", Datacenter: "kitchen"},
			{Name: "down", Address: "192.168.56.53", Status: "down", Datacenter: "kitchen"},
			{Name: "other-dc", Address: "10.0.0.9", Status: "ready", Datacenter: "elsewhere"},
			{Name: "no-addr", Address: "", Status: "ready", Datacenter: "kitchen"},
		})
	})

	r := newTestResolver(t, sock)
	peers, err := r.List(context.Background())
	require.NoError(t, err)
	require.Len(t, peers, 2, "down, other-dc, and no-addr nodes filtered out")
	assert.Equal(t, NodeInfo{Node: "n1", Addr: "192.168.56.51:9602"}, peers[0])
	assert.Equal(t, NodeInfo{Node: "n2", Addr: "192.168.56.52:9602"}, peers[1])
}

func TestNomadResolver_Resolve(t *testing.T) {
	sock, h := startUnixNomad(t)
	h.set(func(w http.ResponseWriter, _ *http.Request) {
		writeNodes(w, []nodeStub{{Name: "n2", Address: "10.0.0.2", Status: "ready", Datacenter: "kitchen"}})
	})
	r := newTestResolver(t, sock)

	addr, err := r.Resolve(context.Background(), "n2")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.2:9602", addr)

	_, err = r.Resolve(context.Background(), "ghost")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not in datacenter")
}

func TestNomadResolver_CacheTTL(t *testing.T) {
	sock, h := startUnixNomad(t)
	h.set(func(w http.ResponseWriter, _ *http.Request) {
		writeNodes(w, []nodeStub{{Name: "n1", Address: "10.0.0.1", Status: "ready", Datacenter: "kitchen"}})
	})
	r := newTestResolver(t, sock) // TTL = 1h

	for i := 0; i < 3; i++ {
		_, err := r.List(context.Background())
		require.NoError(t, err)
	}
	assert.Equal(t, int32(1), h.calls.Load(), "fresh cache served without re-fetching")
}

func TestNomadResolver_RefreshOnMiss(t *testing.T) {
	sock, h := startUnixNomad(t)
	// First fetch: only n1. Second fetch (forced by the miss): n1 + n2 joined.
	h.set(func(w http.ResponseWriter, _ *http.Request) {
		writeNodes(w, []nodeStub{{Name: "n1", Address: "10.0.0.1", Status: "ready", Datacenter: "kitchen"}})
	})
	r := newTestResolver(t, sock)

	_, err := r.List(context.Background()) // primes cache (n1 only)
	require.NoError(t, err)
	h.set(func(w http.ResponseWriter, _ *http.Request) {
		writeNodes(w, []nodeStub{
			{Name: "n1", Address: "10.0.0.1", Status: "ready", Datacenter: "kitchen"},
			{Name: "n2", Address: "10.0.0.2", Status: "ready", Datacenter: "kitchen"},
		})
	})

	addr, err := r.Resolve(context.Background(), "n2") // cache miss -> force refresh
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.2:9602", addr)
	assert.Equal(t, int32(2), h.calls.Load(), "miss triggered exactly one extra fetch")
}

func TestNomadResolver_StaleServeOnError(t *testing.T) {
	sock, h := startUnixNomad(t)
	h.set(func(w http.ResponseWriter, _ *http.Request) {
		writeNodes(w, []nodeStub{{Name: "n1", Address: "10.0.0.1", Status: "ready", Datacenter: "kitchen"}})
	})
	r := newTestResolver(t, sock, func(o *NomadOptions) { o.CacheTTL = time.Nanosecond }) // always stale

	_, err := r.List(context.Background()) // good fetch primes cache
	require.NoError(t, err)
	h.set(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) })

	peers, err := r.List(context.Background()) // refresh fails -> stale cache
	require.NoError(t, err)
	require.Len(t, peers, 1)
	assert.Equal(t, "n1", peers[0].Node)
}

func TestNomadResolver_NoCacheHardFails(t *testing.T) {
	sock, h := startUnixNomad(t)
	h.set(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) })
	r := newTestResolver(t, sock)

	_, err := r.List(context.Background())
	require.Error(t, err, "no prior cache to serve -> error surfaces")
	assert.Contains(t, err.Error(), "403")
}

func TestNomadResolver_NodeFilterSentServerSide(t *testing.T) {
	sock, h := startUnixNomad(t)
	h.set(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, `NodeClass == "storage"`, r.URL.Query().Get("filter"))
		writeNodes(w, []nodeStub{})
	})
	r := newTestResolver(t, sock, func(o *NomadOptions) { o.NodeFilter = `NodeClass == "storage"` })
	_, err := r.List(context.Background())
	require.NoError(t, err)
}

func TestNomadResolver_TokenFilePreferredAndRotation(t *testing.T) {
	sock, h := startUnixNomad(t)
	var gotTok atomic.Pointer[string]
	h.set(func(w http.ResponseWriter, r *http.Request) {
		tok := r.Header.Get("Authorization")
		gotTok.Store(&tok)
		writeNodes(w, []nodeStub{})
	})
	tokFile := filepath.Join(t.TempDir(), "nomad_token")
	require.NoError(t, os.WriteFile(tokFile, []byte("file-tok-v1\n"), 0o600))

	r, err := NewNomadResolver(NomadOptions{
		Self: "n1", SocketPath: sock, TokenPath: tokFile, Token: "literal-ignored",
		Datacenter: "kitchen", ForwardPort: 9602, CacheTTL: time.Nanosecond,
	})
	require.NoError(t, err)

	_, err = r.List(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "Bearer file-tok-v1", *gotTok.Load(), "file token preferred over literal")

	// Rotate the file; next refresh must pick it up (TTL ~0 forces a re-fetch).
	require.NoError(t, os.WriteFile(tokFile, []byte("file-tok-v2"), 0o600))
	_, err = r.List(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "Bearer file-tok-v2", *gotTok.Load(), "rotated token re-read from file")
}

func TestNewNomadResolver_Validation(t *testing.T) {
	t.Run("missing socket", func(t *testing.T) {
		_, err := NewNomadResolver(NomadOptions{Self: "n1", Token: "t"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "socket")
	})
	t.Run("missing token source", func(t *testing.T) {
		t.Setenv("NOMAD_TOKEN", "")
		_, err := NewNomadResolver(NomadOptions{Self: "n1", SocketPath: "/x/api.sock"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "identity")
	})
	t.Run("env token satisfies", func(t *testing.T) {
		t.Setenv("NOMAD_TOKEN", "env-tok")
		_, err := NewNomadResolver(NomadOptions{Self: "n1", SocketPath: "/x/api.sock"})
		require.NoError(t, err)
	})
}
