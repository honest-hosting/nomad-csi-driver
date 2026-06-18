package cluster

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

// fakeNomad serves /v1/volumes over a unix socket, like the task API.
func fakeNomad(t *testing.T, vols []volStub) (sock string, calls *int32) {
	t.Helper()
	sock = filepath.Join(t.TempDir(), "api.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	var n int32
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/volumes", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&n, 1)
		_ = json.NewEncoder(w).Encode(vols)
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return sock, &n
}

func TestNomadVolumes_ResolveAndCache(t *testing.T) {
	sock, calls := fakeNomad(t, []volStub{
		{ID: "local-data", Namespace: "default", ExternalID: "local/v1/n/p/local-data"},
		{ID: "db", Namespace: "default", ExternalID: "local/v1/n/p/db"},
	})
	nv, err := NewNomadVolumes(NomadOptions{SocketPath: sock, Token: "t", CacheTTL: time.Minute})
	if err != nil {
		t.Fatalf("NewNomadVolumes: %v", err)
	}
	ctx := context.Background()

	ext, found, err := nv.ExternalID(ctx, "default", "local-data")
	if err != nil || !found || ext != "local/v1/n/p/local-data" {
		t.Fatalf("resolve = %q found=%v err=%v", ext, found, err)
	}
	// A second hit is served from cache (no extra fetch).
	if _, _, err := nv.ExternalID(ctx, "default", "db"); err != nil {
		t.Fatal(err)
	}
	if c := atomic.LoadInt32(calls); c != 1 {
		t.Fatalf("expected 1 fetch (cached), got %d", c)
	}

	// A miss forces exactly one refresh, then reports not-found.
	if _, found, _ := nv.ExternalID(ctx, "default", "nope"); found {
		t.Fatal("unknown id should not be found")
	}
	if c := atomic.LoadInt32(calls); c != 2 {
		t.Fatalf("miss should force one refresh; fetches=%d", c)
	}

	// Reverse map is the inverse.
	rev, err := nv.Reverse(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if rev["local/v1/n/p/local-data"] != "local-data" || rev["local/v1/n/p/db"] != "db" {
		t.Fatalf("reverse map wrong: %v", rev)
	}
}

func TestNomadVolumes_RequiresIdentity(t *testing.T) {
	t.Setenv("NOMAD_TOKEN", "")
	if _, err := NewNomadVolumes(NomadOptions{SocketPath: ""}); err == nil {
		t.Fatal("missing socket/token must fail fast")
	}
}
