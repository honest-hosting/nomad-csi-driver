package qnap

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/cluster"
	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

func nodeServer(t *testing.T, secret, nodeName, externalID string) (string, *stats.Registry) {
	t.Helper()
	reg := stats.NewRegistry(stats.Config{Enabled: true, WalkEnabled: false, Interval: time.Hour}, nodeName, zap.NewNop())
	t.Cleanup(reg.Close)
	reg.Track(externalID, "/p", stats.AccessMount)
	nd := &node{stats: reg}
	ts := httptest.NewServer(cluster.NewServer(secret, nd.dispatchForward))
	t.Cleanup(ts.Close)
	return ts.Listener.Addr().String(), reg
}

// fakeMapper resolves Nomad id ↔ external id from a fixed table.
type fakeMapper struct{ fwd map[string]string }

func (m fakeMapper) ExternalID(_ context.Context, _, nomadID string) (string, bool, error) {
	ext, ok := m.fwd[nomadID]
	return ext, ok, nil
}

func (m fakeMapper) Reverse(_ context.Context, _ string) (map[string]string, error) {
	rev := make(map[string]string, len(m.fwd))
	for n, e := range m.fwd {
		rev[e] = n
	}
	return rev, nil
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

// TestQNAPSource_FanOutAggregates verifies the controller aggregates node
// readings, resolves Nomad ids to external ids on lookup, and degrades when a
// node is unreachable.
func TestQNAPSource_FanOutAggregates(t *testing.T) {
	const secret = "s"
	extA := "qnap/v1/1/2/t/vol-a"
	extB := "qnap/v1/1/2/t/vol-b"
	addr1, _ := nodeServer(t, secret, "node1", extA)
	addr2, _ := nodeServer(t, secret, "node2", extB)

	res := &cluster.StaticResolver{Self: "ctrl", Peers: []cluster.NodeInfo{
		{Node: "node1", Addr: addr1},
		{Node: "node2", Addr: addr2},
		{Node: "deadnode", Addr: "127.0.0.1:1"}, // unreachable
	}}
	// "vol-c" is registered in Nomad (mapper resolves it) but no node serves it.
	mapper := fakeMapper{fwd: map[string]string{"vol-a": extA, "vol-b": extB, "vol-c": "qnap/v1/9/9/t/vol-c"}}
	src := newQNAPSource(res, cluster.NewClient(secret), mapper, "default", 30*time.Millisecond, nil, zap.NewNop())
	t.Cleanup(src.Close)

	waitFor(t, 2*time.Second, func() bool {
		all, _ := src.All(context.Background(), "default")
		return len(all) == 2
	})

	if cs, found, _ := src.Stats(context.Background(), "vol-a", "default"); !found || cs.ID != "vol-a" {
		t.Fatalf("vol-a: cs=%+v found=%v", cs, found)
	}
	if cs, found, _ := src.Stats(context.Background(), "vol-b", "default"); !found || cs.Node != "node2" {
		t.Fatalf("vol-b aggregation wrong: cs=%+v found=%v", cs, found)
	}
	// Unknown to Nomad (mapper miss) → found=false, no error (→ 404).
	if _, found, err := src.Stats(context.Background(), "nope", "default"); found || err != nil {
		t.Fatalf("unknown volume: found=%v err=%v; want 404 semantics", found, err)
	}
	// Known to Nomad but not in the aggregate → ErrNotMounted (→ 412).
	if _, found, err := src.Stats(context.Background(), "vol-c", "default"); found || !errors.Is(err, stats.ErrNotMounted) {
		t.Fatalf("vol-c: found=%v err=%v; want not-mounted error", found, err)
	}
}

func TestQNAPSource_DispatchNotFound(t *testing.T) {
	reg := stats.NewRegistry(stats.Config{Enabled: true, WalkEnabled: false, Interval: time.Hour}, "node1", zap.NewNop())
	t.Cleanup(reg.Close)
	nd := &node{stats: reg}

	_, err := nd.dispatchForward(context.Background(), stats.MethodVolStats, []byte(`{"id":"missing"}`))
	if err == nil {
		t.Fatal("expected NotFound error for untracked volume")
	}
	body, err := nd.dispatchForward(context.Background(), stats.MethodVolStatsDump, nil)
	if err != nil || string(body) != "[]" {
		t.Fatalf("empty dump = %q err %v; want []", body, err)
	}
}
