package local

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

func dormantReg(t *testing.T, node string) *stats.Registry {
	t.Helper()
	r := stats.NewRegistry(stats.Config{Enabled: true, WalkEnabled: false, Interval: time.Hour}, node, zap.NewNop())
	t.Cleanup(r.Close)
	return r
}

// fakeMapper resolves a Nomad id to an external id (and back) from a fixed table.
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

// TestController_StatsRouting verifies id→externalID resolution + the local-vs-
// forward routing, returning Nomad-id-keyed DTOs.
func TestController_StatsRouting(t *testing.T) {
	const secret = "sek"
	extA := "local/v1/nodeA/tank/nomad-csi/vola"
	extB := "local/v1/nodeB/tank/nomad-csi/volb"

	// Owner node "nodeB" runs a forward server backed by its registry.
	regB := dormantReg(t, "nodeB")
	ctrlB := &controller{statsReg: regB}
	ts := httptest.NewServer(cluster.NewServer(secret, ctrlB.dispatchForward))
	t.Cleanup(ts.Close)

	res := &cluster.StaticResolver{Self: "nodeA", Peers: []cluster.NodeInfo{
		{Node: "nodeA", Addr: "127.0.0.1:1"},
		{Node: "nodeB", Addr: ts.Listener.Addr().String()},
	}}
	regA := dormantReg(t, "nodeA")
	mapper := fakeMapper{fwd: map[string]string{
		"vola":  extA,
		"volb":  extB,
		"ghost": "local/v1/nodeB/tank/nomad-csi/ghost", // resolves, but not staged
	}}
	ctrlA := &controller{res: res, fwd: cluster.NewClient(secret), statsReg: regA, mapper: mapper, statsNS: "default", log: zap.NewNop()}

	regA.Track(extA, "/p", stats.AccessMount)
	regB.Track(extB, "/p", stats.AccessMount)

	// Co-located: served from regA.
	cs, found, err := ctrlA.Stats(context.Background(), "vola", "default")
	if err != nil || !found || cs.ID != "vola" || cs.Node != "nodeA" {
		t.Fatalf("local: cs=%+v found=%v err=%v; want vola/nodeA/found", cs, found, err)
	}
	// Remote: forwarded to nodeB.
	cs, found, err = ctrlA.Stats(context.Background(), "volb", "default")
	if err != nil || !found || cs.ID != "volb" || cs.Node != "nodeB" {
		t.Fatalf("forward: cs=%+v found=%v err=%v; want volb/nodeB/found", cs, found, err)
	}
	// Resolves in Nomad but not staged on the owner → ErrNotMounted (→ 412).
	if _, found, err := ctrlA.Stats(context.Background(), "ghost", "default"); found || !errors.Is(err, stats.ErrNotMounted) {
		t.Fatalf("ghost: found=%v err=%v; want not-mounted error", found, err)
	}
	// Unknown to Nomad (mapper miss) → found=false, no error (→ 404).
	if _, found, err := ctrlA.Stats(context.Background(), "nope", "default"); err != nil || found {
		t.Fatalf("unknown id: found=%v err=%v; want not-found/no-err", found, err)
	}
}

func TestController_AllReturnsOwnNodeRelabeled(t *testing.T) {
	ext1 := "local/v1/nodeA/tank/nomad-csi/v1"
	ext2 := "local/v1/nodeA/tank/nomad-csi/v2"
	reg := dormantReg(t, "nodeA")
	mapper := fakeMapper{fwd: map[string]string{"v1": ext1, "v2": ext2}}
	c := &controller{statsReg: reg, mapper: mapper, statsNS: "default", log: zap.NewNop()}
	reg.Track(ext1, "/p", stats.AccessMount)
	reg.Track(ext2, "/p", stats.AccessMount)

	all, err := c.All(context.Background(), "default")
	if err != nil || len(all) != 2 {
		t.Fatalf("All = %d items, err %v; want 2", len(all), err)
	}
	for _, s := range all {
		if s.ID != "v1" && s.ID != "v2" {
			t.Fatalf("All returned non-Nomad id: %q", s.ID)
		}
	}
}
