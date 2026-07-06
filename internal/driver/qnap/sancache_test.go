package qnap

import (
	"context"
	"testing"
	"time"

	goqnap "github.com/honest-hosting/go-qnap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// countingClient wraps a Client and counts the list calls, to prove hydration is
// single-flighted (coalesced) rather than per-resolve.
type countingClient struct {
	Client
	listTargets int
	listLUNs    int
}

func (c *countingClient) ListTargets(ctx context.Context, s goqnap.Session) ([]goqnap.Target, error) {
	c.listTargets++
	return c.Client.ListTargets(ctx, s)
}

func (c *countingClient) ListLUNs(ctx context.Context, s goqnap.Session) ([]goqnap.LUN, error) {
	c.listLUNs++
	return c.Client.ListLUNs(ctx, s)
}

func newTestSANCache(t *testing.T) (*sanIdentityCache, *countingClient) {
	t.Helper()
	fc := newFakeClient()
	fc.targets[10] = goqnap.Target{Index: 10, IQN: "iqn.qnap:t10"}
	fc.luns[42] = goqnap.LUN{Index: 42, Name: "vol-a"}
	cc := &countingClient{Client: fc}
	c := newSANIdentityCache(cc, newSessionManager(cc, "u", "p"), 30*time.Second, zap.NewNop())
	c.nowFn = func() time.Time { return time.Unix(1000, 0) } // freeze so the TTL never expires mid-test
	return c, cc
}

func TestSANCache_ResolvesAndVerifiesLUNName(t *testing.T) {
	c, _ := newTestSANCache(t)
	iqn, ok := c.resolveIQN(context.Background(), externalID{LUNIndex: 42, TargetIndex: 10, LUNName: "vol-a"})
	require.True(t, ok)
	assert.Equal(t, "iqn.qnap:t10", iqn)
}

func TestSANCache_ResolveExternalID(t *testing.T) {
	fc := newFakeClient()
	// 1:1 target: one LUN mapped at SCSI LUN 0, alias == LUN name → OwnTarget "t".
	fc.targets[0] = goqnap.Target{Index: 0, IQN: "iqn.qnap:vol-a", Alias: "vol-a", LUNs: []int{42}}
	// A shared 1:N target: two LUNs, alias != either name → OwnTarget "s".
	fc.targets[7] = goqnap.Target{Index: 7, IQN: "iqn.qnap:shared", Alias: "shared", LUNs: []int{5, 9}}
	fc.luns[42] = goqnap.LUN{Index: 42, Name: "vol-a"}
	fc.luns[5] = goqnap.LUN{Index: 5, Name: "vol-b"}
	fc.luns[9] = goqnap.LUN{Index: 9, Name: "vol-c"}
	c := newSANIdentityCache(fc, newSessionManager(fc, "u", "p"), 30*time.Second, zap.NewNop())
	c.nowFn = func() time.Time { return time.Unix(1000, 0) }

	// 1:1 owned target → the verified production shape (qnap/v1/<lun>/<tgt>/t/<name>).
	got, ok := c.resolveExternalID(context.Background(), "iqn.qnap:vol-a", 0)
	require.True(t, ok)
	assert.Equal(t, "qnap/v1/42/0/t/vol-a", got)

	// Shared target: the SCSI LUN number selects the mapped LUN ordinally; alias !=
	// name → shared ("s").
	got, ok = c.resolveExternalID(context.Background(), "iqn.qnap:shared", 1)
	require.True(t, ok)
	assert.Equal(t, "qnap/v1/9/7/s/vol-c", got)

	// Unknown IQN and out-of-range LUN number both fail cleanly (caller stays add-only).
	_, ok = c.resolveExternalID(context.Background(), "iqn.qnap:nope", 0)
	assert.False(t, ok)
	_, ok = c.resolveExternalID(context.Background(), "iqn.qnap:shared", 5)
	assert.False(t, ok, "LUN number past the target's mapped LUNs must not resolve")
}

func TestSANCache_RejectsNameMismatch(t *testing.T) {
	// Index reuse: the LUN at index 42 is now a different volume → refuse (guard).
	c, _ := newTestSANCache(t)
	_, ok := c.resolveIQN(context.Background(), externalID{LUNIndex: 42, TargetIndex: 10, LUNName: "some-other-vol"})
	assert.False(t, ok, "LUN name mismatch must refuse teardown identity")
}

func TestSANCache_MissingIndex(t *testing.T) {
	c, _ := newTestSANCache(t)
	_, ok := c.resolveIQN(context.Background(), externalID{LUNIndex: 999, TargetIndex: 10, LUNName: "vol-a"})
	assert.False(t, ok)
}

func TestSANCache_SingleFlightHydration(t *testing.T) {
	// A storm of cold resolves within the TTL costs ONE ListTargets+ListLUNs pair
	// (OQ4/D8), not one pair per resolve.
	c, cc := newTestSANCache(t)
	for i := 0; i < 5; i++ {
		_, ok := c.resolveIQN(context.Background(), externalID{LUNIndex: 42, TargetIndex: 10, LUNName: "vol-a"})
		require.True(t, ok)
	}
	assert.Equal(t, 1, cc.listTargets, "hydrated once for the whole storm")
	assert.Equal(t, 1, cc.listLUNs)
}

func TestSANCache_RehydratesAfterTTL(t *testing.T) {
	c, cc := newTestSANCache(t)
	now := time.Unix(1000, 0)
	c.nowFn = func() time.Time { return now }

	_, _ = c.resolveIQN(context.Background(), externalID{LUNIndex: 42, TargetIndex: 10, LUNName: "vol-a"})
	require.Equal(t, 1, cc.listTargets)

	now = now.Add(2 * sanIdentityCacheTTL) // past the TTL
	_, _ = c.resolveIQN(context.Background(), externalID{LUNIndex: 42, TargetIndex: 10, LUNName: "vol-a"})
	assert.Equal(t, 2, cc.listTargets, "re-hydrated after the TTL elapsed")
}
