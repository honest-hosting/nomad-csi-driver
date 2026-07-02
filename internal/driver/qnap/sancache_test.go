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
