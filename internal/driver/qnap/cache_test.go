package qnap

import (
	"testing"
	"time"

	goqnap "github.com/honest-hosting/go-qnap"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCacheRefreshHonorsTTL(t *testing.T) {
	c := newLUNCache(60 * time.Second)
	now := time.Unix(1000, 0)
	c.now = func() time.Time { return now }

	calls := 0
	fetch := func() ([]goqnap.LUN, []goqnap.Target, error) {
		calls++
		return []goqnap.LUN{{Index: 0, Name: "v1", CapacityBytes: giB}},
			[]goqnap.Target{{Index: 0, Alias: "v1", IQN: "iqn:v1", LUNs: []int{0}}}, nil
	}

	require.NoError(t, c.ensureFresh(fetch))
	require.NoError(t, c.ensureFresh(fetch)) // within TTL -> no refetch
	assert.Equal(t, 1, calls)

	now = now.Add(61 * time.Second) // past TTL
	require.NoError(t, c.ensureFresh(fetch))
	assert.Equal(t, 2, calls)

	lun, ok := c.lookupByName("v1")
	require.True(t, ok)
	assert.Equal(t, 0, lun.Index)
	tgt, ok := c.targetForLUN(0)
	require.True(t, ok)
	assert.Equal(t, "v1", tgt.Alias)
}

func TestCacheIncrementalAddRemove(t *testing.T) {
	c := newLUNCache(time.Minute)
	c.addLUN(goqnap.LUN{Index: 5, Name: "vol"}, goqnap.Target{Index: 2, Alias: "vol", LUNs: []int{5}})

	_, ok := c.lookupByName("vol")
	assert.True(t, ok)
	_, ok = c.targetForLUN(5)
	assert.True(t, ok)
	assert.Len(t, c.all(), 1)

	c.removeLUN(5)
	_, ok = c.lookupByName("vol")
	assert.False(t, ok)
	_, ok = c.targetForLUN(5)
	assert.False(t, ok)
	assert.Empty(t, c.all())
}

func TestCacheLookupMiss(t *testing.T) {
	c := newLUNCache(time.Minute)
	_, ok := c.lookupByName("nope")
	assert.False(t, ok)
}
