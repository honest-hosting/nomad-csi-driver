package local

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/cluster"
	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/zfs"
)

func multiPoolConfig() *config.LocalConfig {
	return &config.LocalConfig{
		Pools: []config.PoolConfig{
			{Name: "tank", ParentDataset: "csi"},
			{Name: "nvme", ParentDataset: "csi"},
		},
		DefaultPool:         "tank",
		DefaultVolblocksize: "16K",
		ForwardSecret:       "s3cret",
	}
}

func TestResolveParams_PoolAllowlist(t *testing.T) {
	cfg := multiPoolConfig()
	caps := []driver.VolumeCapability{{AccessType: driver.AccessTypeMount, FsType: "ext4"}}

	t.Run("default when omitted", func(t *testing.T) {
		vp, err := resolveParams(cfg, nil, caps)
		require.NoError(t, err)
		assert.Equal(t, "tank", vp.pool)
	})
	t.Run("explicit allowed", func(t *testing.T) {
		vp, err := resolveParams(cfg, map[string]string{"pool": "nvme"}, caps)
		require.NoError(t, err)
		assert.Equal(t, "nvme", vp.pool)
	})
	t.Run("unknown pool rejected", func(t *testing.T) {
		_, err := resolveParams(cfg, map[string]string{"pool": "rpool"}, caps)
		requireCode(t, err, driver.CodeInvalidArgument)
	})
	t.Run("path-like pool rejected", func(t *testing.T) {
		_, err := resolveParams(cfg, map[string]string{"pool": "/mnt/zfs/tank"}, caps)
		requireCode(t, err, driver.CodeInvalidArgument)
	})
}

func TestExternalID_DatasetRoundTrip(t *testing.T) {
	eid := externalID{Node: "n3", Dataset: "nvme/csi/vol-1"}
	got, err := parseExternalID(eid.String())
	require.NoError(t, err)
	assert.Equal(t, eid, got)
	assert.Equal(t, "nvme", got.Pool())
	assert.Equal(t, "vol-1", got.VolName())

	for _, bad := range []string{
		"local/v1/n3/",            // empty dataset
		"local/v1/n3/tank",        // pool only (no vol segment)
		"local/v1/n3//csi/v",      // double slash
		"local/v1/n3/tank/../etc", // traversal
		"local/v2/n3/tank/v",      // wrong version
	} {
		_, err := parseExternalID(bad)
		requireCode(t, err, driver.CodeInvalidArgument)
	}
}

func TestSnapshotID_DatasetRoundTrip(t *testing.T) {
	sid := snapshotID{Node: "n3", Dataset: "tank/csi/v1", SnapName: "s1"}
	got, err := parseSnapshotID(sid.String())
	require.NoError(t, err)
	assert.Equal(t, sid, got)

	for _, bad := range []string{
		"locals/v1/n3/tank/v1",   // missing snap segment (dataset has no pool/vol split)
		"locals/v1/n3/tankv1/s1", // dataset lacks a slash (no pool/vol)
		"locals/v1/n3/tank/v1/",  // empty snap
	} {
		_, err := parseSnapshotID(bad)
		requireCode(t, err, driver.CodeInvalidArgument)
	}
}

func TestValidatePools(t *testing.T) {
	ok := multiPoolConfig()
	require.NoError(t, validatePools(ok))

	bad := []*config.LocalConfig{
		{DefaultPool: "tank"},                                                    // no pools
		{Pools: []config.PoolConfig{{Name: "tank"}}},                             // no default
		{Pools: []config.PoolConfig{{Name: "tank"}}, DefaultPool: "nvme"},        // default not a member
		{Pools: []config.PoolConfig{{Name: "a/b"}}, DefaultPool: "a/b"},          // path-like name
		{Pools: []config.PoolConfig{{Name: "t"}, {Name: "t"}}, DefaultPool: "t"}, // duplicate
	}
	for _, c := range bad {
		requireCode(t, validatePools(c), driver.CodeInvalidArgument)
	}
}

func TestCheckCapacity_RejectsBelowReserve(t *testing.T) {
	cfg := multiPoolConfig()
	cfg.ReservePercent = 100 // reserve the whole pool -> nothing available
	mz := newMemZfs()
	res := &cluster.StaticResolver{Self: "A", Peers: []cluster.NodeInfo{{Node: "A", Addr: "127.0.0.1:1"}}}
	c := newController(zfs.New(mz), cfg, "local", res, cluster.NewClient("s3cret"), zap.NewNop())

	// Pin host explicitly so placement's auto availability filter is bypassed and
	// the create reaches localCreate's checkCapacity guard (which returns OutOfRange).
	req := localCreateReq("toobig", 16384)
	req.Parameters = map[string]string{"host": "A"}
	_, err := c.CreateVolume(context.Background(), req)
	requireCode(t, err, driver.CodeOutOfRange)
}

// --- pool-aware placement (two nodes, two pools) ---

func twoNodePoolSetup(t *testing.T, cfg *config.LocalConfig) (ca *controller, mzA, mzB *memZfs, cleanup func()) {
	t.Helper()
	secret := "s3cret"

	mzB = newMemZfs()
	resB := &cluster.StaticResolver{Self: "B", Peers: nil}
	cB := newController(zfs.New(mzB), cfg, "local", resB, cluster.NewClient(secret), zap.NewNop())
	srvB := httptest.NewServer(cluster.NewServer(secret, cB.dispatchForward))

	addrB := strings.TrimPrefix(srvB.URL, "http://")
	resA := &cluster.StaticResolver{Self: "A", Peers: []cluster.NodeInfo{
		{Node: "A", Addr: "127.0.0.1:1"},
		{Node: "B", Addr: addrB},
	}}
	mzA = newMemZfs()
	ca = newController(zfs.New(mzA), cfg, "local", resA, cluster.NewClient(secret), zap.NewNop())
	return ca, mzA, mzB, srvB.Close
}

func TestPlacement_AutoSkipsNodeMissingPool(t *testing.T) {
	ca, mzA, mzB, cleanup := twoNodePoolSetup(t, multiPoolConfig())
	defer cleanup()

	// Node A does not have the nvme pool; B does. auto on nvme must land on B.
	mzA.absentPools["nvme"] = true

	req := localCreateReq("fast", 16384)
	req.Parameters = map[string]string{"pool": "nvme"}
	vol, err := ca.CreateVolume(context.Background(), req)
	require.NoError(t, err)

	eid, _ := parseExternalID(vol.VolumeID)
	assert.Equal(t, "B", eid.Node, "auto avoided node A which lacks nvme")
	assert.Equal(t, "nvme", eid.Pool())
	assert.True(t, mzB.hasVol("nvme/csi/fast"))
}

func TestPlacement_ExplicitHostMissingPoolRejected(t *testing.T) {
	ca, mzA, _, cleanup := twoNodePoolSetup(t, multiPoolConfig())
	defer cleanup()

	mzA.absentPools["nvme"] = true
	req := localCreateReq("fast", 16384)
	req.Parameters = map[string]string{"pool": "nvme", "host": "A"}
	_, err := ca.CreateVolume(context.Background(), req)
	requireCode(t, err, driver.CodeInvalidArgument)
}

func requireCode(t *testing.T, err error, code driver.Code) {
	t.Helper()
	require.Error(t, err)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, code, de.Code)
}
