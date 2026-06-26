package local

import (
	"context"
	"errors"
	"net"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/cluster"
	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/zfs"
)

func testConfig() *config.LocalConfig {
	return &config.LocalConfig{
		Pools:               []config.PoolConfig{{Name: "tank", ParentDataset: "csi"}},
		DefaultPool:         "tank",
		DefaultVolblocksize: "16K",
		ForwardSecret:       "s3cret",
	}
}

// TestParentDatasetForPool locks in the precedence: an explicit per-pool
// parent_dataset overrides the deployment default; otherwise the default
// (from --parent-dataset) is the namespace under the pool.
func TestParentDatasetForPool(t *testing.T) {
	cfg := &config.LocalConfig{Pools: []config.PoolConfig{
		{Name: "tank", ParentDataset: "csi"}, // explicit override
		{Name: "nvme"},                       // no override -> default
	}}
	assert.Equal(t, "tank/csi", parentDatasetForPool(cfg, "tank", "nomad-csi"), "explicit parent_dataset wins")
	assert.Equal(t, "nvme/nomad-csi", parentDatasetForPool(cfg, "nvme", "nomad-csi"), "default is used when a pool omits parent_dataset")
	assert.Equal(t, "other/nomad-csi", parentDatasetForPool(cfg, "other", "nomad-csi"), "unknown pool falls back to the default")
}

// singleNodeController builds a controller for node "A" with a static resolver
// listing only itself, backed by an in-memory ZFS.
func singleNodeController(t *testing.T) (*controller, *memZfs) {
	t.Helper()
	mz := newMemZfs()
	res := &cluster.StaticResolver{Self: "A", Peers: []cluster.NodeInfo{{Node: "A", Addr: "127.0.0.1:1"}}}
	c := newController(zfs.New(mz), testConfig(), "local", res, cluster.NewClient("s3cret"), zap.NewNop())
	return c, mz
}

func localCreateReq(name string, bytes int64) *driver.CreateVolumeRequest {
	return &driver.CreateVolumeRequest{
		Name:          name,
		CapacityRange: driver.CapacityRange{RequiredBytes: bytes},
		VolumeCapabilities: []driver.VolumeCapability{
			{AccessType: driver.AccessTypeMount, AccessMode: driver.AccessModeSingleNodeWriter, FsType: "ext4"},
		},
	}
}

func TestLocalCreate_AutoPlacesAndRounds(t *testing.T) {
	c, mz := singleNodeController(t)
	vol, err := c.CreateVolume(context.Background(), localCreateReq("vol-a", 1000)) // < 16K
	require.NoError(t, err)

	assert.Equal(t, int64(16384), vol.CapacityBytes, "rounded up to volblocksize")
	eid, err := parseExternalID(vol.VolumeID)
	require.NoError(t, err)
	assert.Equal(t, "A", eid.Node)
	assert.Equal(t, "A", vol.AccessibleTopology[0].Segments[topologyKey])
	assert.Equal(t, "tank/csi/vol-a", vol.VolumeContext[ctxKeyDataset])
	assert.True(t, mz.hasVol("tank/csi/vol-a"))
}

func TestLocalCreate_Idempotent(t *testing.T) {
	c, _ := singleNodeController(t)
	v1, err := c.CreateVolume(context.Background(), localCreateReq("dup", 16384))
	require.NoError(t, err)
	v2, err := c.CreateVolume(context.Background(), localCreateReq("dup", 16384))
	require.NoError(t, err)
	assert.Equal(t, v1.VolumeID, v2.VolumeID)
}

func TestLocalCreate_AlreadyExistsDifferentSize(t *testing.T) {
	c, _ := singleNodeController(t)
	_, err := c.CreateVolume(context.Background(), localCreateReq("x", 16384))
	require.NoError(t, err)
	_, err = c.CreateVolume(context.Background(), localCreateReq("x", 2*16384))
	require.Error(t, err)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeAlreadyExists, de.Code)
}

func TestLocalDelete(t *testing.T) {
	c, mz := singleNodeController(t)
	vol, err := c.CreateVolume(context.Background(), localCreateReq("del", 16384))
	require.NoError(t, err)
	require.NoError(t, c.DeleteVolume(context.Background(), vol.VolumeID, nil))
	assert.False(t, mz.hasVol("tank/csi/del"))
	// Idempotent second delete.
	require.NoError(t, c.DeleteVolume(context.Background(), vol.VolumeID, nil))
}

func TestLocalExpand_GrowOnly(t *testing.T) {
	c, _ := singleNodeController(t)
	vol, err := c.CreateVolume(context.Background(), localCreateReq("grow", 16384))
	require.NoError(t, err)
	newBytes, nodeExpand, err := c.ExpandVolume(context.Background(), vol.VolumeID, driver.CapacityRange{RequiredBytes: 10 * 16384}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(10*16384), newBytes)
	assert.True(t, nodeExpand)

	// Shrink is rejected.
	_, _, err = c.ExpandVolume(context.Background(), vol.VolumeID, driver.CapacityRange{RequiredBytes: 16384}, nil)
	require.Error(t, err)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeOutOfRange, de.Code)
}

func TestLocalSnapshotLifecycle(t *testing.T) {
	c, _ := singleNodeController(t)
	vol, err := c.CreateVolume(context.Background(), localCreateReq("snapvol", 16384))
	require.NoError(t, err)
	snap, err := c.CreateSnapshot(context.Background(), &driver.CreateSnapshotRequest{SourceVolumeID: vol.VolumeID, Name: "s1"})
	require.NoError(t, err)
	assert.True(t, snap.ReadyToUse)
	assert.Equal(t, simCreationUnix, snap.CreationTime.Unix(), "creation time comes from the ZFS `creation` property")

	sid, err := parseSnapshotID(snap.SnapshotID)
	require.NoError(t, err)
	assert.Equal(t, "A", sid.Node)

	require.NoError(t, c.DeleteSnapshot(context.Background(), snap.SnapshotID, nil))
}

func TestLocalListSnapshots(t *testing.T) {
	c, _ := singleNodeController(t)
	vol, err := c.CreateVolume(context.Background(), localCreateReq("lsnap", 16384))
	require.NoError(t, err)
	snap, err := c.CreateSnapshot(context.Background(), &driver.CreateSnapshotRequest{SourceVolumeID: vol.VolumeID, Name: "s1"})
	require.NoError(t, err)

	all, err := c.ListSnapshots(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, all, 1)
	assert.Equal(t, snap.SnapshotID, all[0].SnapshotID)
	assert.Equal(t, vol.VolumeID, all[0].SourceVolumeID, "snapshot reports its source volume")
	assert.Equal(t, simCreationUnix, all[0].CreationTime.Unix(), "list reports the ZFS creation time")

	// Filtered to the source volume.
	filtered, err := c.ListSnapshots(context.Background(), vol.VolumeID)
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	assert.Equal(t, snap.SnapshotID, filtered[0].SnapshotID)
}

// DATA SAFETY: a ZFS snapshot is a dependent child, so deleting the volume must
// be refused with FailedPrecondition (not a raw zfs error) until it's gone.
func TestLocalDeleteVolume_RefusesWithSnapshot(t *testing.T) {
	c, _ := singleNodeController(t)
	vol, err := c.CreateVolume(context.Background(), localCreateReq("delsnap", 16384))
	require.NoError(t, err)
	_, err = c.CreateSnapshot(context.Background(), &driver.CreateSnapshotRequest{SourceVolumeID: vol.VolumeID, Name: "s1"})
	require.NoError(t, err)

	err = c.DeleteVolume(context.Background(), vol.VolumeID, nil)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeFailedPrecondition, de.Code)
}

func TestLocalCloneFromVolume(t *testing.T) {
	c, mz := singleNodeController(t)
	src, err := c.CreateVolume(context.Background(), localCreateReq("clonesrc", 16384))
	require.NoError(t, err)

	req := localCreateReq("clonedst", 16384)
	req.ContentSource = &driver.ContentSource{VolumeID: src.VolumeID}
	dst, err := c.CreateVolume(context.Background(), req)
	require.NoError(t, err)
	assert.True(t, mz.hasVol("tank/csi/clonedst"))
	eid, _ := parseExternalID(dst.VolumeID)
	assert.Equal(t, "A", eid.Node, "clone placed on source node")

	// The clone must be fully independent: no inherited snapshot from the
	// send|recv, so it deletes cleanly (no FailedPrecondition).
	snaps, err := c.ListSnapshots(context.Background(), dst.VolumeID)
	require.NoError(t, err)
	assert.Empty(t, snaps, "clone carries no inherited snapshot")
	require.NoError(t, c.DeleteVolume(context.Background(), dst.VolumeID, nil), "clone deletes cleanly")
}

// local_zfs_op_total records the op + outcome of each logical local op. A forced
// `zfs create` failure must land in {op=create,outcome=error}, a success in {ok}.
func TestLocalMetrics_ZFSOpOutcomes(t *testing.T) {
	mz := newMemZfs()
	res := &cluster.StaticResolver{Self: "A", Peers: []cluster.NodeInfo{{Node: "A", Addr: "127.0.0.1:1"}}}
	c := newController(zfs.New(mz), testConfig(), "local", res, cluster.NewClient("s3cret"), zap.NewNop())
	c.metrics = newLocalMetrics(prometheus.NewRegistry())

	_, err := c.CreateVolume(context.Background(), localCreateReq("ok1", 16384))
	require.NoError(t, err)
	assert.Equal(t, 1.0, testutil.ToFloat64(c.metrics.zfsOpTotal.WithLabelValues("create", "ok")),
		"successful create → {create,ok}")

	mz.failOps["create"] = true // force the zvol create to fail
	_, err = c.CreateVolume(context.Background(), localCreateReq("bad1", 16384))
	require.Error(t, err)
	assert.Equal(t, 1.0, testutil.ToFloat64(c.metrics.zfsOpTotal.WithLabelValues("create", "error")),
		"failed create → {create,error}")
}

func metricsController(t *testing.T, mz *memZfs) *controller {
	t.Helper()
	res := &cluster.StaticResolver{Self: "A", Peers: []cluster.NodeInfo{{Node: "A", Addr: "127.0.0.1:1"}}}
	c := newController(zfs.New(mz), testConfig(), "local", res, cluster.NewClient("s3cret"), zap.NewNop())
	c.metrics = newLocalMetrics(prometheus.NewRegistry())
	return c
}

func TestLocalMetrics_PlacementAndCapacity(t *testing.T) {
	t.Run("auto create -> placement{auto,ok} + resolve{ok}", func(t *testing.T) {
		c := metricsController(t, newMemZfs())
		_, err := c.CreateVolume(context.Background(), localCreateReq("p1", 16384))
		require.NoError(t, err)
		assert.Equal(t, 1.0, testutil.ToFloat64(c.metrics.placementTotal.WithLabelValues("auto", "ok")))
		assert.GreaterOrEqual(t, testutil.ToFloat64(c.metrics.resolveTotal.WithLabelValues("ok")), 1.0)
	})

	t.Run("reserve breach -> placement{host,ok} + capacity_reject", func(t *testing.T) {
		mz := newMemZfs()
		mz.size = 1000 // provisioned-available (size - zvols) far below the requested size
		c := metricsController(t, mz)
		req := localCreateReq("p2", 16384)
		req.Parameters = map[string]string{"host": "A", "fsType": "ext4"}
		_, err := c.CreateVolume(context.Background(), req)
		require.Error(t, err)
		assert.Equal(t, 1.0, testutil.ToFloat64(c.metrics.placementTotal.WithLabelValues("host", "ok")),
			"placement succeeded; the rejection is the post-placement capacity guard")
		assert.Equal(t, 1.0, testutil.ToFloat64(c.metrics.capacityReject))
	})
}

func TestLocalMetrics_Forward(t *testing.T) {
	ca, _, cleanup := twoNodeSetup(t)
	defer cleanup()
	ca.metrics = newLocalMetrics(prometheus.NewRegistry())

	req := localCreateReq("remote", 16384)
	req.Parameters = map[string]string{"host": "B"}
	_, err := ca.CreateVolume(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, 1.0, testutil.ToFloat64(ca.metrics.forwardTotal.WithLabelValues("create", "ok")),
		"forwarded create records forward{create,ok}")
}

func TestForwardOutcome(t *testing.T) {
	assert.Equal(t, "unreachable", forwardOutcome(&net.OpError{Op: "dial", Err: errors.New("connection refused")}),
		"transport errors are unreachable")
	assert.Equal(t, "error", forwardOutcome(errors.New("remote op failed")),
		"non-transport (coded remote) errors are error")
}

// collectGauges gathers a collector into a name{labels}->value map.
func collectGauges(t *testing.T, c prometheus.Collector) map[string]float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(c))
	mfs, err := reg.Gather()
	require.NoError(t, err)
	out := map[string]float64{}
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			key := mf.GetName()
			for _, l := range m.GetLabel() {
				key += "{" + l.GetName() + "=" + l.GetValue() + "}"
			}
			out[key] = m.GetGauge().GetValue()
		}
	}
	return out
}

func TestPoolCollector(t *testing.T) {
	mz := newMemZfs()
	mz.free = 5000 // physical (written) free
	mz.size = 10000
	mz.vols["tank/csi/v1"] = 4000 // one thick zvol: charges 4000 of provisioned space

	g := collectGauges(t, newPoolCollector(zfs.New(mz), testConfig(), "local", zap.NewNop()))
	assert.Equal(t, 1.0, g["nomad_csi_local_pool_online{pool=tank}"])
	// Physical axis: size = allocated + free.
	assert.Equal(t, 10000.0, g["nomad_csi_local_pool_size_bytes{pool=tank}"])
	assert.Equal(t, 5000.0, g["nomad_csi_local_pool_allocated_bytes{pool=tank}"], "allocated == size - free (10000-5000)")
	assert.Equal(t, 5000.0, g["nomad_csi_local_pool_free_bytes{pool=tank}"], "free_bytes is physical (written) free")
	// Provisioned axis: available == size - provisioned - reserve.
	assert.Equal(t, 6000.0, g["nomad_csi_local_pool_available_bytes{pool=tank}"], "reserve 0 -> available == size - provisioned (10000-4000)")
	assert.Equal(t, 0.0, g["nomad_csi_local_pool_reserve_bytes{pool=tank}"])
	assert.Equal(t, 1.0, g["nomad_csi_local_pool_volumes{pool=tank}"])

	// An absent pool reports online=0 and no capacity series.
	mz.absentPools["tank"] = true
	g = collectGauges(t, newPoolCollector(zfs.New(mz), testConfig(), "local", zap.NewNop()))
	assert.Equal(t, 0.0, g["nomad_csi_local_pool_online{pool=tank}"])
	_, hasFree := g["nomad_csi_local_pool_free_bytes{pool=tank}"]
	assert.False(t, hasFree, "absent pool emits no free-bytes series")
}

func TestGetCapacity(t *testing.T) {
	c, _ := singleNodeController(t)
	avail, err := c.GetCapacity(context.Background(), nil, nil)
	require.NoError(t, err)
	// Provisioned-aware available with no zvols and reserve 0 == pool size.
	assert.Equal(t, int64(1)<<43, avail)
}

// --- forwarding (two-node) ---

// twoNodeSetup wires controller A (local) with a resolver that also knows node
// B, whose forwarding server runs in-process backed by its own ZFS.
func twoNodeSetup(t *testing.T) (ca *controller, mzB *memZfs, cleanup func()) {
	t.Helper()
	secret := "s3cret"

	mzB = newMemZfs()
	resB := &cluster.StaticResolver{Self: "B", Peers: nil}
	cB := newController(zfs.New(mzB), testConfig(), "local", resB, cluster.NewClient(secret), zap.NewNop())
	srvB := httptest.NewServer(cluster.NewServer(secret, cB.dispatchForward))

	addrB := strings.TrimPrefix(srvB.URL, "http://")
	resA := &cluster.StaticResolver{Self: "A", Peers: []cluster.NodeInfo{
		{Node: "A", Addr: "127.0.0.1:1"},
		{Node: "B", Addr: addrB},
	}}
	mzA := newMemZfs()
	ca = newController(zfs.New(mzA), testConfig(), "local", resA, cluster.NewClient(secret), zap.NewNop())
	return ca, mzB, srvB.Close
}

func TestForward_CreateAndDeleteOnRemoteNode(t *testing.T) {
	ca, mzB, cleanup := twoNodeSetup(t)
	defer cleanup()

	req := localCreateReq("remote", 16384)
	req.Parameters = map[string]string{"host": "B"}
	vol, err := ca.CreateVolume(context.Background(), req)
	require.NoError(t, err)

	eid, _ := parseExternalID(vol.VolumeID)
	assert.Equal(t, "B", eid.Node, "volume owned by node B")
	assert.True(t, mzB.hasVol("tank/csi/remote"), "zvol created on node B via forwarding")

	require.NoError(t, ca.DeleteVolume(context.Background(), vol.VolumeID, nil))
	assert.False(t, mzB.hasVol("tank/csi/remote"), "delete forwarded to node B")
}

func TestForward_AuthAndErrorPropagation(t *testing.T) {
	ca, _, cleanup := twoNodeSetup(t)
	defer cleanup()

	// Expanding a nonexistent volume on B should surface a driver error with the
	// remote code preserved across the wire.
	_, _, err := ca.ExpandVolume(context.Background(),
		externalID{Node: "B", Dataset: "tank/csi/ghost"}.String(),
		driver.CapacityRange{RequiredBytes: 16384}, nil)
	require.Error(t, err)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeInternal, de.Code)
}

// A non-Internal coded error from the remote node must survive the forwarding
// boundary with its code intact (here: FailedPrecondition for deleting a volume
// that still has a snapshot), not collapse to Internal.
func TestForward_CodedErrorPreservedAcrossForward(t *testing.T) {
	ca, _, cleanup := twoNodeSetup(t)
	defer cleanup()

	req := localCreateReq("withsnap", 16384)
	req.Parameters = map[string]string{"host": "B"}
	vol, err := ca.CreateVolume(context.Background(), req)
	require.NoError(t, err)

	_, err = ca.CreateSnapshot(context.Background(), &driver.CreateSnapshotRequest{SourceVolumeID: vol.VolumeID, Name: "s1"})
	require.NoError(t, err)

	err = ca.DeleteVolume(context.Background(), vol.VolumeID, nil)
	require.Error(t, err)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeFailedPrecondition, de.Code, "remote code preserved across forward")
}

func TestForward_AutoPicksRoomiest(t *testing.T) {
	ca, mzB, cleanup := twoNodeSetup(t)
	defer cleanup()

	// Pin a volume explicitly to A so B becomes the emptier (more-available) node.
	a1 := localCreateReq("a1", 16384)
	a1.Parameters = map[string]string{"host": "A"}
	_, err := ca.CreateVolume(context.Background(), a1)
	require.NoError(t, err)

	vol, err := ca.CreateVolume(context.Background(), localCreateReq("auto", 16384))
	require.NoError(t, err)
	eid, _ := parseExternalID(vol.VolumeID)
	assert.Equal(t, "B", eid.Node, "auto placed on the node with more available space")
	assert.True(t, mzB.hasVol("tank/csi/auto"))
}

// twoNodeSetupBoth is twoNodeSetup but exposes BOTH nodes' backing ZFS, so a
// test can stage asymmetric capacity vs. volume-count.
func twoNodeSetupBoth(t *testing.T) (ca *controller, mzA, mzB *memZfs, cleanup func()) {
	t.Helper()
	secret := "s3cret"

	mzB = newMemZfs()
	cB := newController(zfs.New(mzB), testConfig(), "local", &cluster.StaticResolver{Self: "B", Peers: nil}, cluster.NewClient(secret), zap.NewNop())
	srvB := httptest.NewServer(cluster.NewServer(secret, cB.dispatchForward))

	resA := &cluster.StaticResolver{Self: "A", Peers: []cluster.NodeInfo{
		{Node: "A", Addr: "127.0.0.1:1"},
		{Node: "B", Addr: strings.TrimPrefix(srvB.URL, "http://")},
	}}
	mzA = newMemZfs()
	ca = newController(zfs.New(mzA), testConfig(), "local", resA, cluster.NewClient(secret), zap.NewNop())
	return ca, mzA, mzB, srvB.Close
}

// Capacity-based placement: the node with FEWER volumes but LESS available space
// must lose to the emptier node — the exact case the old volume-count heuristic
// got wrong (one large, mostly-provisioned zvol looked "less loaded" than three
// tiny ones). Both nodes have room for the request, so this exercises ranking,
// not the availability filter.
func TestForward_AutoPicksMostAvailableNotFewestVolumes(t *testing.T) {
	ca, mzA, mzB, cleanup := twoNodeSetupBoth(t)
	defer cleanup()

	mzA.size, mzB.size = 100000, 100000
	mzA.vols["tank/csi/big"] = 50000 // 1 volume, ~50000 available
	mzB.vols["tank/csi/s1"] = 1000   // 3 volumes, ~97000 available
	mzB.vols["tank/csi/s2"] = 1000
	mzB.vols["tank/csi/s3"] = 1000

	vol, err := ca.CreateVolume(context.Background(), localCreateReq("auto", 16384))
	require.NoError(t, err)
	eid, _ := parseExternalID(vol.VolumeID)
	assert.Equal(t, "B", eid.Node, "placed on the node with more available space, despite it holding more volumes")
	assert.True(t, mzB.hasVol("tank/csi/auto"))
}

func TestForward_ListVolumesAggregatesAcrossPeers(t *testing.T) {
	ca, _, cleanup := twoNodeSetup(t)
	defer cleanup()

	onA := localCreateReq("v-on-a", 16384)
	onA.Parameters = map[string]string{"host": "A"}
	_, err := ca.CreateVolume(context.Background(), onA)
	require.NoError(t, err)

	onB := localCreateReq("v-on-b", 16384)
	onB.Parameters = map[string]string{"host": "B"}
	_, err = ca.CreateVolume(context.Background(), onB)
	require.NoError(t, err)

	vols, err := ca.ListVolumes(context.Background())
	require.NoError(t, err)

	byNode := map[string]string{} // node -> volume id seen
	for _, v := range vols {
		byNode[v.AccessibleTopology[0].Segments[topologyKey]] = v.VolumeID
	}
	assert.Contains(t, byNode, "A")
	assert.Contains(t, byNode, "B", "ListVolumes aggregated the remote node's volume via forwarding")
}

func TestUnauthorizedForwardRejected(t *testing.T) {
	mzB := newMemZfs()
	cB := newController(zfs.New(mzB), testConfig(), "local", &cluster.StaticResolver{Self: "B"}, cluster.NewClient("right"), zap.NewNop())
	srvB := httptest.NewServer(cluster.NewServer("right", cB.dispatchForward))
	defer srvB.Close()

	// Client with the wrong secret must be rejected.
	bad := cluster.NewClient("wrong")
	_, err := bad.Call(context.Background(), strings.TrimPrefix(srvB.URL, "http://"), mStats, nil)
	require.Error(t, err)
	var re *cluster.RemoteError
	require.ErrorAs(t, err, &re)
	assert.Contains(t, strings.ToLower(re.Msg), "unauthorized")
}
