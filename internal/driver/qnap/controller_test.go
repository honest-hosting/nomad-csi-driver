package qnap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

func testController(t *testing.T) (*controller, *fakeClient) {
	t.Helper()
	fc := newFakeClient()
	cfg := &config.QNAPConfig{Interfaces: []string{"eth0"}, DefaultPoolID: 1, Portal: "10.0.0.1"}
	c := newController(fc, newSessionManager(fc, "u", "p"), cfg, zap.NewNop())
	return c, fc
}

func createReq(name string, gib int64) *driver.CreateVolumeRequest {
	return &driver.CreateVolumeRequest{
		Name:          name,
		CapacityRange: driver.CapacityRange{RequiredBytes: gib * giB},
		VolumeCapabilities: []driver.VolumeCapability{
			{AccessType: driver.AccessTypeMount, AccessMode: driver.AccessModeSingleNodeWriter, FsType: "ext4"},
		},
	}
}

func TestCreateVolume_Block(t *testing.T) {
	c, fc := testController(t)
	vol, err := c.CreateVolume(context.Background(), createReq("vol-a", 1))
	require.NoError(t, err)
	assert.Equal(t, int64(giB), vol.CapacityBytes)

	eid, err := parseExternalID(vol.VolumeID)
	require.NoError(t, err)
	assert.True(t, eid.OwnTarget, "1:1 target should be owned")
	assert.Equal(t, "10.0.0.1:3260", vol.VolumeContext[ctxKeyPortal])
	assert.Equal(t, "ext4", vol.VolumeContext[ctxKeyFsType])
	assert.NotEmpty(t, vol.VolumeContext[ctxKeyIQN])

	assert.Len(t, fc.luns, 1)
	assert.Len(t, fc.targets, 1)
	assert.True(t, fc.luns[eid.LUNIndex].Mapped)
}

func TestCreateVolume_ThinDefaultAndOptOut(t *testing.T) {
	t.Run("default is thin", func(t *testing.T) {
		c, fc := testController(t)
		_, err := c.CreateVolume(context.Background(), createReq("vol-thin", 1))
		require.NoError(t, err)
		assert.True(t, fc.lastBlockReq.Thin, "qnap volumes default to thin")
	})
	t.Run("opt into thick via parameters.thin=false", func(t *testing.T) {
		c, fc := testController(t)
		req := createReq("vol-thick", 1)
		req.Parameters = map[string]string{"thin": "false"}
		_, err := c.CreateVolume(context.Background(), req)
		require.NoError(t, err)
		assert.False(t, fc.lastBlockReq.Thin, "parameters.thin=false selects thick")
	})
	t.Run("invalid thin rejected", func(t *testing.T) {
		c, _ := testController(t)
		req := createReq("vol-bad", 1)
		req.Parameters = map[string]string{"thin": "maybe"}
		_, err := c.CreateVolume(context.Background(), req)
		var de *driver.Error
		require.ErrorAs(t, err, &de)
		assert.Equal(t, driver.CodeInvalidArgument, de.Code)
	})
}

func TestCreateVolume_RejectsNonGiB(t *testing.T) {
	c, _ := testController(t)
	req := createReq("vol-x", 1)
	req.CapacityRange.RequiredBytes = giB + (giB / 2) // 1.5 GiB
	_, err := c.CreateVolume(context.Background(), req)
	require.Error(t, err)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeInvalidArgument, de.Code)
}

func TestCreateVolume_Idempotent(t *testing.T) {
	c, fc := testController(t)
	v1, err := c.CreateVolume(context.Background(), createReq("dup", 1))
	require.NoError(t, err)
	v2, err := c.CreateVolume(context.Background(), createReq("dup", 1))
	require.NoError(t, err)
	assert.Equal(t, v1.VolumeID, v2.VolumeID, "idempotent create returns same volume id")
	assert.Len(t, fc.luns, 1, "no duplicate LUN created")
}

func TestCreateVolume_AlreadyExistsDifferentSize(t *testing.T) {
	c, _ := testController(t)
	_, err := c.CreateVolume(context.Background(), createReq("conflict", 1))
	require.NoError(t, err)
	_, err = c.CreateVolume(context.Background(), createReq("conflict", 2))
	require.Error(t, err)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeAlreadyExists, de.Code)
}

func TestCreateVolume_FromSnapshot(t *testing.T) {
	c, fc := testController(t)
	src, err := c.CreateVolume(context.Background(), createReq("src", 1))
	require.NoError(t, err)

	snap, err := c.CreateSnapshot(context.Background(), &driver.CreateSnapshotRequest{SourceVolumeID: src.VolumeID, Name: "snap1"})
	require.NoError(t, err)

	req := createReq("restored", 1)
	req.ContentSource = &driver.ContentSource{SnapshotID: snap.SnapshotID}
	restored, err := c.CreateVolume(context.Background(), req)
	require.NoError(t, err)
	assert.NotEqual(t, src.VolumeID, restored.VolumeID)
	assert.Len(t, fc.luns, 2)
}

func TestCreateVolume_CloneThenResize(t *testing.T) {
	c, fc := testController(t)
	src, err := c.CreateVolume(context.Background(), createReq("clonesrc", 1))
	require.NoError(t, err)

	req := createReq("clonedst", 2) // larger than source -> clone-then-resize
	req.ContentSource = &driver.ContentSource{VolumeID: src.VolumeID}
	dst, err := c.CreateVolume(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, int64(2)*giB, dst.CapacityBytes)
	eid, _ := parseExternalID(dst.VolumeID)
	assert.Equal(t, int64(2)*giB, fc.luns[eid.LUNIndex].CapacityBytes)
}

func TestDeleteVolume(t *testing.T) {
	c, fc := testController(t)
	vol, err := c.CreateVolume(context.Background(), createReq("del", 1))
	require.NoError(t, err)
	require.NoError(t, c.DeleteVolume(context.Background(), vol.VolumeID, nil))
	assert.Empty(t, fc.luns)
	assert.Empty(t, fc.targets, "owned 1:1 target deleted with the LUN")
}

func TestDeleteVolume_Idempotent(t *testing.T) {
	c, _ := testController(t)
	vol, err := c.CreateVolume(context.Background(), createReq("del2", 1))
	require.NoError(t, err)
	require.NoError(t, c.DeleteVolume(context.Background(), vol.VolumeID, nil))
	// Deleting again must not error (LUN already gone).
	require.NoError(t, c.DeleteVolume(context.Background(), vol.VolumeID, nil))
}

// QuTScloud returns a ghost row (empty name) for a removed LUN index and a -1
// from remove_lun. DeleteVolume must treat that as already-deleted, not surface
// the misleading "invalid username or password" error.
func TestDeleteVolume_GhostLUNTreatedAsGone(t *testing.T) {
	c, fc := testController(t)
	vol, err := c.CreateVolume(context.Background(), createReq("ghosty", 1))
	require.NoError(t, err)
	eid, err := parseExternalID(vol.VolumeID)
	require.NoError(t, err)

	fc.makeGhost(eid.LUNIndex) // removed out-of-band: GetLUN ghost row + remove_lun -1
	require.NoError(t, c.DeleteVolume(context.Background(), vol.VolumeID, nil),
		"a ghost/already-removed LUN must be treated as deleted")
}

// DATA SAFETY: QNAP reuses LUN indices. A stale volume_id whose index now holds
// a DIFFERENT volume's LUN must never delete that LUN.
func TestDeleteVolume_ReusedIndexNotDeleted(t *testing.T) {
	c, fc := testController(t)
	vol, err := c.CreateVolume(context.Background(), createReq("volA", 1))
	require.NoError(t, err)
	eid, err := parseExternalID(vol.VolumeID)
	require.NoError(t, err)

	fc.replaceLUN(eid.LUNIndex, "volB-reused") // a different volume reused the index

	require.NoError(t, c.DeleteVolume(context.Background(), vol.VolumeID, nil),
		"deleting a stale volume_id must be a safe no-op")
	got, ok := fc.lunByIndex(eid.LUNIndex)
	require.True(t, ok, "the reused LUN must NOT be deleted")
	assert.Equal(t, "volB-reused", got.Name)
}

func TestExpandVolume_ReusedIndexRejected(t *testing.T) {
	c, fc := testController(t)
	vol, err := c.CreateVolume(context.Background(), createReq("volA", 1))
	require.NoError(t, err)
	eid, err := parseExternalID(vol.VolumeID)
	require.NoError(t, err)

	fc.replaceLUN(eid.LUNIndex, "volB-reused")

	_, _, err = c.ExpandVolume(context.Background(), vol.VolumeID, driver.CapacityRange{RequiredBytes: 2 * giB}, nil)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeNotFound, de.Code, "expanding a reused index must report NotFound, not resize the wrong LUN")
}

func TestExpandVolume(t *testing.T) {
	c, fc := testController(t)
	vol, err := c.CreateVolume(context.Background(), createReq("grow", 1))
	require.NoError(t, err)
	newBytes, nodeExpand, err := c.ExpandVolume(context.Background(), vol.VolumeID, driver.CapacityRange{RequiredBytes: 3 * giB}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3)*giB, newBytes)
	assert.True(t, nodeExpand)
	eid, _ := parseExternalID(vol.VolumeID)
	assert.Equal(t, int64(3)*giB, fc.luns[eid.LUNIndex].CapacityBytes)
}

func TestSnapshotLifecycle(t *testing.T) {
	c, fc := testController(t)
	vol, err := c.CreateVolume(context.Background(), createReq("snapvol", 1))
	require.NoError(t, err)

	snap, err := c.CreateSnapshot(context.Background(), &driver.CreateSnapshotRequest{SourceVolumeID: vol.VolumeID, Name: "s1"})
	require.NoError(t, err)
	assert.True(t, snap.ReadyToUse)
	assert.Equal(t, "Fri May 29 10:13:33 2026", snap.CreationTime.Format("Mon Jan _2 15:04:05 2006"),
		"creation time parsed from QNAP create_time")
	assert.Len(t, fc.snaps, 1)
	// CSI-managed snapshots must not auto-expire.
	for _, s := range fc.snaps {
		assert.Equal(t, 0, s.ExpireMin)
		assert.True(t, s.Vital)
	}

	require.NoError(t, c.DeleteSnapshot(context.Background(), snap.SnapshotID, nil))
	assert.Empty(t, fc.snaps)
}

func TestListSnapshots(t *testing.T) {
	c, _ := testController(t)
	vol, err := c.CreateVolume(context.Background(), createReq("lsnap", 1))
	require.NoError(t, err)
	snap, err := c.CreateSnapshot(context.Background(), &driver.CreateSnapshotRequest{SourceVolumeID: vol.VolumeID, Name: "s1"})
	require.NoError(t, err)

	all, err := c.ListSnapshots(context.Background(), "")
	require.NoError(t, err)
	require.Len(t, all, 1)
	assert.Equal(t, snap.SnapshotID, all[0].SnapshotID)
	assert.Equal(t, vol.VolumeID, all[0].SourceVolumeID, "snapshot reports its source volume")
	assert.False(t, all[0].CreationTime.IsZero(), "list reports the QNAP create_time")

	// Filtered to the source volume.
	filtered, err := c.ListSnapshots(context.Background(), vol.VolumeID)
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	assert.Equal(t, snap.SnapshotID, filtered[0].SnapshotID)
}

// DATA SAFETY: refuse to delete a volume whose LUN still has snapshots
// (dependent), consistent with the local backend.
func TestDeleteVolume_RefusesWithSnapshot(t *testing.T) {
	c, _ := testController(t)
	vol, err := c.CreateVolume(context.Background(), createReq("delsnap", 1))
	require.NoError(t, err)
	_, err = c.CreateSnapshot(context.Background(), &driver.CreateSnapshotRequest{SourceVolumeID: vol.VolumeID, Name: "s1"})
	require.NoError(t, err)

	err = c.DeleteVolume(context.Background(), vol.VolumeID, nil)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeFailedPrecondition, de.Code)
}

func TestListVolumes(t *testing.T) {
	c, _ := testController(t)
	v1, err := c.CreateVolume(context.Background(), createReq("lv1", 1))
	require.NoError(t, err)
	v2, err := c.CreateVolume(context.Background(), createReq("lv2", 1))
	require.NoError(t, err)

	vols, err := c.ListVolumes(context.Background())
	require.NoError(t, err)
	ids := map[string]bool{}
	for _, v := range vols {
		ids[v.VolumeID] = true
	}
	assert.True(t, ids[v1.VolumeID])
	assert.True(t, ids[v2.VolumeID])
}

func TestGetCapacity(t *testing.T) {
	c, _ := testController(t)
	avail, err := c.GetCapacity(context.Background(), nil, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(1)<<42, avail)
}
