package local

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// Expansion must round to the volume's actual volblocksize (read on the owning
// node), not a controller-side default, and an idempotent re-expand must not be
// mistaken for a shrink.
func TestLocalExpand_RoundsToActualVolblocksize(t *testing.T) {
	c, mz := singleNodeController(t)
	mz.blocksize = 64 << 10 // the volume's real block size

	vol, err := c.CreateVolume(context.Background(), localCreateReq("v64", 64<<10))
	require.NoError(t, err)

	// A request that is not a 64K multiple rounds up to the 64K boundary.
	newBytes, _, err := c.ExpandVolume(context.Background(), vol.VolumeID,
		driver.CapacityRange{RequiredBytes: 64<<10 + 1}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(128<<10), newBytes, "rounded up to the volume's 64K block size")

	// Re-issuing the same expand is idempotent, not a shrink.
	again, _, err := c.ExpandVolume(context.Background(), vol.VolumeID,
		driver.CapacityRange{RequiredBytes: 64<<10 + 1}, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(128<<10), again)
}

// An expand that rounds above the CSI limit is rejected.
func TestLocalExpand_LimitEnforced(t *testing.T) {
	c, _ := singleNodeController(t)
	vol, err := c.CreateVolume(context.Background(), localCreateReq("vlim", 16384))
	require.NoError(t, err)

	_, _, err = c.ExpandVolume(context.Background(), vol.VolumeID,
		driver.CapacityRange{RequiredBytes: 10 * 16384, LimitBytes: 5 * 16384}, nil)
	require.Error(t, err)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeOutOfRange, de.Code)
}

// A create that reuses an existing name but names a DIFFERENT content source must
// be rejected, not silently aliased to the first clone.
func TestLocalClone_ProvenanceMismatchRejected(t *testing.T) {
	c, _ := singleNodeController(t)
	src, err := c.CreateVolume(context.Background(), localCreateReq("psrc", 16384))
	require.NoError(t, err)

	req := localCreateReq("pdst", 16384)
	req.ContentSource = &driver.ContentSource{VolumeID: src.VolumeID}
	_, err = c.CreateVolume(context.Background(), req)
	require.NoError(t, err)

	// Idempotent retry with the SAME source returns the existing clone.
	_, err = c.CreateVolume(context.Background(), req)
	require.NoError(t, err)

	// Same name+size but a DIFFERENT source must conflict.
	src2, err := c.CreateVolume(context.Background(), localCreateReq("psrc2", 16384))
	require.NoError(t, err)
	req2 := localCreateReq("pdst", 16384)
	req2.ContentSource = &driver.ContentSource{VolumeID: src2.VolumeID}
	_, err = c.CreateVolume(context.Background(), req2)
	require.Error(t, err)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeAlreadyExists, de.Code)
}
