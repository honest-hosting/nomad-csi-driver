package qnap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// 1:N shared-target mapping is gated off for v1.
func TestResolveParams_RejectsTargetIndex(t *testing.T) {
	cfg := &config.QNAPConfig{DefaultPoolID: 1}
	_, err := resolveParams(cfg, map[string]string{"targetIndex": "3"},
		[]driver.VolumeCapability{{AccessType: driver.AccessTypeMount, FsType: "ext4"}})
	require.Error(t, err)
	var de *driver.Error
	require.True(t, errors.As(err, &de))
	assert.Equal(t, driver.CodeInvalidArgument, de.Code)
}

// A CreateVolume retry whose existing LUN reports a sub-GiB-different capacity
// (sector rounding / thin metadata) must be treated as idempotent, not rejected
// as a size conflict.
func TestCreateVolume_IdempotentGiBTolerance(t *testing.T) {
	c, _ := testController(t)
	vol1, err := c.CreateVolume(context.Background(), createReq("vol-a", 1))
	require.NoError(t, err)

	// Simulate the appliance reporting a slightly-off capacity for the LUN.
	c.cache.mu.Lock()
	lun := c.cache.byName["vol-a"]
	lun.CapacityBytes += 4096
	c.cache.byName["vol-a"] = lun
	c.cache.mu.Unlock()

	vol2, err := c.CreateVolume(context.Background(), createReq("vol-a", 1))
	require.NoError(t, err, "sub-GiB capacity delta must be tolerated as idempotent")
	assert.Equal(t, vol1.VolumeID, vol2.VolumeID)
}

// A genuine size difference (>= 1 GiB) for the same name is still a conflict.
func TestCreateVolume_DifferentSizeConflicts(t *testing.T) {
	c, _ := testController(t)
	_, err := c.CreateVolume(context.Background(), createReq("vol-a", 1))
	require.NoError(t, err)

	_, err = c.CreateVolume(context.Background(), createReq("vol-a", 3))
	require.Error(t, err)
	var de *driver.Error
	require.True(t, errors.As(err, &de))
	assert.Equal(t, driver.CodeAlreadyExists, de.Code)
}
