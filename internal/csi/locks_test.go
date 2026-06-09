package csi

import (
	"context"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

func TestKeyedLocker(t *testing.T) {
	k := newKeyedLocker()

	rel, ok := k.tryAcquire("a")
	require.True(t, ok)

	_, ok2 := k.tryAcquire("a")
	require.False(t, ok2, "same key must be reported busy")

	relB, okB := k.tryAcquire("b")
	require.True(t, okB, "a different key must be free")
	relB()

	rel()
	_, ok3 := k.tryAcquire("a")
	require.True(t, ok3, "key must be free after release")
}

// TestController_CreateVolume_ConcurrentSameNameAborted holds the per-name lock
// in a parked CreateVolume and asserts a second concurrent create for the same
// name fails fast with Aborted (the project's contended-key policy).
func TestController_CreateVolume_ConcurrentSameNameAborted(t *testing.T) {
	b := fullBackend()
	entered := make(chan struct{})
	release := make(chan struct{})
	b.ctrl.createFn = func(req *driver.CreateVolumeRequest) (*driver.Volume, error) {
		close(entered) // reached only by the call that holds the lock
		<-release
		return &driver.Volume{VolumeID: req.Name}, nil
	}
	c := newTestClients(t, b, driver.ModeController)

	req := &csipb.CreateVolumeRequest{
		Name:               "vol-x",
		VolumeCapabilities: []*csipb.VolumeCapability{mountCap(csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "ext4")},
	}

	firstErr := make(chan error, 1)
	go func() {
		_, err := c.controller.CreateVolume(context.Background(), req)
		firstErr <- err
	}()
	<-entered // first call now holds the lock, parked in createFn

	_, err := c.controller.CreateVolume(context.Background(), req)
	require.Error(t, err)
	assert.Equal(t, codes.Aborted, status.Code(err), "contended same-name create must be Aborted")

	close(release)
	require.NoError(t, <-firstErr)
}

// A create for a *different* name must not be blocked by an in-flight create.
func TestController_CreateVolume_ConcurrentDifferentNameOK(t *testing.T) {
	b := fullBackend()
	entered := make(chan struct{})
	release := make(chan struct{})
	b.ctrl.createFn = func(req *driver.CreateVolumeRequest) (*driver.Volume, error) {
		if req.Name == "vol-hold" {
			close(entered)
			<-release
		}
		return &driver.Volume{VolumeID: req.Name}, nil
	}
	c := newTestClients(t, b, driver.ModeController)

	go func() {
		_, _ = c.controller.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
			Name:               "vol-hold",
			VolumeCapabilities: []*csipb.VolumeCapability{mountCap(csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "ext4")},
		})
	}()
	<-entered

	_, err := c.controller.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name:               "vol-other",
		VolumeCapabilities: []*csipb.VolumeCapability{mountCap(csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "ext4")},
	})
	require.NoError(t, err, "a different key must proceed concurrently")
	close(release)
}

func TestController_ValidateVolumeCapabilities_NotFound(t *testing.T) {
	b := fullBackend()
	b.ctrl.existsFn = func(string) (bool, error) { return false, nil }
	c := newTestClients(t, b, driver.ModeController)

	_, err := c.controller.ValidateVolumeCapabilities(context.Background(), &csipb.ValidateVolumeCapabilitiesRequest{
		VolumeId:           "missing",
		VolumeCapabilities: []*csipb.VolumeCapability{mountCap(csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "ext4")},
	})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestController_ValidateVolumeCapabilities_Confirmed(t *testing.T) {
	b := fullBackend() // existsFn nil -> default exists=true
	c := newTestClients(t, b, driver.ModeController)

	resp, err := c.controller.ValidateVolumeCapabilities(context.Background(), &csipb.ValidateVolumeCapabilitiesRequest{
		VolumeId:           "vol-a",
		VolumeCapabilities: []*csipb.VolumeCapability{mountCap(csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "ext4")},
	})
	require.NoError(t, err)
	require.NotNil(t, resp.GetConfirmed())
}

// A backend that does not advertise CreateDelete must not dispatch
// CreateVolume/DeleteVolume into the controller.
func TestController_CreateDeleteGatedByCaps(t *testing.T) {
	b := fullBackend()
	b.caps.CreateDelete = false
	c := newTestClients(t, b, driver.ModeController)

	_, err := c.controller.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name:               "v",
		VolumeCapabilities: []*csipb.VolumeCapability{mountCap(csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "ext4")},
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err))

	_, err = c.controller.DeleteVolume(context.Background(), &csipb.DeleteVolumeRequest{VolumeId: "v"})
	require.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}
