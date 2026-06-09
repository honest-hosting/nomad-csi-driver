package csi

import (
	"context"
	"net"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
)

// testClients dials an in-memory gRPC server serving the given backend in the
// given mode, and returns CSI clients for it.
type testClients struct {
	identity   csipb.IdentityClient
	controller csipb.ControllerClient
	node       csipb.NodeClient
}

func newTestClients(t *testing.T, b driver.Backend, mode driver.Mode) testClients {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs, err := newGRPCServer(b, mode, zap.NewNop(), metrics.New("test"))
	require.NoError(t, err)

	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return testClients{
		identity:   csipb.NewIdentityClient(conn),
		controller: csipb.NewControllerClient(conn),
		node:       csipb.NewNodeClient(conn),
	}
}

func mountCap(mode csipb.VolumeCapability_AccessMode_Mode, fsType string) *csipb.VolumeCapability {
	return &csipb.VolumeCapability{
		AccessType: &csipb.VolumeCapability_Mount{Mount: &csipb.VolumeCapability_MountVolume{FsType: fsType}},
		AccessMode: &csipb.VolumeCapability_AccessMode{Mode: mode},
	}
}

func fullBackend() *fakeBackend {
	return &fakeBackend{
		name:   "fake",
		plugin: "io.honesthosting.csi.fake",
		caps: driver.Capabilities{
			CreateDelete: true, PublishUnpublish: false, Expand: true,
			Snapshot: true, Clone: true, GetCapacity: true, ListVolumes: true,
			NodeStage: true, NodeExpand: true, NodeVolumeStats: true, Topology: true,
		},
		ctrl: &fakeController{},
		node: &fakeNode{nodeID: "node-1", topology: &driver.Topology{Segments: map[string]string{"x/node": "node-1"}}},
	}
}

func TestIdentity_GetPluginInfo(t *testing.T) {
	c := newTestClients(t, fullBackend(), driver.ModeMonolith)
	resp, err := c.identity.GetPluginInfo(context.Background(), &csipb.GetPluginInfoRequest{})
	require.NoError(t, err)
	assert.Equal(t, "io.honesthosting.csi.fake", resp.GetName())
	assert.NotEmpty(t, resp.GetVendorVersion())
}

func TestIdentity_Probe(t *testing.T) {
	b := fullBackend()
	c := newTestClients(t, b, driver.ModeController)
	_, err := c.identity.Probe(context.Background(), &csipb.ProbeRequest{})
	require.NoError(t, err)

	b.probeErr = driver.Unavailable("appliance unreachable")
	c2 := newTestClients(t, b, driver.ModeController)
	_, err = c2.identity.Probe(context.Background(), &csipb.ProbeRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestController_CreateVolume(t *testing.T) {
	b := fullBackend()
	c := newTestClients(t, b, driver.ModeController)
	resp, err := c.controller.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name:               "vol-a",
		CapacityRange:      &csipb.CapacityRange{RequiredBytes: 1 << 30},
		VolumeCapabilities: []*csipb.VolumeCapability{mountCap(csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "ext4")},
		Parameters:         map[string]string{"fsType": "ext4"},
	})
	require.NoError(t, err)
	assert.Equal(t, "vol-a", resp.GetVolume().GetVolumeId())
	assert.Equal(t, int64(1<<30), resp.GetVolume().GetCapacityBytes())
	require.NotNil(t, b.ctrl.lastCreate)
	assert.Equal(t, "ext4", b.ctrl.lastCreate.VolumeCapabilities[0].FsType)
}

func TestController_CreateVolume_RejectsMultiNode(t *testing.T) {
	c := newTestClients(t, fullBackend(), driver.ModeController)
	_, err := c.controller.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name:               "vol-b",
		VolumeCapabilities: []*csipb.VolumeCapability{mountCap(csipb.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER, "ext4")},
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestController_CreateVolume_RequiresName(t *testing.T) {
	c := newTestClients(t, fullBackend(), driver.ModeController)
	_, err := c.controller.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		VolumeCapabilities: []*csipb.VolumeCapability{mountCap(csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "ext4")},
	})
	require.Error(t, err)
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestController_ErrorMapping(t *testing.T) {
	b := fullBackend()
	b.ctrl.deleteFn = func(string) error { return driver.NotFound("volume gone") }
	c := newTestClients(t, b, driver.ModeController)
	_, err := c.controller.DeleteVolume(context.Background(), &csipb.DeleteVolumeRequest{VolumeId: "x"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestController_SnapshotGating(t *testing.T) {
	b := fullBackend()
	b.caps.Snapshot = false // not advertised
	c := newTestClients(t, b, driver.ModeController)
	_, err := c.controller.CreateSnapshot(context.Background(), &csipb.CreateSnapshotRequest{
		SourceVolumeId: "v", Name: "snap",
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestController_GetCapabilities(t *testing.T) {
	c := newTestClients(t, fullBackend(), driver.ModeController)
	resp, err := c.controller.ControllerGetCapabilities(context.Background(), &csipb.ControllerGetCapabilitiesRequest{})
	require.NoError(t, err)
	var have []csipb.ControllerServiceCapability_RPC_Type
	for _, cap := range resp.GetCapabilities() {
		have = append(have, cap.GetRpc().GetType())
	}
	assert.Contains(t, have, csipb.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME)
	assert.Contains(t, have, csipb.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT)
	assert.Contains(t, have, csipb.ControllerServiceCapability_RPC_CLONE_VOLUME)
	assert.NotContains(t, have, csipb.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME)
}

func TestController_NotServedInNodeMode(t *testing.T) {
	// A node-only process must not serve the controller service.
	b := fullBackend()
	c := newTestClients(t, b, driver.ModeNode)
	_, err := c.controller.CreateVolume(context.Background(), &csipb.CreateVolumeRequest{
		Name:               "v",
		VolumeCapabilities: []*csipb.VolumeCapability{mountCap(csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "ext4")},
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unimplemented, status.Code(err))
}

func TestController_ListVolumes(t *testing.T) {
	c := newTestClients(t, fullBackend(), driver.ModeController)
	resp, err := c.controller.ListVolumes(context.Background(), &csipb.ListVolumesRequest{})
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	assert.Equal(t, "vol-a", resp.GetEntries()[0].GetVolume().GetVolumeId())
}

func TestNode_GetVolumeStats(t *testing.T) {
	c := newTestClients(t, fullBackend(), driver.ModeNode)
	resp, err := c.node.NodeGetVolumeStats(context.Background(), &csipb.NodeGetVolumeStatsRequest{
		VolumeId: "v1", VolumePath: "/local/csi/v1",
	})
	require.NoError(t, err)
	require.Len(t, resp.GetUsage(), 2)
	assert.Equal(t, csipb.VolumeUsage_BYTES, resp.GetUsage()[0].GetUnit())
	assert.Equal(t, int64(1<<30), resp.GetUsage()[0].GetTotal())
}

func TestNode_StageAndInfo(t *testing.T) {
	b := fullBackend()
	c := newTestClients(t, b, driver.ModeNode)

	info, err := c.node.NodeGetInfo(context.Background(), &csipb.NodeGetInfoRequest{})
	require.NoError(t, err)
	assert.Equal(t, "node-1", info.GetNodeId())
	assert.Equal(t, "node-1", info.GetAccessibleTopology().GetSegments()["x/node"])

	_, err = c.node.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "vol-a",
		StagingTargetPath: "/local/csi/staging/vol-a",
		VolumeCapability:  mountCap(csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER, "xfs"),
	})
	require.NoError(t, err)
	require.NotNil(t, b.node.lastStage)
	assert.Equal(t, "xfs", b.node.lastStage.VolumeCapability.FsType)
}
