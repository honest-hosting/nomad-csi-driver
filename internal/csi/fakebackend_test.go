package csi

import (
	"context"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// fakeBackend is a configurable driver.Backend for exercising the CSI layer in
// isolation. Behavior is supplied via the controller/node func fields.
type fakeBackend struct {
	name     string
	plugin   string
	caps     driver.Capabilities
	ctrl     *fakeController
	node     *fakeNode
	probeErr error
}

func (f *fakeBackend) Name() string                      { return f.name }
func (f *fakeBackend) PluginName() string                { return f.plugin }
func (f *fakeBackend) Capabilities() driver.Capabilities { return f.caps }
func (f *fakeBackend) Probe(context.Context) error       { return f.probeErr }
func (f *fakeBackend) Controller() driver.ControllerBackend {
	if f.ctrl == nil {
		return nil
	}
	return f.ctrl
}
func (f *fakeBackend) Node() driver.NodeBackend {
	if f.node == nil {
		return nil
	}
	return f.node
}

type fakeController struct {
	lastCreate *driver.CreateVolumeRequest
	createFn   func(*driver.CreateVolumeRequest) (*driver.Volume, error)
	deleteFn   func(string) error
	expandFn   func(string, driver.CapacityRange) (int64, bool, error)
	snapFn     func(*driver.CreateSnapshotRequest) (*driver.Snapshot, error)
	existsFn   func(string) (bool, error)
}

func (c *fakeController) VolumeExists(_ context.Context, volumeID string) (bool, error) {
	if c.existsFn != nil {
		return c.existsFn(volumeID)
	}
	return true, nil
}

func (c *fakeController) CreateVolume(_ context.Context, req *driver.CreateVolumeRequest) (*driver.Volume, error) {
	c.lastCreate = req
	if c.createFn != nil {
		return c.createFn(req)
	}
	return &driver.Volume{VolumeID: req.Name, CapacityBytes: req.CapacityRange.RequiredBytes}, nil
}

func (c *fakeController) DeleteVolume(_ context.Context, volumeID string, _ map[string]string) error {
	if c.deleteFn != nil {
		return c.deleteFn(volumeID)
	}
	return nil
}

func (c *fakeController) ExpandVolume(_ context.Context, volumeID string, cr driver.CapacityRange, _ map[string]string) (int64, bool, error) {
	if c.expandFn != nil {
		return c.expandFn(volumeID, cr)
	}
	return cr.RequiredBytes, true, nil
}

func (c *fakeController) ControllerPublishVolume(context.Context, string, string, driver.VolumeCapability, bool, map[string]string, map[string]string) (map[string]string, error) {
	return nil, nil
}
func (c *fakeController) ControllerUnpublishVolume(context.Context, string, string, map[string]string) error {
	return nil
}

func (c *fakeController) CreateSnapshot(_ context.Context, req *driver.CreateSnapshotRequest) (*driver.Snapshot, error) {
	if c.snapFn != nil {
		return c.snapFn(req)
	}
	return &driver.Snapshot{SnapshotID: req.Name, SourceVolumeID: req.SourceVolumeID, ReadyToUse: true}, nil
}
func (c *fakeController) DeleteSnapshot(context.Context, string, map[string]string) error { return nil }
func (c *fakeController) GetCapacity(context.Context, []driver.Topology, map[string]string) (int64, error) {
	return 1 << 40, nil
}
func (c *fakeController) ListVolumes(context.Context) ([]driver.VolumeInfo, error) {
	return []driver.VolumeInfo{{VolumeID: "vol-a", CapacityBytes: 1 << 30}}, nil
}
func (c *fakeController) ListSnapshots(_ context.Context, sourceVolumeID string) ([]driver.SnapshotInfo, error) {
	return []driver.SnapshotInfo{{SnapshotID: "snap-a", SourceVolumeID: "vol-a", SizeBytes: 1 << 30, ReadyToUse: true}}, nil
}

type fakeNode struct {
	nodeID    string
	topology  *driver.Topology
	lastStage *driver.StageRequest
	stageFn   func(*driver.StageRequest) error
}

func (n *fakeNode) StageVolume(_ context.Context, req *driver.StageRequest) error {
	n.lastStage = req
	if n.stageFn != nil {
		return n.stageFn(req)
	}
	return nil
}
func (n *fakeNode) UnstageVolume(context.Context, *driver.UnstageRequest) error     { return nil }
func (n *fakeNode) PublishVolume(context.Context, *driver.PublishRequest) error     { return nil }
func (n *fakeNode) UnpublishVolume(context.Context, *driver.UnpublishRequest) error { return nil }
func (n *fakeNode) ExpandVolume(_ context.Context, req *driver.NodeExpandRequest) (int64, error) {
	return req.CapacityRange.RequiredBytes, nil
}
func (n *fakeNode) GetInfo(context.Context) (*driver.NodeInfo, error) {
	return &driver.NodeInfo{NodeID: n.nodeID, AccessibleTopology: n.topology}, nil
}
func (n *fakeNode) GetVolumeStats(context.Context, string, string) (*driver.VolumeStats, error) {
	return &driver.VolumeStats{TotalBytes: 1 << 30, UsedBytes: 1 << 20, AvailableBytes: (1 << 30) - (1 << 20)}, nil
}
