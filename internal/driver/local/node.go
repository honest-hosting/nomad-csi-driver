package local

import (
	"context"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
	"github.com/honest-hosting/nomad-csi-driver/internal/zfs"
)

// node implements driver.NodeBackend for the local backend: locate the zvol
// device, then the shared format/mount layer. There is no attach step — the
// zvol already lives on this node (it is topology-pinned here).
type node struct {
	cfg     *config.LocalConfig
	z       *zfs.ZFS
	nodeID  string
	mounter *mountutil.Mounter
	log     *zap.Logger
	nodeM   *metrics.NodeMetrics // shared node metrics (staged count); nil-safe
	stats   *stats.Registry      // per-volume usage stats; nil-safe no-op when disabled

	// waitForPath polls until the device path exists; overridable in tests.
	waitForPath func(ctx context.Context, path string) (string, error)
}

func (n *node) StageVolume(ctx context.Context, req *driver.StageRequest) (err error) {
	defer func() {
		if err == nil {
			n.nodeM.StagedInc()
			n.stats.Track(req.VolumeID, req.StagingTargetPath, stageAccessType(req.VolumeCapability.AccessType))
		}
	}()
	// Wrong-node guard (data safety): a stage that lands on a non-owner node must
	// refuse rather than risk materializing a second, empty zvol.
	if owner := req.VolumeContext[ctxKeyNode]; owner != "" && owner != n.nodeID {
		return driver.FailedPrecondition("volume %s is owned by node %q but staged on %q", req.VolumeID, owner, n.nodeID)
	}
	dataset := req.VolumeContext[ctxKeyDataset]
	if dataset == "" {
		return driver.InvalidArgument("volume context missing dataset")
	}
	// Checkpoint 3: the volume's pool must still be present + ONLINE on this node
	// (it may have been exported, or the node reimaged, since create). Probe for a
	// clear error rather than a bare device-not-found from waitForPath.
	if n.z != nil {
		pool := poolOf(dataset)
		present, online, err := n.z.PoolStatus(ctx, pool)
		if err != nil {
			return driver.Unavailable("checking pool %q on node %q: %v", pool, n.nodeID, err)
		}
		if !present || !online {
			return driver.FailedPrecondition("pool %q unavailable on node %q", pool, n.nodeID)
		}
	}
	dev, err := n.waitForPath(ctx, zfs.DevicePath(dataset))
	if err != nil {
		return driver.Internal("zvol device did not appear: %v", err)
	}

	if req.VolumeCapability.AccessType == driver.AccessTypeBlock {
		return nil // NodePublishVolume bind-mounts the device node
	}
	fsType := req.VolumeContext[ctxKeyFsType]
	if fsType == "" {
		fsType = req.VolumeCapability.FsType
	}
	if _, err := n.mounter.FormatIfEmpty(ctx, dev, fsType, nil); err != nil {
		return driver.Internal("format: %v", err)
	}
	if err := n.mounter.Mount(ctx, dev, req.StagingTargetPath, fsType, req.VolumeCapability.MountFlags); err != nil {
		return driver.Internal("mount: %v", err)
	}
	return nil
}

func (n *node) UnstageVolume(ctx context.Context, req *driver.UnstageRequest) error {
	// The zvol stays on this node; only the mount is torn down.
	if err := n.mounter.Unmount(ctx, req.StagingTargetPath); err != nil {
		return driver.Internal("unmount staging: %v", err)
	}
	n.nodeM.StagedDec()
	n.stats.Untrack(req.VolumeID)
	return nil
}

// stageAccessType maps the CSI access type to the stats access-type label.
func stageAccessType(at driver.AccessType) string {
	if at == driver.AccessTypeBlock {
		return stats.AccessBlock
	}
	return stats.AccessMount
}

func (n *node) PublishVolume(ctx context.Context, req *driver.PublishRequest) error {
	if req.VolumeCapability.AccessType == driver.AccessTypeBlock {
		dataset := req.VolumeContext[ctxKeyDataset]
		dev, err := n.waitForPath(ctx, zfs.DevicePath(dataset))
		if err != nil {
			return driver.Internal("zvol device did not appear: %v", err)
		}
		if err := n.mounter.BindMount(ctx, dev, req.TargetPath, false, req.Readonly); err != nil {
			return driver.Internal("bind device: %v", err)
		}
		return nil
	}
	if err := n.mounter.BindMount(ctx, req.StagingTargetPath, req.TargetPath, true, req.Readonly); err != nil {
		return driver.Internal("bind mount: %v", err)
	}
	return nil
}

func (n *node) UnpublishVolume(ctx context.Context, req *driver.UnpublishRequest) error {
	if err := n.mounter.Unmount(ctx, req.TargetPath); err != nil {
		return driver.Internal("unpublish unmount: %v", err)
	}
	return nil
}

func (n *node) ExpandVolume(ctx context.Context, req *driver.NodeExpandRequest) (int64, error) {
	if req.VolumeCapability.AccessType == driver.AccessTypeBlock {
		return req.CapacityRange.RequiredBytes, nil // raw block: nothing to grow
	}
	dev, err := n.mounter.SourceDevice(ctx, req.VolumePath)
	if err != nil {
		return 0, driver.Internal("locating device for expand: %v", err)
	}
	if err := n.mounter.Resize(ctx, dev, req.VolumePath, req.VolumeCapability.FsType); err != nil {
		return 0, driver.Internal("resize filesystem: %v", err)
	}
	return req.CapacityRange.RequiredBytes, nil
}

func (n *node) GetInfo(_ context.Context) (*driver.NodeInfo, error) {
	return &driver.NodeInfo{
		NodeID:             n.nodeID,
		AccessibleTopology: &driver.Topology{Segments: map[string]string{topologyKey: n.nodeID}},
	}, nil
}

func (n *node) GetVolumeStats(_ context.Context, _ string, volumePath string) (*driver.VolumeStats, error) {
	s, err := mountutil.StatFS(volumePath)
	if err != nil {
		return nil, driver.Internal("statfs %s: %v", volumePath, err)
	}
	return &driver.VolumeStats{
		TotalBytes: s.TotalBytes, UsedBytes: s.UsedBytes, AvailableBytes: s.AvailableBytes,
		TotalInodes: s.TotalInodes, UsedInodes: s.UsedInodes, FreeInodes: s.FreeInodes,
	}, nil
}

// osWaitForPath polls until path exists, returning its symlink-resolved target.
func osWaitForPath(timeout time.Duration) func(context.Context, string) (string, error) {
	return func(ctx context.Context, path string) (string, error) {
		deadline := time.Now().Add(timeout)
		for {
			if real, err := filepath.EvalSymlinks(path); err == nil {
				return real, nil
			}
			if time.Now().After(deadline) {
				return "", context.DeadlineExceeded
			}
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
	}
}
