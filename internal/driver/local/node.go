package local

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
	"github.com/honest-hosting/nomad-csi-driver/internal/zfs"
)

// node implements driver.NodeBackend for the local backend: locate the zvol
// device, then the shared format/mount layer. There is no attach step — the
// zvol already lives on this node (it is topology-pinned here).
type node struct {
	cfg           *config.LocalConfig
	z             *zfs.ZFS
	nodeID        string
	parentDataset string // the "<pool>/<parentDataset>/<vol>" middle segment; scopes StagedCount
	mounter       *mountutil.Mounter
	log           *zap.Logger
	stats         *stats.Registry // per-volume usage stats; nil-safe no-op when disabled

	// waitForPath polls until the device path exists; overridable in tests.
	waitForPath func(ctx context.Context, path string) (string, error)
	// zvolDevices returns this plugin's zvol device paths — the /dev/zvol/<pool>/
	// <parentDataset>/* symlinks AND their resolved /dev/zdN targets — so a staged
	// mount matches whichever form findmnt reports. Overridable in tests.
	zvolDevices func() map[string]struct{}
}

// StagedCount reports how many of THIS plugin's volumes are currently staged on
// this node, counted from the live mount table (metrics.StagedCounter). A staged
// filesystem volume is a mount whose source is one of this plugin's zvol devices;
// the publish bind-mount's source is the staging dir (not the zvol), so each
// volume is counted once. Block volumes are counted once published (their bind
// mount exposes the zvol device); a block volume staged-but-not-yet-published has
// no host artifact and is not counted — a narrow, inherent blind spot.
//
// Matching is by device identity, not path prefix: findmnt reports the resolved
// device (/dev/zdN), not the /dev/zvol/... symlink, so we compare against BOTH
// forms of this plugin's zvols (OQ1: scoped to our pools × parentDataset, so
// co-located local plugins / unmanaged zvols don't inflate the count).
func (n *node) StagedCount(ctx context.Context) (int, error) {
	mounts, err := n.mounter.ListMounts(ctx)
	if err != nil {
		return 0, err
	}
	ourDevs := n.zvolDevices()
	seen := map[string]struct{}{}
	for _, m := range mounts {
		if _, ok := ourDevs[m.Source]; ok {
			seen[m.Source] = struct{}{}
			continue
		}
		if real, err := filepath.EvalSymlinks(m.Source); err == nil {
			if _, ok := ourDevs[real]; ok {
				seen[m.Source] = struct{}{}
			}
		}
	}
	return len(seen), nil
}

// osZvolDevices reads this plugin's zvol device set from /dev/zvol: for each
// configured pool it lists the <pool>/<parentDataset>/ directory (one symlink per
// zvol) and records both the symlink path and its resolved target, so StagedCount
// matches a mount whether findmnt names the symlink or the /dev/zdN device.
func (n *node) osZvolDevices() map[string]struct{} {
	out := map[string]struct{}{}
	for _, pool := range n.cfg.PoolNames() {
		dir := zfs.DevicePath(parentDatasetForPool(n.cfg, pool, n.parentDataset))
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue // pool has no zvols yet, or dir not present
		}
		for _, e := range entries {
			p := filepath.Join(dir, e.Name())
			out[p] = struct{}{}
			if real, err := filepath.EvalSymlinks(p); err == nil {
				out[real] = struct{}{}
			}
		}
	}
	return out
}

func (n *node) StageVolume(ctx context.Context, req *driver.StageRequest) (err error) {
	defer func() {
		if err == nil {
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
