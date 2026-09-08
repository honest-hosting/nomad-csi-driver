package local

import (
	"context"
	"os"
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
	cfg           *config.LocalConfig
	z             *zfs.ZFS
	nodeID        string
	parentDataset string // the "<pool>/<parentDataset>/<vol>" middle segment; scopes StagedCount
	mounter       *mountutil.Mounter
	log           *zap.Logger
	stats         *stats.Registry      // per-volume usage stats; nil-safe no-op when disabled
	nodeMetrics   *metrics.NodeMetrics // shared node collectors; nil-safe (records context rebuilds)

	// waitForPath polls until the device path exists; overridable in tests.
	waitForPath func(ctx context.Context, path string) (string, error)
	// zvolDatasets maps this plugin's zvol device paths — the /dev/zvol/<pool>/
	// <parentDataset>/* symlinks AND their resolved /dev/zdN targets — to the full
	// zvol dataset (<pool>/<parent>/<vol>). A staged mount matches whichever form
	// findmnt reports (StagedCount uses the key set); the dataset value lets the
	// stats reconciler reconstruct the CSI volume id for rehydration. Overridable
	// in tests.
	zvolDatasets func() map[string]string
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
	ourDevs := n.zvolDatasets()
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

// stagedVolumes lists this plugin's currently-staged filesystem volumes from the
// live mount table, as stats.TrackSpecs, so the stats registry can be rehydrated
// after a plugin restart without waiting for a re-stage (which Nomad never issues
// — see NOMAD-CSI-DRIVER-METRICS-RESTART-CONSISTENCY.PLAN.md §4.2). It mirrors
// StagedCount's device-identity matching, but for each match it reconstructs the
// CSI volume id (the registry key) and captures the staging path:
//
//   - VolumeID   = externalID{Node: this node, Dataset: <matched zvol dataset>}.
//     The node is safe to assume as ours: the stage-time wrong-node guard refuses
//     a stage on a non-owner, so any locally-staged zvol is owned here.
//   - StagingPath = the mount target (where NodeStageVolume mounted the zvol).
//   - AccessType  = "mount" — a filesystem staging mount is the only host artifact
//     the mount table exposes. Local block is presence-only and staged block has
//     no mount; a *published* block volume's bind-mount also has a zvol source, but
//     block is not used in production and its rehydration is out of scope here
//     (the same local-block blind spot the staged gauge documents).
//
// Deduped by volume id (a zvol is staged once). A ListMounts error is surfaced so
// the caller stays add-only (never evicts on a transient enumeration failure).
func (n *node) stagedVolumes(ctx context.Context) ([]stats.TrackSpec, error) {
	mounts, err := n.mounter.ListMounts(ctx)
	if err != nil {
		return nil, err
	}
	devDatasets := n.zvolDatasets()
	seen := map[string]struct{}{}
	var out []stats.TrackSpec
	for _, m := range mounts {
		ds, ok := devDatasets[m.Source]
		if !ok {
			if real, err := filepath.EvalSymlinks(m.Source); err == nil {
				ds, ok = devDatasets[real]
			}
		}
		if !ok {
			continue
		}
		id := externalID{Node: n.nodeID, Dataset: ds}.String()
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, stats.TrackSpec{VolumeID: id, StagingPath: m.Target, AccessType: stats.AccessMount})
	}
	return out, nil
}

// osZvolDatasets reads this plugin's zvol device→dataset map from /dev/zvol: for
// each configured pool it lists the <pool>/<parentDataset>/ directory (one symlink
// per zvol) and records BOTH the symlink path and its resolved target, each mapped
// to the full dataset (<pool>/<parent>/<vol>). StagedCount uses the key set (match
// a mount whether findmnt names the symlink or the /dev/zdN device); the stats
// reconciler uses the dataset value to reconstruct the CSI volume id.
func (n *node) osZvolDatasets() map[string]string {
	out := map[string]string{}
	for _, pool := range n.cfg.PoolNames() {
		parent := parentDatasetForPool(n.cfg, pool, n.parentDataset) // "<pool>/<parent>"
		dir := zfs.DevicePath(parent)
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue // pool has no zvols yet, or dir not present
		}
		for _, e := range entries {
			dataset := parent + "/" + e.Name()
			p := filepath.Join(dir, e.Name())
			out[p] = dataset
			if real, err := filepath.EvalSymlinks(p); err == nil {
				out[real] = dataset
			}
		}
	}
	return out
}

// resolveVolumeIdentity returns the volume's dataset and owning node.
//
// Both normally arrive in the volume context that CreateVolume returned. Nomad
// can hand it back EMPTY: `nomad volume create` against a volume that already
// exists never calls the plugin at all (the server merges the submitted spec
// instead), and before Nomad 1.9.6 that merge assigned the spec's context
// unconditionally — a create spec carries no context block, so the stored
// context was replaced with nothing. See hashicorp/nomad#24922 and
// specs/nomad-csi-driver/NOMAD-CSI-DRIVER-MISSING-CTX-FIX.PLAN.md.
//
// The volume's data is untouched in that state, so refusing to mount would be an
// outage over bookkeeping. The external id encodes both values, Nomad refuses to
// let it change ("volume external ID cannot be updated"), and it is handed to
// every node RPC — so rebuild from it. This recomputes what the context held
// rather than guessing, and matches how DeleteVolume already resolves a volume.
func (n *node) resolveVolumeIdentity(volumeID string, vctx map[string]string) (string, string, error) {
	dataset, owner := vctx[ctxKeyDataset], vctx[ctxKeyNode]
	if dataset != "" && owner != "" {
		return dataset, owner, nil
	}

	eid, err := parseExternalID(volumeID)
	if err != nil {
		return "", "", driver.InvalidArgument(
			"volume context is missing %q and volume id %q cannot be parsed to recover it: %v",
			ctxKeyDataset, volumeID, err)
	}

	rebuilt := make([]string, 0, 2)
	if dataset == "" {
		dataset = eid.Dataset
		rebuilt = append(rebuilt, ctxKeyDataset)
	}
	if owner == "" {
		owner = eid.Node
		rebuilt = append(rebuilt, ctxKeyNode)
	}
	for _, field := range rebuilt {
		n.nodeMetrics.VolumeContextReconstructed(field)
	}
	n.log.Warn("volume context incomplete; rebuilt from external id",
		zap.String("volume_id", volumeID),
		zap.Strings("rebuilt_fields", rebuilt),
		zap.String("dataset", dataset),
		zap.String("owner_node", owner),
		zap.String("likely_cause", "`nomad volume create` run against an existing volume; Nomad < 1.9.6 erases volume context on update (hashicorp/nomad#24922)"))
	return dataset, owner, nil
}

func (n *node) StageVolume(ctx context.Context, req *driver.StageRequest) (err error) {
	defer func() {
		if err == nil {
			n.stats.Track(req.VolumeID, req.StagingTargetPath, stageAccessType(req.VolumeCapability.AccessType))
		}
	}()
	dataset, owner, err := n.resolveVolumeIdentity(req.VolumeID, req.VolumeContext)
	if err != nil {
		return err
	}
	// Wrong-node guard (data safety): a stage that lands on a non-owner node must
	// refuse rather than risk materializing a second, empty zvol. Resolving the
	// owner FIRST also closes a latent hole: this used to read the owner straight
	// from the volume context and skip the check entirely when that key was empty,
	// which is exactly the state a context wipe leaves behind.
	if owner != n.nodeID {
		return driver.FailedPrecondition("volume %s is owned by node %q but staged on %q", req.VolumeID, owner, n.nodeID)
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
	if fsType == "" {
		// Third fallback, for the same context-wipe case as resolveVolumeIdentity.
		// Nomad only populates the capability's fs_type from the volume spec's
		// mount_options.fs_type, which most specs omit — so once the context is
		// erased there is no declared filesystem left anywhere. Both callers below
		// need one: FormatIfEmpty would see the existing ext4, compare it against
		// "" and REFUSE ("already carries filesystem ext4, refusing to format as
		// ..."), and Mount would shell out to `mount -t "" ...`.
		//
		// The zvol is already formatted, so ask the device instead of guessing. That
		// is authoritative rather than inferred, and it cannot mis-format: this only
		// ever reports a filesystem that is already there.
		if detected, derr := n.mounter.DetectFilesystem(ctx, dev); derr == nil && detected != "" {
			fsType = detected
			n.nodeMetrics.VolumeContextReconstructed(ctxKeyFsType)
			n.log.Warn("volume context has no fsType and the capability declares none; probed the device",
				zap.String("volume_id", req.VolumeID),
				zap.String("device", dev),
				zap.String("detected_fstype", fsType))
		}
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
		// Same rebuild as StageVolume: this path read the dataset straight from the
		// context with no empty check at all, so a wiped context produced a bare
		// device-not-found from waitForPath instead of a diagnosable error.
		dataset, _, err := n.resolveVolumeIdentity(req.VolumeID, req.VolumeContext)
		if err != nil {
			return err
		}
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
