package local

import (
	"context"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// sourceProp is the ZFS user property recording a clone/restore's source id, so
// an idempotent create retry can verify provenance.
const sourceProp = "nomad-csi:source"

// contentSourceID returns the create's content-source id (snapshot or volume),
// or "" for a fresh create.
func contentSourceID(a createArgs) string {
	if a.ContentSnapshotID != "" {
		return a.ContentSnapshotID
	}
	return a.ContentVolumeID
}

// recordProvenance stamps the source id onto the freshly created clone/restore.
func (c *controller) recordProvenance(ctx context.Context, dataset, sourceID string) error {
	if err := c.z.SetUserProp(ctx, dataset, sourceProp, sourceID); err != nil {
		return driver.Internal("recording provenance: %v", err)
	}
	return nil
}

// localCreate provisions a zvol on this node (fresh, restored from a snapshot,
// or cloned from another volume — all node-local), returning the volume's
// routing identity and dataset. It is idempotent by name+size.
func (c *controller) localCreate(ctx context.Context, a createArgs) (out createResult, err error) {
	op := "create"
	if a.ContentVolumeID != "" || a.ContentSnapshotID != "" {
		op = "clone"
	}
	defer c.observeZFS(op, time.Now(), &err)
	// Checkpoint 2: the target pool must be imported + ONLINE on this node.
	present, online, err := c.z.PoolStatus(ctx, a.Pool)
	if err != nil {
		return createResult{}, driver.Internal("checking pool %q: %v", a.Pool, err)
	}
	if !present || !online {
		return createResult{}, driver.FailedPrecondition("pool %q is not available on node %q", a.Pool, c.res.LocalNode())
	}

	dataset := c.datasetFor(a.Pool, a.Name)

	exists, err := c.z.Exists(ctx, dataset)
	if err != nil {
		return createResult{}, driver.Internal("checking dataset: %v", err)
	}
	if exists {
		cur, err := c.z.GetVolsize(ctx, dataset)
		if err != nil {
			return createResult{}, driver.Internal("reading volsize: %v", err)
		}
		if cur != a.SizeBytes {
			return createResult{}, driver.AlreadyExists("volume %q exists with size %d, requested %d", a.Name, cur, a.SizeBytes)
		}
		// Provenance: for a clone/restore, the existing dataset must have been
		// created from the SAME source (recorded at clone time). Otherwise a name
		// reuse would silently return a volume that is not the requested copy.
		if src := contentSourceID(a); src != "" {
			got, err := c.z.GetUserProp(ctx, dataset, sourceProp)
			if err != nil {
				return createResult{}, driver.Internal("reading provenance: %v", err)
			}
			if got != src {
				return createResult{}, driver.AlreadyExists("volume %q already exists from a different source (have %q, requested %q)", a.Name, got, src)
			}
		}
		return c.result(a, dataset), nil
	}

	switch {
	case a.ContentSnapshotID != "":
		if err := c.restoreFromSnapshot(ctx, a, dataset); err != nil {
			return createResult{}, err
		}
	case a.ContentVolumeID != "":
		if err := c.cloneFromVolume(ctx, a, dataset); err != nil {
			return createResult{}, err
		}
	default:
		// Capacity guard: refuse a fresh create that would push the pool below
		// its reserve floor.
		if err := c.checkCapacity(ctx, a.Pool, a.SizeBytes); err != nil {
			c.recordCapacityReject()
			return createResult{}, err
		}
		if err := c.z.CreateZvol(ctx, dataset, a.SizeBytes, a.Volblocksize); err != nil {
			return createResult{}, driver.Internal("zfs create: %v", err)
		}
	}
	return c.result(a, dataset), nil
}

// checkCapacity refuses a create of size bytes when the pool's available space
// (free minus its reserve) can't hold it.
func (c *controller) checkCapacity(ctx context.Context, pool string, size int64) error {
	free, err := c.z.PoolFree(ctx, pool)
	if err != nil {
		return driver.Internal("zpool free: %v", err)
	}
	total, err := c.z.PoolSize(ctx, pool)
	if err != nil {
		return driver.Internal("zpool size: %v", err)
	}
	avail := free - reserveBytes(total, c.cfg.ReserveFor(pool))
	if avail < size {
		return driver.OutOfRange("pool %q has %d bytes available after reserve, need %d", pool, max64(avail, 0), size)
	}
	return nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (c *controller) restoreFromSnapshot(ctx context.Context, a createArgs, dataset string) error {
	sid, err := parseSnapshotID(a.ContentSnapshotID)
	if err != nil {
		return err
	}
	if sid.Node != c.res.LocalNode() {
		return driver.FailedPrecondition("snapshot %q is on node %q, not %q (local restore only)", a.ContentSnapshotID, sid.Node, c.res.LocalNode())
	}
	// Capacity guard: a restore is a full independent copy, so it must fit within
	// the pool's reserve floor just like a fresh create.
	if err := c.checkCapacity(ctx, a.Pool, a.SizeBytes); err != nil {
		c.recordCapacityReject()
		return err
	}
	srcDataset := sid.Dataset
	if err := c.z.CloneIndependent(ctx, srcDataset, sid.SnapName, dataset); err != nil {
		return driver.Internal("clone from snapshot: %v", err)
	}
	if err := c.recordProvenance(ctx, dataset, a.ContentSnapshotID); err != nil {
		return err
	}
	return c.growIfNeeded(ctx, dataset, a.SizeBytes)
}

func (c *controller) cloneFromVolume(ctx context.Context, a createArgs, dataset string) error {
	srcEID, err := parseExternalID(a.ContentVolumeID)
	if err != nil {
		return err
	}
	if srcEID.Node != c.res.LocalNode() {
		return driver.FailedPrecondition("source volume %q is on node %q, not %q (local clone only)", a.ContentVolumeID, srcEID.Node, c.res.LocalNode())
	}
	// Capacity guard: an independent clone is a full copy, so it must fit within
	// the pool's reserve floor just like a fresh create.
	if err := c.checkCapacity(ctx, a.Pool, a.SizeBytes); err != nil {
		c.recordCapacityReject()
		return err
	}
	srcDataset := srcEID.Dataset
	// Independent clone = transient snapshot + send|recv, then drop the snapshot.
	snapName := "csiclone-" + a.Name
	if err := c.z.Snapshot(ctx, srcDataset, snapName); err != nil {
		return driver.Internal("transient snapshot: %v", err)
	}
	if err := c.z.CloneIndependent(ctx, srcDataset, snapName, dataset); err != nil {
		_ = c.z.DestroySnapshot(ctx, srcDataset, snapName)
		return driver.Internal("clone send|recv: %v", err)
	}
	if err := c.z.DestroySnapshot(ctx, srcDataset, snapName); err != nil {
		c.log.Warn("dropping transient clone snapshot", zap.String("snapshot", srcDataset+"@"+snapName), zap.Error(err))
	}
	if err := c.recordProvenance(ctx, dataset, a.ContentVolumeID); err != nil {
		return err
	}
	return c.growIfNeeded(ctx, dataset, a.SizeBytes)
}

func (c *controller) growIfNeeded(ctx context.Context, dataset string, want int64) error {
	cur, err := c.z.GetVolsize(ctx, dataset)
	if err != nil {
		return driver.Internal("reading cloned volsize: %v", err)
	}
	if cur >= want {
		return nil
	}
	if err := c.z.SetVolsize(ctx, dataset, want); err != nil {
		return driver.Internal("growing cloned zvol: %v", err)
	}
	return nil
}

func (c *controller) localDelete(ctx context.Context, volumeID string) (err error) {
	defer c.observeZFS("destroy", time.Now(), &err)
	eid, err := parseExternalID(volumeID)
	if err != nil {
		return err
	}
	if eid.Node != c.res.LocalNode() {
		return driver.Internal("delete for node %q routed to %q", eid.Node, c.res.LocalNode())
	}
	dataset := eid.Dataset
	exists, err := c.z.Exists(ctx, dataset)
	if err != nil {
		return driver.Internal("checking dataset: %v", err)
	}
	if !exists {
		return nil // idempotent
	}
	// A ZFS snapshot is a dependent child: leaf-only destroy refuses while one
	// exists (and we never destroy recursively). Surface that as a clean
	// FailedPrecondition instead of a raw zfs error — delete the snapshots first.
	snaps, err := c.z.ListSnapshots(ctx, dataset)
	if err != nil {
		return driver.Internal("checking snapshots: %v", err)
	}
	if len(snaps) > 0 {
		return driver.FailedPrecondition("volume %s still has %d snapshot(s); delete them first", volumeID, len(snaps))
	}
	if err := c.z.DestroyZvol(ctx, dataset); err != nil {
		return driver.Internal("zfs destroy: %v", err)
	}
	return nil
}

// localExists reports whether the volume's dataset exists on this node. Used by
// VolumeExists (ValidateVolumeCapabilities) and routed here when this node owns
// the volume.
func (c *controller) localExists(ctx context.Context, volumeID string) (existsResult, error) {
	eid, err := parseExternalID(volumeID)
	if err != nil {
		return existsResult{}, err
	}
	ok, err := c.z.Exists(ctx, eid.Dataset)
	if err != nil {
		return existsResult{}, driver.Internal("checking dataset: %v", err)
	}
	return existsResult{Exists: ok}, nil
}

// localListSnapshots lists snapshots on this node. datasetFilter scopes to a
// single volume's dataset ("" = every managed pool's snapshots).
func (c *controller) localListSnapshots(ctx context.Context, datasetFilter string) (out snapListResult, err error) {
	defer c.observeZFS("list_snapshots", time.Now(), &err)
	var targets []string
	if datasetFilter != "" {
		targets = []string{datasetFilter}
	} else {
		for _, p := range c.cfg.Pools {
			targets = append(targets, c.parentDatasetFor(p.Name))
		}
	}
	node := c.res.LocalNode()
	var res snapListResult
	for _, target := range targets {
		entries, err := c.z.ListSnapshots(ctx, target)
		if err != nil {
			return snapListResult{}, driver.Internal("listing snapshots in %q: %v", target, err)
		}
		for _, e := range entries {
			ds, snap, ok := strings.Cut(e.Name, "@")
			if !ok {
				continue
			}
			res.Snapshots = append(res.Snapshots, snapWire{
				SnapshotID:     snapshotID{Node: node, Dataset: ds, SnapName: snap}.String(),
				SourceVolumeID: externalID{Node: node, Dataset: ds}.String(),
				SizeBytes:      e.VolsizeBytes,
				CreationUnix:   e.CreationUnix,
				ReadyToUse:     true,
			})
		}
	}
	return res, nil
}

func (c *controller) localExpand(ctx context.Context, volumeID string, requiredBytes, limitBytes int64) (out expandResult, err error) {
	defer c.observeZFS("expand", time.Now(), &err)
	eid, err := parseExternalID(volumeID)
	if err != nil {
		return expandResult{}, err
	}
	dataset := eid.Dataset

	// Round up to the volume's actual block size (read here, on the owning node),
	// not a controller-side default — otherwise the target could disagree with
	// ZFS's own rounding and a later idempotent re-expand would look like a shrink.
	block, err := c.z.GetVolblocksize(ctx, dataset)
	if err != nil {
		return expandResult{}, driver.Internal("reading volblocksize: %v", err)
	}
	if block <= 0 {
		return expandResult{}, driver.Internal("invalid volblocksize %d for %s", block, dataset)
	}
	size := ((requiredBytes + block - 1) / block) * block
	if limitBytes > 0 && size > limitBytes {
		return expandResult{}, driver.OutOfRange("required %d rounds up to %d which exceeds limit %d", requiredBytes, size, limitBytes)
	}

	cur, err := c.z.GetVolsize(ctx, dataset)
	if err != nil {
		return expandResult{}, driver.Internal("reading volsize: %v", err)
	}
	if size < cur {
		return expandResult{}, driver.OutOfRange("cannot shrink volume from %d to %d", cur, size)
	}
	if size == cur {
		return expandResult{CapacityBytes: cur}, nil
	}
	if err := c.z.SetVolsize(ctx, dataset, size); err != nil {
		return expandResult{}, driver.Internal("zfs set volsize: %v", err)
	}
	// Report the authoritative post-set size (ZFS may round volsize itself).
	newSize, err := c.z.GetVolsize(ctx, dataset)
	if err != nil {
		return expandResult{}, driver.Internal("reading volsize after grow: %v", err)
	}
	return expandResult{CapacityBytes: newSize}, nil
}

func (c *controller) localSnapshot(ctx context.Context, a snapshotArgs) (out snapshotResult, err error) {
	defer c.observeZFS("snapshot", time.Now(), &err)
	srcEID, err := parseExternalID(a.SourceVolumeID)
	if err != nil {
		return snapshotResult{}, err
	}
	dataset := srcEID.Dataset
	snapName := sanitizeName(a.Name)
	if err := c.z.Snapshot(ctx, dataset, snapName); err != nil {
		return snapshotResult{}, driver.Internal("zfs snapshot: %v", err)
	}
	size, err := c.z.GetVolsize(ctx, dataset)
	if err != nil {
		return snapshotResult{}, driver.Internal("reading volsize: %v", err)
	}
	created, err := c.z.GetCreation(ctx, dataset+"@"+snapName)
	if err != nil {
		return snapshotResult{}, driver.Internal("reading snapshot creation time: %v", err)
	}
	return snapshotResult{
		SnapshotID:   snapshotID{Node: c.res.LocalNode(), Dataset: dataset, SnapName: snapName}.String(),
		SizeBytes:    size,
		CreationUnix: created.Unix(),
		ReadyToUse:   true,
	}, nil
}

func (c *controller) localDeleteSnapshot(ctx context.Context, id string) (err error) {
	defer c.observeZFS("snapshot_delete", time.Now(), &err)
	sid, err := parseSnapshotID(id)
	if err != nil {
		return err
	}
	dataset := sid.Dataset
	snap := dataset + "@" + sid.SnapName
	exists, err := c.z.Exists(ctx, snap)
	if err != nil {
		return driver.Internal("checking snapshot: %v", err)
	}
	if !exists {
		return nil // idempotent
	}
	if err := c.z.DestroySnapshot(ctx, dataset, sid.SnapName); err != nil {
		return driver.Internal("zfs destroy snapshot: %v", err)
	}
	return nil
}

// localStats reports this node's stats for a single pool: whether it has the
// pool ONLINE, its volume count in that pool, and free/available (after reserve)
// bytes. HasPool=false (with a nil error) means the pool isn't usable here.
func (c *controller) localStats(ctx context.Context, pool string) (statsResult, error) {
	st := statsResult{Node: c.res.LocalNode()}
	present, online, err := c.z.PoolStatus(ctx, pool)
	if err != nil {
		return statsResult{}, driver.Internal("checking pool %q: %v", pool, err)
	}
	if !present || !online {
		return st, nil // HasPool stays false
	}
	vols, err := c.z.ListZvols(ctx, c.parentDatasetFor(pool))
	if err != nil {
		return statsResult{}, driver.Internal("listing zvols: %v", err)
	}
	free, err := c.z.PoolFree(ctx, pool)
	if err != nil {
		return statsResult{}, driver.Internal("zpool free: %v", err)
	}
	total, err := c.z.PoolSize(ctx, pool)
	if err != nil {
		return statsResult{}, driver.Internal("zpool size: %v", err)
	}
	st.HasPool = true
	st.VolumeCount = len(vols)
	st.FreeBytes = free
	st.AvailBytes = max64(free-reserveBytes(total, c.cfg.ReserveFor(pool)), 0)
	return st, nil
}

// localList returns this node's zvols across all configured pools as forwardable
// volume entries (the dataset's first segment is its pool).
func (c *controller) localList(ctx context.Context) (out listResult, err error) {
	defer c.observeZFS("list", time.Now(), &err)
	var res listResult
	for _, p := range c.cfg.Pools {
		parent := c.parentDatasetFor(p.Name)
		vols, err := c.z.ListZvols(ctx, parent)
		if err != nil {
			return listResult{}, driver.Internal("listing zvols in %q: %v", p.Name, err)
		}
		prefix := parent + "/"
		for _, v := range vols {
			if !strings.HasPrefix(v.Name, prefix) {
				continue // not directly under this pool's parent dataset
			}
			res.Volumes = append(res.Volumes, volWire{
				VolumeID:      externalID{Node: c.res.LocalNode(), Dataset: v.Name}.String(),
				CapacityBytes: v.SizeByte,
			})
		}
	}
	return res, nil
}

func (c *controller) result(a createArgs, dataset string) createResult {
	return createResult{
		VolumeID:      externalID{Node: c.res.LocalNode(), Dataset: dataset}.String(),
		Dataset:       dataset,
		CapacityBytes: a.SizeBytes,
		FsType:        a.FsType,
	}
}
