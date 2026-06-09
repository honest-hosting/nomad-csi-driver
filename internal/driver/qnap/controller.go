package qnap

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	goqnap "github.com/honest-hosting/go-qnap"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// controller implements driver.ControllerBackend against a QNAP appliance. It
// is the sole talker to the appliance (one per cluster/DC); the node half never
// touches QNAP.
type controller struct {
	cl       Client
	sm       *sessionManager
	cfg      *config.QNAPConfig
	log      *zap.Logger
	waitOpts goqnap.WaitOptions
	cache    *lunCache
}

const cacheTTL = 60 * time.Second

func newController(cl Client, sm *sessionManager, cfg *config.QNAPConfig, log *zap.Logger) *controller {
	return &controller{
		cl:       cl,
		sm:       sm,
		cfg:      cfg,
		log:      log,
		waitOpts: goqnap.WaitOptions{Interval: time.Second},
		cache:    newLUNCache(cacheTTL),
	}
}

// cacheRefresh returns a fetch closure that pulls the full LUN+target lists.
func (c *controller) cacheRefresh(ctx context.Context) refreshFunc {
	return func() ([]goqnap.LUN, []goqnap.Target, error) {
		var (
			luns    []goqnap.LUN
			targets []goqnap.Target
		)
		err := c.sm.do(ctx, func(s goqnap.Session) error {
			var e error
			if luns, e = c.cl.ListLUNs(ctx, s); e != nil {
				return e
			}
			targets, e = c.cl.ListTargets(ctx, s)
			return e
		})
		return luns, targets, err
	}
}

func (c *controller) CreateVolume(ctx context.Context, req *driver.CreateVolumeRequest) (*driver.Volume, error) {
	vp, err := resolveParams(c.cfg, req.Parameters, req.VolumeCapabilities)
	if err != nil {
		return nil, err
	}
	sizeGB, err := sizeGiBExact(req.CapacityRange)
	if err != nil {
		return nil, err
	}
	lunName := sanitizeLUNName(req.Name)

	// Idempotency: a CreateVolume retry for the same name returns the existing
	// volume if the size matches, or AlreadyExists if it differs.
	if existing, err := c.findExistingVolume(ctx, lunName, int64(sizeGB)*giB, vp); err != nil {
		return nil, err
	} else if existing != nil {
		return existing, nil
	}

	// Resolve the target: 1:N (shared, caller-supplied) or 1:1 (we create one).
	targetIdx, ownTarget, err := c.resolveTarget(ctx, lunName, vp)
	if err != nil {
		return nil, err
	}

	lunIdx, err := c.provisionLUN(ctx, req, vp, lunName, sizeGB, targetIdx)
	if err != nil {
		// Roll back a target we created so a failed create doesn't leak it.
		if ownTarget {
			c.bestEffortDeleteTarget(ctx, targetIdx)
		}
		return nil, err
	}

	return c.assembleVolume(ctx, req, vp, lunIdx, targetIdx, ownTarget, sizeGB)
}

// provisionLUN creates the LUN — fresh, from a snapshot, or as a clone — mapped
// to targetIdx, and returns its index.
func (c *controller) provisionLUN(ctx context.Context, req *driver.CreateVolumeRequest, vp volumeParams, lunName string, sizeGB, targetIdx int) (int, error) {
	var lunIdx int
	switch src := req.ContentSource; {
	case src != nil && src.SnapshotID != "":
		sid, err := parseSnapshotID(src.SnapshotID)
		if err != nil {
			return 0, err
		}
		err = c.sm.do(ctx, func(s goqnap.Session) error {
			lunIdx, err = c.cl.CreateLUNFromSnapshot(ctx, s, goqnap.CreateLUNFromSnapshotRequest{
				SnapshotID: sid.SnapshotID, Name: lunName, PoolID: vp.poolID, TargetIndex: &targetIdx,
			})
			return err
		})
		if err != nil {
			return 0, mapQNAPError("CreateLUNFromSnapshot", err)
		}
	case src != nil && src.VolumeID != "":
		srcEID, err := parseExternalID(src.VolumeID)
		if err != nil {
			return 0, err
		}
		err = c.sm.do(ctx, func(s goqnap.Session) error {
			lunIdx, err = c.cl.CloneLUN(ctx, s, goqnap.CloneLUNRequest{
				SourceLUNIndex: srcEID.LUNIndex, Name: lunName, PoolID: vp.poolID, TargetIndex: &targetIdx,
			})
			return err
		})
		if err != nil {
			return 0, mapQNAPError("CloneLUN", err)
		}
	default:
		err := c.sm.do(ctx, func(s goqnap.Session) error {
			var e error
			lunIdx, e = c.cl.CreateBlockLUN(ctx, s, goqnap.CreateBlockLUNRequest{
				Name: lunName, PoolID: vp.poolID, SizeGB: sizeGB, SectorSize: vp.sectorSize,
				Thin: vp.thin, TargetIndex: &targetIdx,
			})
			return e
		})
		if err != nil {
			return 0, mapQNAPError("CreateBlockLUN", err)
		}
		return lunIdx, nil
	}

	// Clones/restores inherit the source size; grow to the requested size if
	// larger (clone-then-resize).
	if err := c.growToRequested(ctx, lunIdx, sizeGB); err != nil {
		return 0, err
	}
	return lunIdx, nil
}

// growToRequested resizes a freshly cloned LUN up to sizeGB if it is smaller.
func (c *controller) growToRequested(ctx context.Context, lunIdx, sizeGB int) error {
	var lun goqnap.LUN
	if err := c.sm.do(ctx, func(s goqnap.Session) error {
		var e error
		lun, e = c.cl.GetLUN(ctx, s, lunIdx)
		return e
	}); err != nil {
		return mapQNAPError("GetLUN", err)
	}
	want := int64(sizeGB) * giB
	if lun.CapacityBytes >= want {
		return nil
	}
	return c.sm.do(ctx, func(s goqnap.Session) error {
		if err := c.cl.ResizeLUN(ctx, s, lunIdx, sizeGB); err != nil {
			return err
		}
		_, err := c.cl.WaitForResizeComplete(ctx, s, lunIdx, want, c.waitOpts)
		return err
	})
}

// resolveTarget returns the target to map the LUN to. A caller-supplied
// targetIndex (1:N) is used as-is; otherwise a dedicated 1:1 target is created.
func (c *controller) resolveTarget(ctx context.Context, lunName string, vp volumeParams) (int, bool, error) {
	if vp.targetIndex != nil {
		return *vp.targetIndex, false, nil
	}
	var idx int
	err := c.sm.do(ctx, func(s goqnap.Session) error {
		var e error
		idx, e = c.cl.CreateTarget(ctx, s, goqnap.CreateTargetRequest{
			Name: lunName, Alias: lunName, Interfaces: c.cfg.Interfaces,
		})
		return e
	})
	if err != nil {
		return 0, false, mapQNAPError("CreateTarget", err)
	}
	return idx, true, nil
}

// assembleVolume fetches the new LUN/target and builds the CSI Volume, encoding
// the routing identity into the volume_id and the iSCSI attach info into the
// volume context.
func (c *controller) assembleVolume(ctx context.Context, req *driver.CreateVolumeRequest, vp volumeParams, lunIdx, targetIdx int, ownTarget bool, sizeGB int) (*driver.Volume, error) {
	var (
		lun goqnap.LUN
		tgt goqnap.Target
	)
	if err := c.sm.do(ctx, func(s goqnap.Session) error {
		var e error
		if lun, e = c.cl.GetLUN(ctx, s, lunIdx); e != nil {
			return e
		}
		tgt, e = c.cl.GetTarget(ctx, s, targetIdx)
		return e
	}); err != nil {
		return nil, mapQNAPError("GetLUN/GetTarget", err)
	}

	// Keep the controller cache warm so idempotency/list don't rescan.
	c.cache.addLUN(lun, tgt)

	capBytes := lun.CapacityBytes
	if capBytes == 0 {
		capBytes = int64(sizeGB) * giB
	}
	vctx := map[string]string{
		ctxKeyPortal:    strings.Join(c.portals(), ","),
		ctxKeyIQN:       tgt.IQN,
		ctxKeyLUNNumber: "0", // 1:1 target — the LUN is mapped at iSCSI LUN 0
		ctxKeyLUNIndex:  strconv.Itoa(lunIdx),
	}
	if !vp.block {
		vctx[ctxKeyFsType] = vp.fsType
	}
	// Trace what the node will be told to log into (one path per portal).
	c.log.Debug("volume iscsi portals advertised",
		zap.String("iqn", tgt.IQN), zap.Strings("portals", c.portals()))
	return &driver.Volume{
		VolumeID:      externalID{LUNIndex: lunIdx, TargetIndex: targetIdx, OwnTarget: ownTarget, LUNName: lun.Name}.String(),
		CapacityBytes: capBytes,
		VolumeContext: vctx,
		ContentSource: req.ContentSource,
	}, nil
}

// VolumeExists reports whether volumeID still refers to our LUN. It mirrors the
// index-reuse guard used by delete/expand/snapshot: a GetLUN failure, a ghost
// row, or a name mismatch (index reused by a different volume) all read as
// "absent". An auth/session failure surfaces as an error so the caller can
// distinguish "gone" from "couldn't check".
func (c *controller) VolumeExists(ctx context.Context, volumeID string) (bool, error) {
	eid, err := parseExternalID(volumeID)
	if err != nil {
		return false, err
	}
	var lun goqnap.LUN
	if err := c.sm.do(ctx, func(s goqnap.Session) error {
		var e error
		lun, e = c.cl.GetLUN(ctx, s, eid.LUNIndex)
		return e
	}); err != nil {
		if isAuthError(err) {
			return false, driver.Unavailable("checking volume %s: %v", volumeID, err)
		}
		return false, nil // LUN index not found -> absent
	}
	if lun.Name == "" {
		return false, nil // ghost row for a removed index
	}
	if eid.LUNName != "" && lun.Name != eid.LUNName {
		return false, nil // index reused by a different volume
	}
	return true, nil
}

func (c *controller) DeleteVolume(ctx context.Context, volumeID string, _ map[string]string) error {
	eid, err := parseExternalID(volumeID)
	if err != nil {
		return err
	}
	// Evict from the cache regardless of outcome; a stale entry would only cause
	// a safe re-delete, and re-adding on a later create keeps it correct.
	defer c.cache.removeLUN(eid.LUNIndex)
	return c.sm.do(ctx, func(s goqnap.Session) error {
		// DATA SAFETY: QNAP reuses LUN/target indices, so a stale volume_id could
		// point at a DIFFERENT volume now. Only destroy a LUN whose name still
		// matches the one we recorded at create.
		lun, gerr := c.cl.GetLUN(ctx, s, eid.LUNIndex)
		switch {
		case gerr != nil || lun.Name == "":
			c.log.Debug("lun already gone; delete is a no-op", zap.Int("lun", eid.LUNIndex))
		case eid.LUNName != "" && lun.Name != eid.LUNName:
			// A DIFFERENT LUN occupies this index now (index reused). Our volume's
			// LUN is already gone; refuse to touch the one that's there.
			c.log.Warn("lun index reused by a different volume; refusing to delete it",
				zap.Int("lun", eid.LUNIndex), zap.String("expected", eid.LUNName), zap.String("found", lun.Name))
		default:
			// Refuse if the LUN still has snapshots — they're dependent on it (same
			// data-safety stance as local). Delete the snapshots first.
			snaps, serr := c.cl.ListSnapshots(ctx, s, eid.LUNIndex)
			if serr != nil {
				return mapQNAPError("ListSnapshots", serr)
			}
			if len(snaps) > 0 {
				return driver.FailedPrecondition("volume %s still has %d snapshot(s); delete them first", volumeID, len(snaps))
			}
			// Our LUN: unmap (ignore errors) then delete.
			if err := c.cl.UnmapLUN(ctx, s, eid.LUNIndex, eid.TargetIndex); err != nil {
				c.log.Debug("unmap during delete (ignored)", zap.Int("lun", eid.LUNIndex), zap.Error(err))
			}
			if err := c.cl.DeleteLUN(ctx, s, eid.LUNIndex); err != nil {
				if !c.lunGone(ctx, s, eid.LUNIndex) {
					return mapQNAPError("DeleteLUN", err)
				}
				c.log.Debug("lun already gone after delete", zap.Int("lun", eid.LUNIndex))
			} else if err := c.cl.WaitForLUNGone(ctx, s, eid.LUNIndex, c.waitOpts); err != nil {
				return mapQNAPError("WaitForLUNGone", err)
			}
		}
		// Owned 1:1 target: delete only if its alias still matches our LUN name
		// (same reuse guard — never tear down another volume's target).
		if eid.OwnTarget && c.targetIsOurs(ctx, s, eid.TargetIndex, eid.LUNName) {
			if err := c.cl.DeleteTarget(ctx, s, eid.TargetIndex); err != nil {
				c.log.Warn("deleting owned target", zap.Int("target", eid.TargetIndex), zap.Error(err))
			}
		}
		return nil
	})
}

// targetIsOurs reports whether the target at idx still belongs to this volume —
// its alias matches the LUN name we created it for. Guards against index reuse
// before deleting a 1:1 target. With no name to verify, returns false (don't
// risk deleting a reused target).
func (c *controller) targetIsOurs(ctx context.Context, s goqnap.Session, targetIdx int, name string) bool {
	if name == "" {
		return false
	}
	tgt, err := c.cl.GetTarget(ctx, s, targetIdx)
	if err != nil {
		return false
	}
	return tgt.Alias == name
}

// ensureLUNIsOurs verifies the LUN at eid.LUNIndex still has eid.LUNName, so a
// reused index can't cause a modifying op (expand, snapshot) to act on a
// different volume. Returns NotFound when the LUN is gone or the index was
// reused. A legacy id without a name skips the check.
func (c *controller) ensureLUNIsOurs(ctx context.Context, eid externalID) error {
	if eid.LUNName == "" {
		return nil
	}
	var lun goqnap.LUN
	if err := c.sm.do(ctx, func(s goqnap.Session) error {
		var e error
		lun, e = c.cl.GetLUN(ctx, s, eid.LUNIndex)
		return e
	}); err != nil {
		return driver.NotFound("volume no longer exists (LUN index %d not found)", eid.LUNIndex)
	}
	if lun.Name != eid.LUNName {
		return driver.NotFound("volume no longer exists (LUN index %d now holds %q, expected %q)", eid.LUNIndex, lun.Name, eid.LUNName)
	}
	return nil
}

func (c *controller) ExpandVolume(ctx context.Context, volumeID string, cr driver.CapacityRange, _ map[string]string) (int64, bool, error) {
	eid, err := parseExternalID(volumeID)
	if err != nil {
		return 0, false, err
	}
	sizeGB, err := sizeGiBExact(cr)
	if err != nil {
		return 0, false, err
	}
	// Safety: don't resize a LUN whose index was reused by another volume.
	if err := c.ensureLUNIsOurs(ctx, eid); err != nil {
		return 0, false, err
	}
	want := int64(sizeGB) * giB
	if err := c.sm.do(ctx, func(s goqnap.Session) error {
		if err := c.cl.ResizeLUN(ctx, s, eid.LUNIndex, sizeGB); err != nil {
			return err
		}
		_, err := c.cl.WaitForResizeComplete(ctx, s, eid.LUNIndex, want, c.waitOpts)
		return err
	}); err != nil {
		return 0, false, mapQNAPError("ResizeLUN", err)
	}
	// The node must rescan the iSCSI session and grow the filesystem.
	return want, true, nil
}

func (c *controller) CreateSnapshot(ctx context.Context, req *driver.CreateSnapshotRequest) (*driver.Snapshot, error) {
	srcEID, err := parseExternalID(req.SourceVolumeID)
	if err != nil {
		return nil, err
	}
	// Safety: never snapshot a LUN whose index was reused by a different volume
	// (would expose another volume's data in this snapshot).
	if err := c.ensureLUNIsOurs(ctx, srcEID); err != nil {
		return nil, err
	}
	var snap goqnap.Snapshot
	if err := c.sm.do(ctx, func(s goqnap.Session) error {
		var e error
		snap, e = c.cl.CreateSnapshot(ctx, s, srcEID.LUNIndex, goqnap.CreateSnapshotRequest{
			Name:      sanitizeLUNName(req.Name),
			ExpireMin: 0,    // CSI-managed: never auto-expire
			Vital:     true, // exempt from QNAP snapshot rotation
		})
		return e
	}); err != nil {
		return nil, mapQNAPError("CreateSnapshot", err)
	}
	// Authoritative creation time: QuTScloud returns <create_time> like
	// "Fri May 29 10:13:33 2026" (Go's ANSIC layout, no timezone). Zero if absent
	// or in an unexpected format — surfaced as an empty CSI creation_time rather
	// than fabricated.
	created, _ := time.Parse(time.ANSIC, snap.CreatedAt)
	return &driver.Snapshot{
		SnapshotID:     snapshotID{LUNIndex: srcEID.LUNIndex, SnapshotID: snap.ID}.String(),
		SourceVolumeID: req.SourceVolumeID,
		SizeBytes:      snap.SizeBytes,
		CreationTime:   created,
		ReadyToUse:     snap.Status == goqnap.SnapshotStatusReady,
	}, nil
}

func (c *controller) DeleteSnapshot(ctx context.Context, id string, _ map[string]string) error {
	sid, err := parseSnapshotID(id)
	if err != nil {
		return err
	}
	return c.sm.do(ctx, func(s goqnap.Session) error {
		if err := c.cl.DeleteSnapshot(ctx, s, sid.SnapshotID); err != nil {
			return mapQNAPError("DeleteSnapshot", err)
		}
		// QNAP del_snapshot is async; wait until it is actually gone so the CSI
		// contract (snapshot removed on success) holds.
		if err := c.cl.WaitForSnapshotGone(ctx, s, sid.LUNIndex, sid.SnapshotID, c.waitOpts); err != nil {
			return mapQNAPError("WaitForSnapshotGone", err)
		}
		return nil
	})
}

func (c *controller) GetCapacity(ctx context.Context, _ []driver.Topology, params map[string]string) (int64, error) {
	poolID := c.cfg.DefaultPoolID
	if v := params["pool"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return 0, driver.InvalidArgument("invalid pool %q", v)
		}
		poolID = n
	}
	if poolID <= 0 {
		return 0, driver.InvalidArgument("pool is required for GetCapacity")
	}
	var pool goqnap.Pool
	if err := c.sm.do(ctx, func(s goqnap.Session) error {
		var e error
		pool, e = c.cl.GetPool(ctx, s, poolID)
		return e
	}); err != nil {
		return 0, mapQNAPError("GetPool", err)
	}
	return pool.FreeBytes, nil
}

// ListVolumes returns the mapped block LUNs as CSI volumes, served from the
// controller cache (refreshed at most once per TTL).
func (c *controller) ListVolumes(ctx context.Context) ([]driver.VolumeInfo, error) {
	if err := c.cache.ensureFresh(c.cacheRefresh(ctx)); err != nil {
		return nil, mapQNAPError("ListVolumes", err)
	}
	luns := c.cache.all()
	out := make([]driver.VolumeInfo, 0, len(luns))
	for _, lun := range luns {
		t, ok := c.cache.targetForLUN(lun.Index)
		if !ok {
			continue // unmapped LUN — not a volume we serve
		}
		out = append(out, driver.VolumeInfo{
			VolumeID:      externalID{LUNIndex: lun.Index, TargetIndex: t.Index, OwnTarget: t.Alias == lun.Name, LUNName: lun.Name}.String(),
			CapacityBytes: lun.CapacityBytes,
		})
	}
	return out, nil
}

// ListSnapshots returns snapshots across mapped LUNs (or one LUN when filtered
// by sourceVolumeID).
func (c *controller) ListSnapshots(ctx context.Context, sourceVolumeID string) ([]driver.SnapshotInfo, error) {
	if sourceVolumeID != "" {
		eid, err := parseExternalID(sourceVolumeID)
		if err != nil {
			return nil, err
		}
		return c.snapshotsForLUN(ctx, eid.LUNIndex, sourceVolumeID)
	}
	if err := c.cache.ensureFresh(c.cacheRefresh(ctx)); err != nil {
		return nil, mapQNAPError("ListSnapshots", err)
	}
	var out []driver.SnapshotInfo
	for _, lun := range c.cache.all() {
		t, ok := c.cache.targetForLUN(lun.Index)
		if !ok {
			continue // unmapped LUN — not a volume we serve
		}
		srcVolID := externalID{LUNIndex: lun.Index, TargetIndex: t.Index, OwnTarget: t.Alias == lun.Name, LUNName: lun.Name}.String()
		infos, err := c.snapshotsForLUN(ctx, lun.Index, srcVolID)
		if err != nil {
			return nil, err
		}
		out = append(out, infos...)
	}
	return out, nil
}

func (c *controller) snapshotsForLUN(ctx context.Context, lunIdx int, srcVolID string) ([]driver.SnapshotInfo, error) {
	var snaps []goqnap.Snapshot
	if err := c.sm.do(ctx, func(s goqnap.Session) error {
		var e error
		snaps, e = c.cl.ListSnapshots(ctx, s, lunIdx)
		return e
	}); err != nil {
		return nil, mapQNAPError("ListSnapshots", err)
	}
	out := make([]driver.SnapshotInfo, 0, len(snaps))
	for _, sn := range snaps {
		created, _ := time.Parse(time.ANSIC, sn.CreatedAt)
		out = append(out, driver.SnapshotInfo{
			SnapshotID:     snapshotID{LUNIndex: lunIdx, SnapshotID: sn.ID}.String(),
			SourceVolumeID: srcVolID,
			SizeBytes:      sn.SizeBytes,
			CreationTime:   created,
			ReadyToUse:     sn.Status == goqnap.SnapshotStatusReady,
		})
	}
	return out, nil
}

// ControllerPublishVolume/Unpublish are no-ops: the LUN is mapped to its target
// at create time (map-at-create), so any node that logs into the target sees
// it. The backend does not advertise PUBLISH_UNPUBLISH, so Nomad never calls
// these; they exist only to satisfy the interface.
func (c *controller) ControllerPublishVolume(context.Context, string, string, driver.VolumeCapability, bool, map[string]string, map[string]string) (map[string]string, error) {
	return nil, nil
}
func (c *controller) ControllerUnpublishVolume(context.Context, string, string, map[string]string) error {
	return nil
}

// --- helpers ---

// portals returns the configured iSCSI portals, each normalized to host:port
// (default port 3260). The node logs into all of them for multipath.
func (c *controller) portals() []string {
	raw := c.cfg.PortalList()
	out := make([]string, 0, len(raw))
	for _, p := range raw {
		if !hasPort(p) {
			p += ":3260"
		}
		out = append(out, p)
	}
	return out
}

func hasPort(hostport string) bool {
	for i := len(hostport) - 1; i >= 0; i-- {
		switch hostport[i] {
		case ':':
			return true
		case ']':
			return false
		}
	}
	return false
}

// findExistingVolume implements CreateVolume idempotency by LUN name, served
// from the controller cache (refreshed at most once per TTL) instead of a full
// ListLUNs scan.
func (c *controller) findExistingVolume(ctx context.Context, lunName string, wantBytes int64, vp volumeParams) (*driver.Volume, error) {
	if err := c.cache.ensureFresh(c.cacheRefresh(ctx)); err != nil {
		return nil, mapQNAPError("cache refresh", err)
	}
	lun, ok := c.cache.lookupByName(lunName)
	if !ok {
		return nil, nil
	}
	// Tolerate sub-GiB differences, don't require exact bytes: QNAP may report a
	// LUN capacity that is sector-rounded or carries thin-provisioning metadata
	// overhead, so an exact-byte check would spuriously reject an idempotent
	// retry of a create for the same requested GiB. Volumes are provisioned in
	// whole GiB, so any genuine size difference is >= 1 GiB.
	if absDiff(lun.CapacityBytes, wantBytes) >= giB {
		return nil, driver.AlreadyExists("volume %q already exists with a different size (%d != %d)", lunName, lun.CapacityBytes, wantBytes)
	}
	tgt, ok := c.cache.targetForLUN(lun.Index)
	if !ok {
		return nil, driver.Internal("existing LUN %d is not mapped to any target", lun.Index)
	}
	return c.assembleVolume(ctx, &driver.CreateVolumeRequest{}, vp, lun.Index, tgt.Index, tgt.Alias == lunName, int(wantBytes/giB))
}

// lunGone reports whether the LUN index no longer holds a real LUN — used to
// make DeleteVolume idempotent. A GetLUN error means not found; QuTScloud also
// returns a "ghost" row for a removed index (empty name, 0 capacity, status -2),
// which we likewise treat as gone (the driver always names the LUNs it creates).
func (c *controller) lunGone(ctx context.Context, s goqnap.Session, lunIdx int) bool {
	lun, err := c.cl.GetLUN(ctx, s, lunIdx)
	if err != nil {
		return true
	}
	return lun.Name == "" || lun.CapacityBytes == 0
}

func (c *controller) bestEffortDeleteTarget(ctx context.Context, targetIdx int) {
	if err := c.sm.do(ctx, func(s goqnap.Session) error {
		return c.cl.DeleteTarget(ctx, s, targetIdx)
	}); err != nil {
		c.log.Warn("rolling back target after failed create", zap.Int("target", targetIdx), zap.Error(err))
	}
}

// mapQNAPError maps go-qnap typed errors to neutral driver errors/codes.
func mapQNAPError(op string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case isAuthError(err):
		return driver.Unavailable("%s: qnap session/auth error: %v", op, err)
	}
	var ae *goqnap.APIError
	if errors.As(err, &ae) {
		switch ae.Code {
		case -22, -17:
			return driver.AlreadyExists("%s: %v", op, err)
		case -34:
			return driver.FailedPrecondition("%s: resource busy: %v", op, err)
		}
	}
	return driver.Internal("%s: %v", op, err)
}
