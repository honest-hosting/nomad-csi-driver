package qnap

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/iscsi"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
	"github.com/honest-hosting/nomad-csi-driver/internal/multipath"
	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

// node implements driver.NodeBackend for the qnap backend: iSCSI login,
// multipath assembly, then the shared format/mount layer.
type node struct {
	cfg          *config.QNAPConfig
	nodeID       string
	iscsi        *iscsi.Connector
	mpath        *multipath.Manager
	mounter      *mountutil.Mounter
	meta         metaStore
	useMultipath bool
	log          *zap.Logger
	metrics      *qnapNodeMetrics     // qnap-specific (login/stage/rescan/device); nil-safe
	nodeM        *metrics.NodeMetrics // shared node metrics (staged count); nil-safe
	stats        *stats.Registry      // per-volume usage stats; nil-safe no-op when disabled

	// waitForPath polls until path exists and returns its resolved real path.
	// Overridable in tests; defaults to an os-backed poller.
	waitForPath func(ctx context.Context, path string) (string, error)
}

func (n *node) StageVolume(ctx context.Context, req *driver.StageRequest) (err error) {
	defer func() {
		if err == nil {
			n.nodeM.StagedInc()
			n.stats.Track(req.VolumeID, req.StagingTargetPath, stageAccessType(req.VolumeCapability.AccessType))
		}
	}()
	dev, meta, err := n.attach(ctx, req.VolumeContext)
	if err != nil {
		return err
	}
	if err := n.meta.Save(req.VolumeID, meta); err != nil {
		return driver.Internal("saving stage metadata: %v", err)
	}

	// Block volumes are not formatted/mounted at stage; NodePublishVolume
	// bind-mounts the device node at the target.
	if req.VolumeCapability.AccessType == driver.AccessTypeBlock {
		return nil
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
	if err := n.mounter.Unmount(ctx, req.StagingTargetPath); err != nil {
		return driver.Internal("unmount staging: %v", err)
	}
	n.stats.Untrack(req.VolumeID)
	meta, err := n.meta.Load(req.VolumeID)
	if err != nil {
		// No metadata: the volume was never staged here (or already cleaned).
		n.log.Debug("no stage metadata at unstage; nothing to detach", zap.String("volume", req.VolumeID))
		return nil
	}
	if n.useMultipath && meta.WWID != "" {
		if err := n.mpath.Flush(ctx, meta.WWID); err != nil {
			n.log.Warn("flushing multipath map (ignored)", zap.String("wwid", meta.WWID), zap.Error(err))
		}
	}
	if meta.IQN != "" {
		for _, p := range meta.portalList() {
			if err := n.iscsi.Logout(ctx, p, meta.IQN); err != nil {
				n.log.Warn("iscsi logout (ignored)", zap.String("portal", p), zap.String("iqn", meta.IQN), zap.Error(err))
			}
		}
	}
	n.nodeM.StagedDec() // metadata was present -> this node had it staged
	return n.meta.Delete(req.VolumeID)
}

func (n *node) PublishVolume(ctx context.Context, req *driver.PublishRequest) error {
	if req.VolumeCapability.AccessType == driver.AccessTypeBlock {
		dev, _, err := n.attach(ctx, req.VolumeContext)
		if err != nil {
			return err
		}
		return n.bindErr(n.mounter.BindMount(ctx, dev, req.TargetPath, false, req.Readonly))
	}
	return n.bindErr(n.mounter.BindMount(ctx, req.StagingTargetPath, req.TargetPath, true, req.Readonly))
}

func (n *node) UnpublishVolume(ctx context.Context, req *driver.UnpublishRequest) error {
	if err := n.mounter.Unmount(ctx, req.TargetPath); err != nil {
		return driver.Internal("unpublish unmount: %v", err)
	}
	return nil
}

func (n *node) ExpandVolume(ctx context.Context, req *driver.NodeExpandRequest) (int64, error) {
	meta, err := n.meta.Load(req.VolumeID)
	if err == nil && meta.IQN != "" {
		// Rescan every session so the grown LUN size is visible before resizing.
		for _, p := range meta.portalList() {
			if err := n.iscsi.Rescan(ctx, p, meta.IQN); err != nil {
				n.log.Warn("rescan before expand (ignored)", zap.String("portal", p), zap.Error(err))
				n.metrics.recordRescan("error")
				continue
			}
			n.metrics.recordRescan("ok")
		}
	}
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
	// qnap volumes are reachable from any node, so no accessible-topology
	// constraint is advertised.
	return &driver.NodeInfo{NodeID: n.nodeID}, nil
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

// loginPortals discovers + logs into each portal, returning the portals whose
// sessions came up. It errors only when none are reachable, so a single dead
// NIC/subnet degrades to fewer paths instead of failing the stage.
func (n *node) loginPortals(ctx context.Context, portals []string, iqn string) ([]string, error) {
	var active []string
	for _, p := range portals {
		if err := n.iscsi.Discover(ctx, p); err != nil {
			n.log.Warn("iscsi portal login failed (discovery); skipping",
				zap.String("portal", p), zap.String("iqn", iqn), zap.Error(err))
			n.metrics.recordLogin("fail")
			continue
		}
		if err := n.iscsi.Login(ctx, p, iqn); err != nil {
			n.log.Warn("iscsi portal login failed (login); skipping",
				zap.String("portal", p), zap.String("iqn", iqn), zap.Error(err))
			n.metrics.recordLogin("fail")
			continue
		}
		n.log.Debug("iscsi portal login ok", zap.String("portal", p), zap.String("iqn", iqn))
		n.metrics.recordLogin("ok")
		active = append(active, p)
	}
	switch {
	case len(active) == 0:
		// All paths down — the stage fails.
		return nil, driver.Unavailable("iscsi login failed: no portal reachable for %s (tried %d: %v)", iqn, len(portals), portals)
	case len(active) < len(portals):
		// Usable but degraded — surface the missing path(s) at warn so a silently
		// reduced multipath is visible even though the mount succeeds.
		n.log.Warn("iscsi multipath degraded: not all portals logged in",
			zap.String("iqn", iqn), zap.Int("up", len(active)), zap.Int("requested", len(portals)),
			zap.Strings("active", active))
	default:
		// All requested paths up — positive confirmation at info level.
		n.log.Info("iscsi sessions established",
			zap.String("iqn", iqn), zap.Int("paths", len(active)), zap.Strings("portals", active))
	}
	return active, nil
}

// stageAccessType maps the CSI access type to the stats access-type label.
func stageAccessType(at driver.AccessType) string {
	if at == driver.AccessTypeBlock {
		return stats.AccessBlock
	}
	return stats.AccessMount
}

// splitPortals parses the comma-separated portal list from the volume context.
func splitPortals(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// attach logs into the target and resolves the usable block device (multipath
// mapper device, or the raw SCSI disk if multipath is disabled), returning the
// device path and the metadata needed to detach later.
func (n *node) attach(ctx context.Context, vctx map[string]string) (devOut string, metaOut stageMeta, err error) {
	// Record exactly one stage outcome: failed (any error), degraded (fewer
	// active paths than configured portals), or ok.
	var degraded bool
	defer func() {
		switch {
		case err != nil:
			n.metrics.recordStage("failed")
		case degraded:
			n.metrics.recordStage("degraded")
		default:
			n.metrics.recordStage("ok")
		}
	}()

	portals := splitPortals(vctx[ctxKeyPortal])
	iqn := vctx[ctxKeyIQN]
	if len(portals) == 0 || iqn == "" {
		return "", stageMeta{}, driver.InvalidArgument("volume context missing portal/iqn")
	}
	lunNum, _ := strconv.Atoi(vctx[ctxKeyLUNNumber])

	// Log into every portal so the LUN is reached over one path per portal;
	// multipathd combines them into a single /dev/mapper device. Tolerate a
	// portal being down as long as at least one session comes up (degraded but
	// usable), so a single failed NIC/subnet doesn't block the mount.
	active, err := n.loginPortals(ctx, portals, iqn)
	if err != nil {
		return "", stageMeta{}, err
	}
	degraded = len(active) < len(portals)

	// Past this point iSCSI sessions exist. If any later step fails, tear them
	// down (and flush the multipath map if one was claimed) so a retried stage
	// doesn't accumulate orphaned sessions and /dev/sd* nodes — StageVolume saves
	// metadata only on success, so UnstageVolume cannot clean these up.
	var wwid string
	defer func() {
		if err != nil {
			n.cleanupAttach(ctx, active, iqn, wwid)
		}
	}()

	// Match the device by IQN + LUN, not the literal portal: udev names the
	// by-path link with the portal IP (never the hostname), so a hostname/DHCP
	// portal would never match an exact ByPath string.
	raw, err := n.waitForPath(ctx, iscsi.DeviceGlob(iqn, lunNum))
	if err != nil {
		n.metrics.recordDeviceWait("timeout")
		return "", stageMeta{}, driver.Unavailable("iscsi device did not appear: %v", err)
	}
	n.metrics.recordDeviceWait("ok")

	n.log.Debug("iscsi raw device resolved",
		zap.String("iqn", iqn), zap.Int("lun", lunNum), zap.String("device", raw))

	meta := stageMeta{Portals: active, IQN: iqn, LUNNumber: lunNum}
	dev := raw
	if n.useMultipath {
		// Use the WWID multipath actually assigned (the /dev/mapper name with
		// user_friendly_names off), NOT scsi_id's: QNAP reports a different
		// identifier on VPD page 0x83 than the page-0x80 serial multipath uses,
		// so a scsi_id-derived mapper path would never appear.
		var werr error
		wwid, werr = n.waitForMapWWID(ctx, raw)
		if werr != nil {
			n.metrics.recordDeviceWait("timeout")
			return "", stageMeta{}, driver.Unavailable("multipath device did not appear: %v", werr)
		}
		mapper := n.mpath.MapperPath(wwid)
		n.log.Debug("multipath device resolved",
			zap.String("raw", raw), zap.String("wwid", wwid), zap.String("mapper", mapper))
		mp, perr := n.waitForPath(ctx, mapper)
		if perr != nil {
			n.metrics.recordDeviceWait("timeout")
			return "", stageMeta{}, driver.Unavailable("multipath device did not appear: %v", perr)
		}
		n.metrics.recordDeviceWait("ok")
		meta.WWID = wwid
		dev = mp
	}
	return dev, meta, nil
}

// cleanupAttach best-effort tears down the iSCSI sessions (and a multipath map,
// if one was claimed) established during a failed attach. It runs on a detached
// context so cleanup still proceeds when the original stage was cancelled or
// timed out. Errors are logged, not returned — the original stage failure is
// what the caller surfaces.
func (n *node) cleanupAttach(ctx context.Context, portals []string, iqn, wwid string) {
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if n.useMultipath && wwid != "" {
		if err := n.mpath.Flush(cctx, wwid); err != nil {
			n.log.Warn("flushing multipath map after failed stage (ignored)",
				zap.String("wwid", wwid), zap.Error(err))
		}
	}
	for _, p := range portals {
		if err := n.iscsi.Logout(cctx, p, iqn); err != nil {
			n.log.Warn("iscsi logout after failed stage (ignored)",
				zap.String("portal", p), zap.String("iqn", iqn), zap.Error(err))
		}
	}
}

// waitForMapWWID polls until multipathd reports the WWID of the map the raw
// device belongs to — multipath claims a freshly-attached device asynchronously,
// so the map may not exist on the first check.
func (n *node) waitForMapWWID(ctx context.Context, rawDevice string) (string, error) {
	deadline := time.Now().Add(devAppearTimeout)
	for {
		wwid, err := n.mpath.MapWWID(ctx, rawDevice)
		if err != nil {
			return "", err
		}
		if wwid != "" {
			return wwid, nil
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

func (n *node) bindErr(err error) error {
	if err != nil {
		return driver.Internal("bind mount: %v", err)
	}
	return nil
}

// osWaitForPath polls until path resolves, returning its symlink-resolved
// target. path may be an exact path or a glob (e.g. the DeviceGlob by-path
// pattern); a glob resolves to the first matching, dereferenceable entry.
func osWaitForPath(timeout time.Duration) func(context.Context, string) (string, error) {
	return func(ctx context.Context, path string) (string, error) {
		deadline := time.Now().Add(timeout)
		for {
			if real, ok := resolvePath(path); ok {
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

// resolvePath dereferences path to its real target. If path contains a glob
// metacharacter it expands the glob and returns the first match that resolves.
func resolvePath(path string) (string, bool) {
	if strings.ContainsAny(path, "*?[") {
		matches, _ := filepath.Glob(path)
		for _, m := range matches {
			if real, err := filepath.EvalSymlinks(m); err == nil {
				return real, true
			}
		}
		return "", false
	}
	if real, err := filepath.EvalSymlinks(path); err == nil {
		return real, true
	}
	return "", false
}
