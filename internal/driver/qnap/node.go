package qnap

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/iscsi"
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
	metrics      *qnapNodeMetrics  // qnap-specific (login/stage/rescan/device); nil-safe
	stats        *stats.Registry   // per-volume usage stats; nil-safe no-op when disabled
	san          *sanIdentityCache // read-only SAN identity resolver for cold-cache block teardown (tier 3); nil = degrade open

	// waitForPath polls until path exists and returns its resolved real path.
	// Overridable in tests; defaults to an os-backed poller.
	waitForPath func(ctx context.Context, path string) (string, error)
	// holdersFn reports whether a block device is currently held (opened/stacked),
	// read from /sys/block/<dev>/holders. Overridable in tests; nil skips the check.
	holdersFn func(device string) (bool, error)
}

// StagedCount reports how many of THIS plugin's volumes are currently staged on
// this node, counted from live iSCSI sessions (metrics.StagedCounter). A staged
// qnap volume is one target-IQN/LUN this node is logged into; login happens at
// stage, so this counts staged-or-published (the correct "staged" semantic) and
// includes block volumes (their session exists even before publish). Multipath
// yields one session per portal path, so we de-dupe on (IQN, LUN).
//
// OQ1: scope to this plugin's own portals so other qnap plugins' SANs on the
// same host are not counted. (Two plugins sharing identical portals can't be
// separated by portal alone — a documented limit.)
func (n *node) StagedCount(ctx context.Context) (int, error) {
	sessions, err := n.iscsi.ListSessions(ctx)
	if err != nil {
		return 0, err
	}
	ours := n.ourPortals()
	seen := map[string]struct{}{}
	for _, s := range sessions {
		if !portalOwned(s.Portal, ours) {
			continue // another plugin's SAN
		}
		seen[s.IQN+"/"+strconv.Itoa(s.LUN)] = struct{}{}
	}
	return len(seen), nil
}

// stagedSession is one volume this node has staged, identified purely from host
// state: the iSCSI session's target IQN + SCSI LUN number, plus the staging mount
// path (for a filesystem volume) and access type. The CSI external id is NOT known
// from the host; the caller resolves it from the SAN (sanIdentityCache).
type stagedSession struct {
	IQN         string
	LUN         int
	StagingPath string // "" for a block volume (no filesystem mount to walk)
	AccessType  string // stats.AccessMount | stats.AccessBlock
}

// stagedSessions enumerates this plugin's staged volumes from live iSCSI sessions,
// de-duped on (IQN, LUN) (multipath yields one session per portal). For each it
// finds the staging mount via the multipath-aware device→mount join: mounted →
// AccessMount + path; not mounted → AccessBlock (presence only, no walk). Host-only
// and scoped to this plugin's portals (OQ1). Used by the stats reconciler to
// rehydrate the registry after a restart; the external id is resolved separately.
func (n *node) stagedSessions(ctx context.Context) ([]stagedSession, error) {
	sessions, err := n.iscsi.ListSessions(ctx)
	if err != nil {
		return nil, err
	}
	mounts, err := n.mounter.ListMounts(ctx)
	if err != nil {
		return nil, err
	}
	ours := n.ourPortals()

	type key struct {
		iqn string
		lun int
	}
	devs := map[key][]string{}
	var order []key
	for _, s := range sessions {
		if !portalOwned(s.Portal, ours) {
			continue // another plugin's SAN
		}
		k := key{s.IQN, s.LUN}
		if _, seen := devs[k]; !seen {
			order = append(order, k)
		}
		if s.Device != "" {
			devs[k] = append(devs[k], s.Device)
		} else if _, seen := devs[k]; !seen {
			devs[k] = nil
		}
	}

	out := make([]stagedSession, 0, len(order))
	for _, k := range order {
		path, mounted := n.stagingMountFor(ctx, devs[k], mounts)
		access := stats.AccessBlock
		if mounted {
			access = stats.AccessMount
		}
		out = append(out, stagedSession{IQN: k.iqn, LUN: k.lun, StagingPath: path, AccessType: access})
	}
	return out, nil
}

// stagingMountFor finds the mount target backing a session's member devices,
// following the multipath mapper (a filesystem volume's staging mount source is the
// mapper device, not the raw sdX). findmnt reports the RESOLVED device (/dev/dm-N),
// so the mapper symlink candidate is compared symlink-resolved too. Returns
// ("", false) for a block volume (no staging mount).
func (n *node) stagingMountFor(ctx context.Context, memberDevs []string, mounts []mountutil.Mount) (string, bool) {
	for _, dev := range memberDevs {
		if dev == "" {
			continue
		}
		candidates := []string{dev}
		if n.useMultipath {
			if wwid, err := n.mpath.MapWWID(ctx, dev); err == nil && wwid != "" {
				candidates = append(candidates, n.mpath.MapperPath(wwid))
			}
		}
		for _, cand := range candidates {
			for _, m := range mounts {
				if m.Source == cand {
					return m.Target, true
				}
				if real, err := filepath.EvalSymlinks(cand); err == nil && m.Source == real {
					return m.Target, true
				}
			}
		}
	}
	return "", false
}

// portalOwned reports whether a session portal belongs to this plugin. The qnap
// NODE normally has NO statically configured portals (it reads them from each
// volume's context, not config), so ours is empty and scoping is disabled — every
// session is treated as ours. When portals ARE configured, only matching ones
// count (OQ1: separates co-located plugins targeting different SANs).
func portalOwned(portal string, ours map[string]struct{}) bool {
	if len(ours) == 0 {
		return true
	}
	_, ok := ours[normalizePortal(portal)]
	return ok
}

// cachedIQNs is the set of target IQNs staged this process lifetime (tier-1
// cache), so the reconciler does not mistake their sessions for leaks.
func (n *node) cachedIQNs() map[string]struct{} { return n.meta.iqns() }

// ourPortals is the set of this plugin's configured portals, normalized to
// "host:port", used to scope host-session enumeration to this plugin's SAN (OQ1).
func (n *node) ourPortals() map[string]struct{} {
	ours := map[string]struct{}{}
	for _, p := range n.cfg.PortalList() {
		if np := normalizePortal(p); np != "" {
			ours[np] = struct{}{}
		}
	}
	return ours
}

// normalizePortal renders an iSCSI portal as "host:port" with the default iSCSI
// port 3260 supplied when absent, so a configured portal (which may omit the
// port) compares equal to the "ip:port" form iscsiadm reports. Note: a portal
// configured as a hostname will not match iscsiadm's resolved-IP form — the
// documented residual of portal-based scoping.
func normalizePortal(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	if _, _, err := net.SplitHostPort(p); err != nil {
		return net.JoinHostPort(p, "3260")
	}
	return p
}

func (n *node) StageVolume(ctx context.Context, req *driver.StageRequest) (err error) {
	defer func() {
		if err == nil {
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

// UnstageVolume tears down a volume's iSCSI/multipath attachment without relying
// on any durable plugin state. It resolves the teardown identity from the cheapest
// available source (tier 1 in-memory cache → tier 2 host reconstruction from the
// staging mount → tier 3 SAN identity for cold block volumes), guards against
// tearing down a still-in-use device (#25813/OQ3), and only logs a shared 1:N
// target out when this is its last live LUN (OQ2).
func (n *node) UnstageVolume(ctx context.Context, req *driver.UnstageRequest) error {
	id, found := n.resolveTeardown(ctx, req)

	// In-use guard BEFORE unmount, so a premature unstage (#25813) returns
	// FAILED_PRECONDITION with the staging mount intact — the retry after unpublish
	// can then still reconstruct the identity from that mount.
	if found {
		if inUse, err := n.deviceInUse(ctx, req, id); err != nil {
			n.log.Warn("teardown in-use check failed; proceeding", zap.Error(err))
		} else if inUse {
			return driver.FailedPrecondition("volume %s still in use on node %q; refusing to detach", req.VolumeID, n.nodeID)
		}
	}

	if err := n.mounter.Unmount(ctx, req.StagingTargetPath); err != nil {
		return driver.Internal("unmount staging: %v", err)
	}
	n.stats.Untrack(req.VolumeID)

	if !found {
		// Never staged here, already cleaned, or a cold-cache block volume the SAN
		// path could not identify — nothing to detach (idempotent per CSI).
		n.log.Debug("no teardown identity at unstage; nothing to detach", zap.String("volume", req.VolumeID))
		return nil
	}

	if n.useMultipath && id.WWID != "" {
		if err := n.mpath.Flush(ctx, id.WWID); err != nil {
			n.log.Warn("flushing multipath map (ignored)", zap.String("wwid", id.WWID), zap.Error(err))
		}
	}

	// OQ2: only log the target session out when this is its last LUN on the node.
	// A shared (1:N) target still serving other LUNs must stay logged in.
	if id.IQN != "" {
		if n.lastLUNOnTarget(ctx, id.IQN, id.LUNNumber) {
			for _, p := range id.portalList() {
				if err := n.iscsi.Logout(ctx, p, id.IQN); err != nil {
					n.log.Warn("iscsi logout (ignored)", zap.String("portal", p), zap.String("iqn", id.IQN), zap.Error(err))
				}
			}
		} else {
			n.log.Info("shared target still serving other LUNs; leaving session logged in",
				zap.String("iqn", id.IQN), zap.Int("lun", id.LUNNumber))
		}
	}

	return n.meta.Delete(req.VolumeID)
}

// resolveTeardown finds the iSCSI/multipath identity needed to detach a volume,
// cheapest source first (§5.2): tier 1 in-memory cache, tier 2 host
// reconstruction from the still-present staging mount (filesystem volumes),
// tier 3 SAN identity lookup for cold-cache block volumes. found=false means the
// volume could not be identified (never staged here, already gone, or a cold
// block volume) — the caller then unmounts and returns OK without detaching.
func (n *node) resolveTeardown(ctx context.Context, req *driver.UnstageRequest) (stageMeta, bool) {
	if m, err := n.meta.Load(req.VolumeID); err == nil {
		return m, true // tier 1: warm cache
	}
	if m, ok := n.reconstructFromMount(ctx, req.StagingTargetPath); ok {
		return m, true // tier 2: fs host reconstruction
	}
	if m, ok := n.reconstructFromSAN(ctx, req.VolumeID); ok {
		return m, true // tier 3: block SAN identity
	}
	return stageMeta{}, false
}

// reconstructFromMount rebuilds a volume's teardown identity from the host when
// the staging path is still mounted (a filesystem volume, cold cache): the
// mount's source device → its multipath member paths → the iSCSI sessions those
// paths belong to → {IQN, portals, WWID}. Scoped to this plugin's portals (OQ1).
func (n *node) reconstructFromMount(ctx context.Context, stagingPath string) (stageMeta, bool) {
	dev, err := n.mounter.SourceDevice(ctx, stagingPath)
	if err != nil {
		return stageMeta{}, false // not mounted (block, or already unmounted)
	}
	sessions, err := n.iscsi.ListSessions(ctx)
	if err != nil {
		n.log.Warn("reconstruct teardown: listing sessions failed", zap.Error(err))
		return stageMeta{}, false
	}

	// Determine the member path devices backing the mount source.
	var wwid string
	members := map[string]struct{}{}
	if strings.HasPrefix(dev, "/dev/mapper/") {
		wwid = filepath.Base(dev)
		mem, merr := n.mpath.Members(ctx, wwid)
		if merr != nil {
			n.log.Warn("reconstruct teardown: multipath members lookup failed", zap.String("wwid", wwid), zap.Error(merr))
		}
		for _, d := range mem {
			members[filepath.Base(d)] = struct{}{}
		}
	} else {
		members[filepath.Base(dev)] = struct{}{}
	}

	ours := n.ourPortals()
	var iqn string
	var lun int
	portals := map[string]struct{}{}
	for _, s := range sessions {
		if _, ok := members[filepath.Base(s.Device)]; !ok {
			continue
		}
		if !portalOwned(s.Portal, ours) {
			continue // another plugin's SAN
		}
		iqn = s.IQN
		lun = s.LUN
		portals[normalizePortal(s.Portal)] = struct{}{}
	}
	if iqn == "" {
		return stageMeta{}, false // could not correlate device to a session
	}
	return stageMeta{IQN: iqn, LUNNumber: lun, WWID: wwid, Portals: sortedKeys(portals)}, true
}

// deviceInUse reports whether the volume's device is still referenced by a mount
// OTHER than the staging mount being torn down — a publish bind-mount at a target
// (fs: source is the staging dir; block: source is the mapper device) or a second
// staging mount under multi-writer sharing. Logging the session out while such a
// mount is live would break it, so the caller refuses (FAILED_PRECONDITION).
func (n *node) deviceInUse(ctx context.Context, req *driver.UnstageRequest, id stageMeta) (bool, error) {
	mounts, err := n.mounter.ListMounts(ctx)
	if err != nil {
		return false, err
	}
	var mapperDev string
	if id.WWID != "" {
		mapperDev = n.mpath.MapperPath(id.WWID)
	}
	for _, m := range mounts {
		if m.Target == req.StagingTargetPath {
			continue // the staging mount we are tearing down
		}
		if m.Source == req.StagingTargetPath { // fs publish bind-mount
			return true, nil
		}
		if mapperDev != "" && m.Source == mapperDev { // block publish / shared staging
			return true, nil
		}
	}
	// OQ3: block volumes have no staging mount to check, so also consult the
	// device's kernel holders (a raw open not visible as a mount).
	if mapperDev != "" && n.holdersFn != nil {
		if held, herr := n.holdersFn(mapperDev); herr == nil && held {
			return true, nil
		}
	}
	return false, nil
}

// deviceHasHolders reports whether a block device currently has holders — some
// other device-mapper target or open referencing it — via /sys/block/<dev>/
// holders. Best-effort: an unreadable path yields (false, nil) so a missing
// sysfs entry never spuriously blocks teardown.
func deviceHasHolders(device string) (bool, error) {
	name := filepath.Base(device)
	if resolved, err := filepath.EvalSymlinks(device); err == nil {
		name = filepath.Base(resolved) // /dev/mapper/<wwid> → dm-N
	}
	entries, err := os.ReadDir(filepath.Join("/sys/block", name, "holders"))
	if err != nil {
		return false, nil
	}
	return len(entries) > 0, nil
}

// lastLUNOnTarget reports whether lun is the only LUN of iqn currently logged in
// on this node — i.e. logging the target out will not cut off another still-
// attached LUN (the 1:N shared-target case, OQ2). On a session-list error it
// returns true (best-effort: proceed with logout rather than leak).
func (n *node) lastLUNOnTarget(ctx context.Context, iqn string, lun int) bool {
	sessions, err := n.iscsi.ListSessions(ctx)
	if err != nil {
		n.log.Warn("refcount: listing sessions failed; assuming last LUN", zap.Error(err))
		return true
	}
	luns := map[int]struct{}{}
	for _, s := range sessions {
		if s.IQN == iqn {
			luns[s.LUN] = struct{}{}
		}
	}
	if len(luns) == 0 {
		return true // session already gone; logout is a harmless no-op
	}
	_, hasOurs := luns[lun]
	return len(luns) == 1 && hasOurs
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

// reconstructFromSAN resolves a block volume's teardown identity when the cache
// is cold and there is no staging mount to anchor host reconstruction (tier 3):
// the SAN provides the target IQN (verified against the LUN name), and this
// node's live iSCSI sessions to that IQN ground the LUN number + WWID. Requires
// a read-only node SAN client; without one, or on any SAN error, it degrades
// open (the caller leaves the session for the reconciler).
func (n *node) reconstructFromSAN(ctx context.Context, volumeID string) (stageMeta, bool) {
	if n.san == nil {
		n.log.Warn("cold-cache block volume: no node SAN client; leaving session for reconciler",
			zap.String("volume", volumeID))
		return stageMeta{}, false
	}
	eid, err := parseExternalID(volumeID)
	if err != nil {
		return stageMeta{}, false
	}
	iqn, ok := n.san.resolveIQN(ctx, eid)
	if !ok {
		return stageMeta{}, false
	}
	// Ground the LUN number + WWID in this node's live sessions to that IQN.
	sessions, err := n.iscsi.ListSessions(ctx)
	if err != nil {
		n.log.Warn("reconstruct(SAN): listing sessions failed", zap.Error(err))
		return stageMeta{}, false
	}
	ours := n.ourPortals()
	portals := map[string]struct{}{}
	lun := -1
	var device string
	for _, s := range sessions {
		if s.IQN != iqn {
			continue
		}
		if !portalOwned(s.Portal, ours) {
			continue
		}
		lun = s.LUN
		device = s.Device
		portals[normalizePortal(s.Portal)] = struct{}{}
	}
	if lun < 0 {
		return stageMeta{}, false // SAN knows the target, but nothing attached on this node
	}
	wwid := ""
	if n.useMultipath && device != "" {
		wwid, _ = n.mpath.MapWWID(ctx, device)
	}
	return stageMeta{IQN: iqn, LUNNumber: lun, WWID: wwid, Portals: sortedKeys(portals)}, true
}

// sortedKeys returns the map's keys in deterministic (sorted) order, so a
// reconstructed portal list is stable across calls.
func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
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
