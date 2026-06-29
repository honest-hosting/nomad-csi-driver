package local

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/cluster"
	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
	"github.com/honest-hosting/nomad-csi-driver/internal/zfs"
)

// controller implements driver.ControllerBackend for the local backend. Because
// Nomad routes controller RPCs to the lowest-node-ID controller (topology
// blind), this controller forwards each operation to the node that owns (or
// will own) the volume, using the cluster forwarding transport. When the owner
// is the local node, it runs the ZFS op directly.
type controller struct {
	z             *zfs.ZFS
	cfg           *config.LocalConfig
	parentDataset string // default ZFS parent dataset per pool (pool config may override)
	res           cluster.Resolver
	fwd           *cluster.Client
	log           *zap.Logger
	// metrics is optional (nil in tests / node-only); observeZFS is nil-safe.
	metrics *localMetrics
	// cluster holds the shared forwarding/resolution metrics (nil-safe); reached
	// via recordForward/recordResolve and listPeers.
	cluster *metrics.ClusterMetrics
	// statsReg is the node-side per-volume usage cache (nil-safe). The controller
	// reads it for co-located volumes and forwards to the owner otherwise.
	statsReg *stats.Registry
	// mapper resolves Nomad volume id ↔ external id for the public stats API.
	mapper stats.Mapper
	// statsNS is the default Nomad namespace for stats id resolution / metrics.
	statsNS string
}

func newController(z *zfs.ZFS, cfg *config.LocalConfig, parentDataset string, res cluster.Resolver, fwd *cluster.Client, log *zap.Logger) *controller {
	return &controller{z: z, cfg: cfg, parentDataset: parentDataset, res: res, fwd: fwd, log: log}
}

func (c *controller) CreateVolume(ctx context.Context, req *driver.CreateVolumeRequest) (*driver.Volume, error) {
	vp, err := resolveParams(c.cfg, req.Parameters, req.VolumeCapabilities)
	if err != nil {
		return nil, err
	}
	size, err := roundUpToBlock(req.CapacityRange, vp.volblocksize)
	if err != nil {
		return nil, err
	}

	node, err := c.placeVolume(ctx, vp, size, req.ContentSource)
	if err != nil {
		return nil, err
	}

	args := createArgs{
		Name: sanitizeName(req.Name), Pool: vp.pool, SizeBytes: size, Volblocksize: vp.volblocksize,
		FsType: vp.fsType, Block: vp.block,
	}
	if req.ContentSource != nil {
		args.ContentSnapshotID = req.ContentSource.SnapshotID
		args.ContentVolumeID = req.ContentSource.VolumeID
	}

	var res createResult
	if node == c.res.LocalNode() {
		res, err = c.localCreate(ctx, args)
	} else {
		err = c.forward(ctx, node, mCreate, args, &res)
	}
	if err != nil {
		return nil, err
	}

	vctx := map[string]string{ctxKeyDataset: res.Dataset, ctxKeyNode: node}
	if res.FsType != "" {
		vctx[ctxKeyFsType] = res.FsType
	}
	return &driver.Volume{
		VolumeID:           res.VolumeID,
		CapacityBytes:      res.CapacityBytes,
		VolumeContext:      vctx,
		AccessibleTopology: []driver.Topology{{Segments: map[string]string{topologyKey: node}}},
		ContentSource:      req.ContentSource,
	}, nil
}

func (c *controller) DeleteVolume(ctx context.Context, volumeID string, _ map[string]string) error {
	eid, err := parseExternalID(volumeID)
	if err != nil {
		return err
	}
	if eid.Node == c.res.LocalNode() {
		return c.localDelete(ctx, volumeID)
	}
	return c.forward(ctx, eid.Node, mDelete, idArgs{ID: volumeID}, nil)
}

// VolumeExists reports whether the volume's dataset exists on its owning node,
// routing to that node when it isn't local.
func (c *controller) VolumeExists(ctx context.Context, volumeID string) (bool, error) {
	eid, err := parseExternalID(volumeID)
	if err != nil {
		return false, err
	}
	if eid.Node == c.res.LocalNode() {
		res, err := c.localExists(ctx, volumeID)
		return res.Exists, err
	}
	var res existsResult
	if err := c.forward(ctx, eid.Node, mExists, idArgs{ID: volumeID}, &res); err != nil {
		return false, err
	}
	return res.Exists, nil
}

func (c *controller) ExpandVolume(ctx context.Context, volumeID string, cr driver.CapacityRange, _ map[string]string) (int64, bool, error) {
	eid, err := parseExternalID(volumeID)
	if err != nil {
		return 0, false, err
	}
	if cr.RequiredBytes <= 0 {
		return 0, false, driver.InvalidArgument("capacity required_bytes must be > 0")
	}
	// Rounding to the volume's block size happens on the owning node, which is the
	// only place the actual volblocksize can be read; the controller forwards the
	// unrounded request.
	var res expandResult
	if eid.Node == c.res.LocalNode() {
		res, err = c.localExpand(ctx, volumeID, cr.RequiredBytes, cr.LimitBytes)
	} else {
		err = c.forward(ctx, eid.Node, mExpand, expandArgs{VolumeID: volumeID, RequiredBytes: cr.RequiredBytes, LimitBytes: cr.LimitBytes}, &res)
	}
	if err != nil {
		return 0, false, err
	}
	return res.CapacityBytes, true, nil // node must grow the filesystem
}

func (c *controller) CreateSnapshot(ctx context.Context, req *driver.CreateSnapshotRequest) (*driver.Snapshot, error) {
	eid, err := parseExternalID(req.SourceVolumeID)
	if err != nil {
		return nil, err
	}
	args := snapshotArgs{SourceVolumeID: req.SourceVolumeID, Name: req.Name}
	var res snapshotResult
	if eid.Node == c.res.LocalNode() {
		res, err = c.localSnapshot(ctx, args)
	} else {
		err = c.forward(ctx, eid.Node, mSnapshot, args, &res)
	}
	if err != nil {
		return nil, err
	}
	var created time.Time
	if res.CreationUnix > 0 {
		created = time.Unix(res.CreationUnix, 0)
	}
	return &driver.Snapshot{
		SnapshotID:     res.SnapshotID,
		SourceVolumeID: req.SourceVolumeID,
		SizeBytes:      res.SizeBytes,
		CreationTime:   created, // authoritative: ZFS `creation` property
		ReadyToUse:     res.ReadyToUse,
	}, nil
}

func (c *controller) DeleteSnapshot(ctx context.Context, id string, _ map[string]string) error {
	sid, err := parseSnapshotID(id)
	if err != nil {
		return err
	}
	if sid.Node == c.res.LocalNode() {
		return c.localDeleteSnapshot(ctx, id)
	}
	return c.forward(ctx, sid.Node, mDeleteSnapshot, idArgs{ID: id}, nil)
}

// GetCapacity reports provisioning headroom for the requested pool (controller-
// level, pre-volume) — distinct from NodeGetVolumeStats, which reports a mounted
// zvol's usage. Reports this node's provisioned-aware available bytes (after the
// pool's reserve), matching the placement/create accounting (0 if absent here).
func (c *controller) GetCapacity(ctx context.Context, _ []driver.Topology, params map[string]string) (int64, error) {
	pool := c.cfg.DefaultPool
	if v := params["pool"]; v != "" {
		pool = v
	}
	if _, ok := c.cfg.PoolByName(pool); !ok {
		return 0, driver.InvalidArgument("parameters.pool %q is not an allowed pool (configured: %v)", pool, c.cfg.PoolNames())
	}
	present, online, err := c.z.PoolStatus(ctx, pool)
	if err != nil {
		return 0, driver.Internal("zpool status: %v", err)
	}
	if !present || !online {
		return 0, nil
	}
	avail, err := c.z.PoolAvailable(ctx, pool)
	if err != nil {
		return 0, driver.Internal("zfs available: %v", err)
	}
	total, err := c.z.PoolSize(ctx, pool)
	if err != nil {
		return 0, driver.Internal("zpool size: %v", err)
	}
	return max64(avail-reserveBytes(total, c.cfg.ReserveFor(pool)), 0), nil
}

// ListVolumes aggregates volumes across all peer nodes, tagging each with its
// owning node's topology segment.
func (c *controller) ListVolumes(ctx context.Context) ([]driver.VolumeInfo, error) {
	peers, err := c.listPeers(ctx)
	if err != nil {
		return nil, driver.Unavailable("listing peers: %v", err)
	}
	var out []driver.VolumeInfo
	for _, p := range peers {
		var lr listResult
		if p.Node == c.res.LocalNode() {
			lr, err = c.localList(ctx)
		} else {
			err = c.forward(ctx, p.Node, mList, nil, &lr)
		}
		if err != nil {
			c.log.Warn("list volumes for peer failed; skipping", zap.String("node", p.Node), zap.Error(err))
			continue
		}
		for _, v := range lr.Volumes {
			out = append(out, driver.VolumeInfo{
				VolumeID:           v.VolumeID,
				CapacityBytes:      v.CapacityBytes,
				AccessibleTopology: []driver.Topology{{Segments: map[string]string{topologyKey: p.Node}}},
			})
		}
	}
	return out, nil
}

// ListSnapshots aggregates snapshots across nodes. A source-volume filter routes
// straight to that volume's owning node; otherwise we gather from every peer.
func (c *controller) ListSnapshots(ctx context.Context, sourceVolumeID string) ([]driver.SnapshotInfo, error) {
	if sourceVolumeID != "" {
		eid, err := parseExternalID(sourceVolumeID)
		if err != nil {
			return nil, err
		}
		res, err := c.listSnapshotsOn(ctx, eid.Node, eid.Dataset)
		if err != nil {
			return nil, err
		}
		return toSnapshotInfos(res), nil
	}
	peers, err := c.listPeers(ctx)
	if err != nil {
		return nil, driver.Unavailable("listing peers: %v", err)
	}
	var out []driver.SnapshotInfo
	for _, p := range peers {
		res, err := c.listSnapshotsOn(ctx, p.Node, "")
		if err != nil {
			c.log.Warn("list snapshots for peer failed; skipping", zap.String("node", p.Node), zap.Error(err))
			continue
		}
		out = append(out, toSnapshotInfos(res)...)
	}
	return out, nil
}

func (c *controller) listSnapshotsOn(ctx context.Context, node, dataset string) (snapListResult, error) {
	if node == c.res.LocalNode() {
		return c.localListSnapshots(ctx, dataset)
	}
	var res snapListResult
	err := c.forward(ctx, node, mListSnapshots, listSnapshotsArgs{Dataset: dataset}, &res)
	return res, err
}

func toSnapshotInfos(res snapListResult) []driver.SnapshotInfo {
	out := make([]driver.SnapshotInfo, 0, len(res.Snapshots))
	for _, s := range res.Snapshots {
		var created time.Time
		if s.CreationUnix > 0 {
			created = time.Unix(s.CreationUnix, 0)
		}
		out = append(out, driver.SnapshotInfo{
			SnapshotID:     s.SnapshotID,
			SourceVolumeID: s.SourceVolumeID,
			SizeBytes:      s.SizeBytes,
			CreationTime:   created,
			ReadyToUse:     s.ReadyToUse,
		})
	}
	return out
}

// ControllerPublishVolume/Unpublish are no-ops: local volumes are node-pinned
// via topology, so there is no controller-side attach step.
func (c *controller) ControllerPublishVolume(context.Context, string, string, driver.VolumeCapability, bool, map[string]string, map[string]string) (map[string]string, error) {
	return nil, nil
}
func (c *controller) ControllerUnpublishVolume(context.Context, string, string, map[string]string) error {
	return nil
}

// placeVolume resolves the owner node for a new volume. A content source forces
// the volume onto the source's node (clones/restores are node-local).
// Otherwise an explicit host pins (and must have the requested pool); "auto"
// picks the node with the most available (provisioned-aware) space in the pool
// (§5.1).
func (c *controller) placeVolume(ctx context.Context, vp volumeParams, size int64, src *driver.ContentSource) (node string, err error) {
	mode := "auto"
	defer func() { c.recordPlacement(mode, err) }()
	if src != nil {
		mode = "content"
		if src.SnapshotID != "" {
			sid, err := parseSnapshotID(src.SnapshotID)
			if err != nil {
				return "", err
			}
			return sid.Node, nil
		}
		if src.VolumeID != "" {
			eid, err := parseExternalID(src.VolumeID)
			if err != nil {
				return "", err
			}
			return eid.Node, nil
		}
	}
	if vp.host != hostAuto {
		mode = "host"
		if err := c.validateNode(ctx, vp.host); err != nil {
			return "", err
		}
		// Fail fast if the chosen node doesn't actually have the requested pool.
		st, err := c.statsFor(ctx, vp.host, vp.pool)
		if err != nil {
			return "", driver.Unavailable("checking pool %q on node %q: %v", vp.pool, vp.host, err)
		}
		if !st.HasPool {
			return "", driver.InvalidArgument("node %q does not have pool %q online", vp.host, vp.pool)
		}
		return vp.host, nil
	}
	return c.pickRoomiest(ctx, vp.pool, size)
}

func (c *controller) validateNode(ctx context.Context, node string) error {
	peers, err := c.listPeers(ctx)
	if err != nil {
		return driver.Unavailable("listing peers: %v", err)
	}
	for _, p := range peers {
		if p.Node == node {
			return nil
		}
	}
	return driver.InvalidArgument("host %q is not a known local-driver node", node)
}

// pickRoomiest implements host=auto for a given pool: among nodes that have the
// pool ONLINE with room for `size` after reserve, the one with the MOST available
// bytes (ties broken by fewest volumes, then node name for determinism).
//
// "Available" is provisioned-aware (`zfs available`, see PoolAvailable), so each
// thick zvol is charged at its full size regardless of how much it has actually
// written. This balances on real remaining capacity, not volume count: a node
// with one large, mostly-empty zvol is correctly seen as fuller than nodes whose
// pools are nearly untouched. It is storage-balancing only — compute-blind.
func (c *controller) pickRoomiest(ctx context.Context, pool string, size int64) (string, error) {
	peers, err := c.listPeers(ctx)
	if err != nil {
		return "", driver.Unavailable("listing peers: %v", err)
	}
	if len(peers) == 0 {
		return "", driver.Unavailable("no local-driver nodes available for placement")
	}
	best := ""
	var bestAvail int64 = -1
	bestCount := math.MaxInt
	for _, p := range peers {
		st, err := c.statsFor(ctx, p.Node, pool)
		if err != nil {
			c.log.Warn("stats for peer failed; skipping", zap.String("node", p.Node), zap.Error(err))
			continue
		}
		if !st.HasPool {
			continue // node doesn't have this pool online
		}
		if st.AvailBytes < size {
			continue // no room after the pool's reserve
		}
		switch {
		case st.AvailBytes > bestAvail,
			st.AvailBytes == bestAvail && st.VolumeCount < bestCount,
			st.AvailBytes == bestAvail && st.VolumeCount == bestCount && p.Node < best:
			best, bestAvail, bestCount = p.Node, st.AvailBytes, st.VolumeCount
		}
	}
	if best == "" {
		return "", driver.Unavailable("no local-driver node has pool %q online with %d bytes available", pool, size)
	}
	return best, nil
}

func (c *controller) statsFor(ctx context.Context, node, pool string) (statsResult, error) {
	if node == c.res.LocalNode() {
		return c.localStats(ctx, pool)
	}
	var st statsResult
	err := c.forward(ctx, node, mStats, statsArgs{Pool: pool}, &st)
	return st, err
}

// forward sends an operation to the owning node and decodes the response.
func (c *controller) forward(ctx context.Context, node, method string, req any, resp any) error {
	// Anti-loop hop guard: a request that already arrived via forwarding must not
	// be forwarded onward. In normal operation forwarded requests run local ops
	// and never reach here; this defends against a future re-forward bug.
	if cluster.IsForwarded(ctx) {
		return driver.Internal("refusing to forward an already-forwarded %q to node %q (possible loop)", method, node)
	}
	addr, err := c.res.Resolve(ctx, node)
	if err != nil {
		c.recordForward(method, "unreachable") // can't even discover the peer
		return driver.Unavailable("resolving node %q: %v", node, err)
	}
	var body []byte
	if req != nil {
		if body, err = json.Marshal(req); err != nil {
			c.recordForward(method, "error")
			return driver.Internal("encoding forward request: %v", err)
		}
	}
	out, err := c.fwd.Call(ctx, addr, method, body)
	if err != nil {
		c.recordForward(method, forwardOutcome(err)) // unreachable (transport) vs error (remote op)
		return remoteToDriver(err)
	}
	if resp != nil {
		if err := json.Unmarshal(out, resp); err != nil {
			c.recordForward(method, "error")
			return driver.Internal("decoding forward response: %v", err)
		}
	}
	c.recordForward(method, "ok")
	return nil
}

// forwardOutcome classifies a forward Call error: a transport/network failure is
// "unreachable" (peer down/partition), anything else (a coded error from the
// remote op) is "error".
func forwardOutcome(err error) string {
	var ne net.Error
	if errors.As(err, &ne) {
		return "unreachable"
	}
	return "error"
}

// listPeers resolves the peer set, recording the peer-roster discovery outcome.
func (c *controller) listPeers(ctx context.Context) ([]cluster.NodeInfo, error) {
	peers, err := c.res.List(ctx)
	c.recordResolve(err)
	if err == nil {
		c.cluster.SetPeers(len(peers))
	}
	return peers, err
}

// datasetFor builds the full zvol dataset path for a volume in the given pool.
func (c *controller) datasetFor(pool, volName string) string {
	return c.parentDatasetFor(pool) + "/" + volName
}

// parentDatasetFor is the dataset zvols are created under in a pool (for
// listing/counting), honoring the pool's configured parent_dataset.
func (c *controller) parentDatasetFor(pool string) string {
	return parentDatasetForPool(c.cfg, pool, c.parentDataset)
}

// reserveBytes is the free-space floor for a pool of total bytes at pct percent.
func reserveBytes(total int64, pct int) int64 {
	switch {
	case pct <= 0:
		return 0
	case pct >= 100:
		return total
	default:
		return total / 100 * int64(pct)
	}
}
