package local

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/honest-hosting/nomad-csi-driver/internal/cluster"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

// Forward operation names (the method segment of the forward URL).
const (
	mCreate         = "create"
	mDelete         = "delete"
	mExpand         = "expand"
	mSnapshot       = "snapshot"
	mDeleteSnapshot = "deletesnapshot"
	mStats          = "stats"
	mList           = "list"
	mListSnapshots  = "listsnapshots"
	mExists         = "exists"
	mVolStats       = stats.MethodVolStats     // per-volume usage for one volume (by id)
	mVolStatsDump   = stats.MethodVolStatsDump // all per-volume usage readings on this node
)

// Wire request/response types for forwarded operations. These cross node
// boundaries as JSON, so they hold only plain data.

type createArgs struct {
	Name              string `json:"name"`
	Pool              string `json:"pool"`
	SizeBytes         int64  `json:"size_bytes"`
	Volblocksize      string `json:"volblocksize"`
	FsType            string `json:"fs_type"`
	Block             bool   `json:"block"`
	ContentSnapshotID string `json:"content_snapshot_id,omitempty"`
	ContentVolumeID   string `json:"content_volume_id,omitempty"`
}

type createResult struct {
	VolumeID      string `json:"volume_id"`
	Dataset       string `json:"dataset"`
	CapacityBytes int64  `json:"capacity_bytes"`
	FsType        string `json:"fs_type"`
}

type idArgs struct {
	ID string `json:"id"`
}

type expandArgs struct {
	VolumeID string `json:"volume_id"`
	// RequiredBytes is the unrounded requested size; the owning node rounds it up
	// to the volume's actual volblocksize (which only it can read). LimitBytes is
	// the optional CSI upper bound (0 = none).
	RequiredBytes int64 `json:"required_bytes"`
	LimitBytes    int64 `json:"limit_bytes"`
}

type expandResult struct {
	CapacityBytes int64 `json:"capacity_bytes"`
}

type snapshotArgs struct {
	SourceVolumeID string `json:"source_volume_id"`
	Name           string `json:"name"`
}

type snapshotResult struct {
	SnapshotID   string `json:"snapshot_id"`
	SizeBytes    int64  `json:"size_bytes"`
	CreationUnix int64  `json:"creation_unix"` // ZFS `creation` epoch; survives forwarding
	ReadyToUse   bool   `json:"ready_to_use"`
}

type statsArgs struct {
	Pool string `json:"pool"`
}

type statsResult struct {
	Node    string `json:"node"`
	HasPool bool   `json:"has_pool"` // false if the pool isn't imported/ONLINE here
	// VolumeCount is scoped to the requested pool's parent dataset. It is the
	// auto-placement tie-break (when two nodes have equal available bytes), not
	// the primary ranking key.
	VolumeCount int   `json:"volume_count"`
	FreeBytes   int64 `json:"free_bytes"`  // physical (written) pool free; diagnostic only
	AvailBytes  int64 `json:"avail_bytes"` // provisioned-aware available minus the pool's reserve (>=0)
}

type volWire struct {
	VolumeID      string `json:"volume_id"`
	CapacityBytes int64  `json:"capacity_bytes"`
}

type listSnapshotsArgs struct {
	Dataset string `json:"dataset,omitempty"` // "" = all snapshots on the node
}

type snapWire struct {
	SnapshotID     string `json:"snapshot_id"`
	SourceVolumeID string `json:"source_volume_id"`
	SizeBytes      int64  `json:"size_bytes"`
	CreationUnix   int64  `json:"creation_unix"`
	ReadyToUse     bool   `json:"ready_to_use"`
}

type snapListResult struct {
	Snapshots []snapWire `json:"snapshots"`
}

type listResult struct {
	Volumes []volWire `json:"volumes"`
}

type existsResult struct {
	Exists bool `json:"exists"`
}

// dispatchForward decodes a forwarded request, runs the matching local
// operation, and encodes the result. It is the Handler the forward server
// calls. Driver errors are converted to cluster.CodedError so the caller can
// reconstruct the original code.
func (c *controller) dispatchForward(ctx context.Context, method string, body []byte) ([]byte, error) {
	switch method {
	case mCreate:
		var a createArgs
		if err := json.Unmarshal(body, &a); err != nil {
			return nil, codedFromErr(driver.InvalidArgument("decoding create args: %v", err))
		}
		res, err := c.localCreate(ctx, a)
		return encodeOrCoded(res, err)
	case mDelete:
		var a idArgs
		if err := json.Unmarshal(body, &a); err != nil {
			return nil, codedFromErr(driver.InvalidArgument("decoding delete args: %v", err))
		}
		return encodeOrCoded(struct{}{}, c.localDelete(ctx, a.ID))
	case mExpand:
		var a expandArgs
		if err := json.Unmarshal(body, &a); err != nil {
			return nil, codedFromErr(driver.InvalidArgument("decoding expand args: %v", err))
		}
		res, err := c.localExpand(ctx, a.VolumeID, a.RequiredBytes, a.LimitBytes)
		return encodeOrCoded(res, err)
	case mSnapshot:
		var a snapshotArgs
		if err := json.Unmarshal(body, &a); err != nil {
			return nil, codedFromErr(driver.InvalidArgument("decoding snapshot args: %v", err))
		}
		res, err := c.localSnapshot(ctx, a)
		return encodeOrCoded(res, err)
	case mDeleteSnapshot:
		var a idArgs
		if err := json.Unmarshal(body, &a); err != nil {
			return nil, codedFromErr(driver.InvalidArgument("decoding deletesnapshot args: %v", err))
		}
		return encodeOrCoded(struct{}{}, c.localDeleteSnapshot(ctx, a.ID))
	case mStats:
		var a statsArgs
		if err := json.Unmarshal(body, &a); err != nil {
			return nil, codedFromErr(driver.InvalidArgument("decoding stats args: %v", err))
		}
		res, err := c.localStats(ctx, a.Pool)
		return encodeOrCoded(res, err)
	case mList:
		res, err := c.localList(ctx)
		return encodeOrCoded(res, err)
	case mListSnapshots:
		var a listSnapshotsArgs
		if err := json.Unmarshal(body, &a); err != nil {
			return nil, codedFromErr(driver.InvalidArgument("decoding list-snapshots args: %v", err))
		}
		res, err := c.localListSnapshots(ctx, a.Dataset)
		return encodeOrCoded(res, err)
	case mExists:
		var a idArgs
		if err := json.Unmarshal(body, &a); err != nil {
			return nil, codedFromErr(driver.InvalidArgument("decoding exists args: %v", err))
		}
		res, err := c.localExists(ctx, a.ID)
		return encodeOrCoded(res, err)
	case mVolStats:
		var a idArgs
		if err := json.Unmarshal(body, &a); err != nil {
			return nil, codedFromErr(driver.InvalidArgument("decoding volstats args: %v", err))
		}
		cs, ok := c.statsReg.Get(a.ID)
		if !ok {
			return nil, codedFromErr(driver.NotFound("volume %s not tracked on this node", a.ID))
		}
		return encodeOrCoded(cs, nil)
	case mVolStatsDump:
		return encodeOrCoded(c.statsReg.Dump(), nil)
	default:
		return nil, codedFromErr(driver.InvalidArgument("unknown forward method %q", method))
	}
}

func encodeOrCoded(v any, err error) ([]byte, error) {
	if err != nil {
		return nil, codedFromErr(err)
	}
	b, merr := json.Marshal(v)
	if merr != nil {
		return nil, codedFromErr(driver.Internal("encoding forward result: %v", merr))
	}
	return b, nil
}

// codedFromErr converts a driver error to a cluster.CodedError carrying the
// numeric code, so the remote caller can rebuild the original driver error.
func codedFromErr(err error) error {
	var de *driver.Error
	if errors.As(err, &de) {
		return &cluster.CodedError{Code: int(de.Code), Msg: de.Error()}
	}
	return &cluster.CodedError{Code: int(driver.CodeInternal), Msg: err.Error()}
}

// remoteToDriver maps a cluster.RemoteError back to a driver.Error with the
// original code, so forwarded failures surface identically to local ones.
func remoteToDriver(err error) error {
	var re *cluster.RemoteError
	if errors.As(err, &re) {
		return &driver.Error{Code: driver.Code(re.Code), Msg: re.Msg}
	}
	return err
}
