package qnap

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// externalID is the routing identity encoded into the CSI volume_id we return
// from CreateVolume. Nomad stores it as the volume's external_id and hands it
// back on Delete/Expand/Snapshot, so it must carry everything the controller
// needs to act without a name lookup: the LUN index, the target it is mapped
// to, whether we own (and must delete) that target, and the LUN name.
//
// The LUNName is a DATA-SAFETY guard: QNAP reuses LUN/target indices, so a
// stale volume_id could point at a DIFFERENT volume's LUN. Before any
// destructive/modifying op we re-fetch the LUN and confirm its name still
// matches, so we never act on the wrong volume.
//
// Wire form: "qnap/v1/<lunIndex>/<targetIndex>/<ownTarget>/<lunName>" where
// ownTarget is "t" (1:1, target created for this LUN — delete it on
// DeleteVolume) or "s" (1:N, shared/pre-existing target — leave it). lunName is
// sanitizeLUNName output ([A-Za-z0-9_-], no slashes), so it can't break parsing.
type externalID struct {
	LUNIndex    int
	TargetIndex int
	OwnTarget   bool
	LUNName     string
}

const externalIDPrefix = "qnap/v1"

func (e externalID) String() string {
	own := "s"
	if e.OwnTarget {
		own = "t"
	}
	return fmt.Sprintf("%s/%d/%d/%s/%s", externalIDPrefix, e.LUNIndex, e.TargetIndex, own, e.LUNName)
}

// parseExternalID decodes a volume_id produced by externalID.String.
func parseExternalID(volumeID string) (externalID, error) {
	parts := strings.Split(volumeID, "/")
	if len(parts) != 6 || parts[0] != "qnap" || parts[1] != "v1" {
		return externalID{}, driver.InvalidArgument("malformed qnap volume id %q", volumeID)
	}
	lun, err := strconv.Atoi(parts[2])
	if err != nil {
		return externalID{}, driver.InvalidArgument("malformed lun index in volume id %q", volumeID)
	}
	tgt, err := strconv.Atoi(parts[3])
	if err != nil {
		return externalID{}, driver.InvalidArgument("malformed target index in volume id %q", volumeID)
	}
	return externalID{LUNIndex: lun, TargetIndex: tgt, OwnTarget: parts[4] == "t", LUNName: parts[5]}, nil
}

// snapshotID encodes the QNAP LUN index and snapshot ID into the CSI
// snapshot_id, so DeleteSnapshot can both delete and wait-for-gone (the latter
// needs the owning LUN index). Wire form: "qnaps/v1/<lunIndex>/<snapshotID>".
type snapshotID struct {
	LUNIndex   int
	SnapshotID int64
}

const snapshotIDPrefix = "qnaps/v1"

func (s snapshotID) String() string {
	return fmt.Sprintf("%s/%d/%d", snapshotIDPrefix, s.LUNIndex, s.SnapshotID)
}

func parseSnapshotID(id string) (snapshotID, error) {
	parts := strings.Split(id, "/")
	if len(parts) != 4 || parts[0] != "qnaps" || parts[1] != "v1" {
		return snapshotID{}, driver.InvalidArgument("malformed qnap snapshot id %q", id)
	}
	lun, err := strconv.Atoi(parts[2])
	if err != nil {
		return snapshotID{}, driver.InvalidArgument("malformed lun index in snapshot id %q", id)
	}
	snap, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return snapshotID{}, driver.InvalidArgument("malformed snapshot id in %q", id)
	}
	return snapshotID{LUNIndex: lun, SnapshotID: snap}, nil
}

// Volume-context keys carried from CreateVolume to NodeStageVolume. These tell
// the node how to attach the LUN over iSCSI; none are secret.
const (
	ctxKeyPortal    = "portal"   // comma-separated iSCSI portal host:port list (one path each)
	ctxKeyIQN       = "iqn"      // target IQN to log into
	ctxKeyLUNNumber = "lun"      // per-target LUN number (0 for 1:1 targets)
	ctxKeyFsType    = "fsType"   // filesystem to format/mount
	ctxKeyLUNIndex  = "lunIndex" // global QNAP LUN index (for rescans)
)
