package local

import (
	"strings"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// topologyKey is the NodeGetInfo/accessible-topology segment that pins a local
// volume to the node whose pool holds its zvol.
const topologyKey = "io.honesthosting.csi.topology/node"

// externalID is the CSI volume_id for a local volume. It encodes the owning
// node (for forward routing on Delete/Expand) and the FULL zvol dataset
// (pool/parent.../volName). Encoding the whole dataset — rather than rebuilding
// it from config — makes delete/expand config-independent: a volume on a pool
// later removed from the allowlist (or whose parent default changed) is still
// destroyable. The pool is the first dataset segment, the volume name the last.
//
// Wire form: "local/v1/<node>/<dataset>" (the dataset keeps its own slashes).
type externalID struct {
	Node    string
	Dataset string
}

const externalIDPrefix = "local/v1"

func (e externalID) String() string {
	return externalIDPrefix + "/" + e.Node + "/" + e.Dataset
}

// Pool returns the zpool name (the first dataset segment).
func (e externalID) Pool() string { return poolOf(e.Dataset) }

// VolName returns the zvol's base name (the last dataset segment).
func (e externalID) VolName() string { return lastSegment(e.Dataset) }

func parseExternalID(volumeID string) (externalID, error) {
	parts := strings.SplitN(volumeID, "/", 4)
	if len(parts) != 4 || parts[0] != "local" || parts[1] != "v1" || parts[2] == "" {
		return externalID{}, driver.InvalidArgument("malformed local volume id %q", volumeID)
	}
	ds := parts[3]
	if !validDataset(ds) {
		return externalID{}, driver.InvalidArgument("malformed local volume id %q", volumeID)
	}
	return externalID{Node: parts[2], Dataset: ds}, nil
}

// snapshotID encodes the owning node, the FULL source zvol dataset, and the
// snapshot name. Wire form: "locals/v1/<node>/<dataset>/<snapName>" — snapName
// is the last segment (slash-free), the dataset is everything before it.
type snapshotID struct {
	Node     string
	Dataset  string
	SnapName string
}

const snapshotIDPrefix = "locals/v1"

func (s snapshotID) String() string {
	return snapshotIDPrefix + "/" + s.Node + "/" + s.Dataset + "/" + s.SnapName
}

func parseSnapshotID(id string) (snapshotID, error) {
	parts := strings.SplitN(id, "/", 4) // [locals, v1, node, <dataset>/<snapName>]
	if len(parts) != 4 || parts[0] != "locals" || parts[1] != "v1" || parts[2] == "" || parts[3] == "" {
		return snapshotID{}, driver.InvalidArgument("malformed local snapshot id %q", id)
	}
	rest := parts[3]
	i := strings.LastIndexByte(rest, '/')
	if i <= 0 || i == len(rest)-1 {
		return snapshotID{}, driver.InvalidArgument("malformed local snapshot id %q", id)
	}
	ds, snap := rest[:i], rest[i+1:]
	if !validDataset(ds) {
		return snapshotID{}, driver.InvalidArgument("malformed local snapshot id %q", id)
	}
	return snapshotID{Node: parts[2], Dataset: ds, SnapName: snap}, nil
}

// validDataset checks a dataset is "<pool>/<...>/<vol>" (>=1 segment beyond the
// pool, no leading/trailing/double slash, no traversal).
func validDataset(ds string) bool {
	if ds == "" || !strings.Contains(ds, "/") {
		return false
	}
	if strings.HasPrefix(ds, "/") || strings.HasSuffix(ds, "/") ||
		strings.Contains(ds, "//") || strings.Contains(ds, "..") {
		return false
	}
	return true
}

// poolOf returns the zpool name (first segment) of a dataset path.
func poolOf(dataset string) string {
	if i := strings.IndexByte(dataset, '/'); i >= 0 {
		return dataset[:i]
	}
	return dataset
}

// lastSegment returns the final '/'-separated component of a dataset path.
func lastSegment(dataset string) string {
	if i := strings.LastIndexByte(dataset, '/'); i >= 0 {
		return dataset[i+1:]
	}
	return dataset
}

// Volume-context keys carried from CreateVolume to NodeStageVolume.
const (
	ctxKeyDataset = "dataset" // full zvol dataset (pool/parent/vol)
	ctxKeyFsType  = "fsType"  // filesystem to format/mount
	ctxKeyNode    = "node"    // owning node (sanity check at stage)
)

// sanitizeName maps a CSI name to a ZFS-safe dataset component
// ([A-Za-z0-9_.:-], reasonable length).
func sanitizeName(name string) string {
	b := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9',
			r == '_', r == '-', r == '.', r == ':':
			b = append(b, r)
		default:
			b = append(b, '-')
		}
	}
	out := string(b)
	if len(out) > 200 {
		out = out[:200]
	}
	return out
}
