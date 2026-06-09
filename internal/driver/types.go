package driver

import "time"

// These are the transport-neutral request/response types that backends operate
// on. The CSI gRPC layer translates protobuf <-> these, so backends are
// testable without the CSI proto and never depend on a specific spec version.

// AccessType is how a volume is consumed by the workload.
type AccessType int

const (
	// AccessTypeMount is a formatted filesystem mounted into the task.
	AccessTypeMount AccessType = iota
	// AccessTypeBlock is a raw block device exposed to the task.
	AccessTypeBlock
)

// AccessMode is the CSI access mode. Only the single-node modes are supported;
// the CSI layer rejects multi-node modes before they reach a backend.
type AccessMode int

// The supported single-node access modes (multi-node is rejected upstream).
const (
	AccessModeUnknown AccessMode = iota
	// AccessModeSingleNodeWriter — one node, read-write.
	AccessModeSingleNodeWriter
	// AccessModeSingleNodeReaderOnly — one node, read-only.
	AccessModeSingleNodeReaderOnly
)

// VolumeCapability pairs an access type with an access mode and, for mount
// volumes, the filesystem and mount flags.
type VolumeCapability struct {
	AccessType AccessType
	AccessMode AccessMode
	FsType     string   // mount only
	MountFlags []string // mount only
}

// CapacityRange is the requested size window in bytes.
type CapacityRange struct {
	RequiredBytes int64
	LimitBytes    int64
}

// Topology is a set of segments (key=value) describing where a volume lives or
// can be reached. For local volumes this pins the node.
type Topology struct {
	Segments map[string]string
}

// ContentSource identifies the origin for a create-from-source request. Exactly
// one of SnapshotID / VolumeID is set.
type ContentSource struct {
	SnapshotID string
	VolumeID   string
}

// CreateVolumeRequest is the neutral form of CSI CreateVolume.
type CreateVolumeRequest struct {
	Name               string
	CapacityRange      CapacityRange
	VolumeCapabilities []VolumeCapability
	Parameters         map[string]string // fsType, sectorSize/volblocksize, host, pool, …
	Secrets            map[string]string
	ContentSource      *ContentSource
	// TopologyRequirement carries the preferred/requisite topologies Nomad
	// passes through. Note (verified): Nomad 1.6.3 does NOT populate this with
	// scheduler intent, so backends must not rely on it for placement.
	TopologyRequirement []Topology
}

// Volume is the neutral form of the CSI Volume returned by CreateVolume.
type Volume struct {
	VolumeID           string
	CapacityBytes      int64
	VolumeContext      map[string]string
	AccessibleTopology []Topology
	ContentSource      *ContentSource
}

// CreateSnapshotRequest is the neutral form of CSI CreateSnapshot.
type CreateSnapshotRequest struct {
	SourceVolumeID string
	Name           string
	Parameters     map[string]string
	Secrets        map[string]string
}

// Snapshot is the neutral form of the CSI Snapshot.
type Snapshot struct {
	SnapshotID     string
	SourceVolumeID string
	SizeBytes      int64
	CreationTime   time.Time
	ReadyToUse     bool
}

// StageRequest is the neutral form of CSI NodeStageVolume.
type StageRequest struct {
	VolumeID          string
	StagingTargetPath string
	VolumeCapability  VolumeCapability
	VolumeContext     map[string]string
	PublishContext    map[string]string
	Secrets           map[string]string
}

// UnstageRequest is the neutral form of CSI NodeUnstageVolume.
type UnstageRequest struct {
	VolumeID          string
	StagingTargetPath string
}

// PublishRequest is the neutral form of CSI NodePublishVolume.
type PublishRequest struct {
	VolumeID          string
	StagingTargetPath string
	TargetPath        string
	VolumeCapability  VolumeCapability
	VolumeContext     map[string]string
	PublishContext    map[string]string
	Readonly          bool
}

// UnpublishRequest is the neutral form of CSI NodeUnpublishVolume.
type UnpublishRequest struct {
	VolumeID   string
	TargetPath string
}

// NodeExpandRequest is the neutral form of CSI NodeExpandVolume.
type NodeExpandRequest struct {
	VolumeID          string
	VolumePath        string
	StagingTargetPath string
	CapacityRange     CapacityRange
	VolumeCapability  VolumeCapability
}

// NodeInfo is the neutral form of CSI NodeGetInfo.
type NodeInfo struct {
	NodeID             string
	MaxVolumesPerNode  int64
	AccessibleTopology *Topology
}

// VolumeInfo is one entry returned by ListVolumes.
type VolumeInfo struct {
	VolumeID           string
	CapacityBytes      int64
	AccessibleTopology []Topology
}

// SnapshotInfo is one entry returned by ListSnapshots.
type SnapshotInfo struct {
	SnapshotID     string
	SourceVolumeID string
	SizeBytes      int64
	CreationTime   time.Time
	ReadyToUse     bool
}

// VolumeStats is filesystem usage for a published volume (NodeGetVolumeStats).
type VolumeStats struct {
	TotalBytes     int64
	UsedBytes      int64
	AvailableBytes int64
	TotalInodes    int64
	UsedInodes     int64
	FreeInodes     int64
}
