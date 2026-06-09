// Package driver defines the backend abstraction that the CSI gRPC layer
// dispatches to. A backend spans the whole volume lifecycle — controller-side
// provisioning and node-side attach/format/mount — because the qnap and local
// backends differ on both ends, not just provisioning. Everything above this
// interface (the CSI server, dispatch, error mapping) is backend-agnostic;
// everything a backend needs to talk to its storage lives below it.
package driver

import "context"

// Backend is one storage backend (selected by --driver). A controller-only
// process has Node()==nil; a node-only process has Controller()==nil; a
// monolith has both.
type Backend interface {
	// Name is the short identifier ("qnap"|"local"), used as the metrics
	// driver= label.
	Name() string
	// PluginName is the reverse-DNS CSI plugin name reported by GetPluginInfo.
	PluginName() string
	// Capabilities declares what this backend supports.
	Capabilities() Capabilities
	// Controller returns the controller half, or nil if this process is
	// node-only.
	Controller() ControllerBackend
	// Node returns the node half, or nil if this process is controller-only.
	Node() NodeBackend
	// Probe reports backend readiness (Identity.Probe + startup validation).
	Probe(ctx context.Context) error
}

// ControllerBackend implements the controller-side RPCs. Methods may return an
// *Error to control the gRPC code; any other error is treated as Internal.
type ControllerBackend interface {
	CreateVolume(ctx context.Context, req *CreateVolumeRequest) (*Volume, error)
	DeleteVolume(ctx context.Context, volumeID string, secrets map[string]string) error
	// VolumeExists reports whether volumeID currently refers to a live volume.
	// Used by ValidateVolumeCapabilities to honor the CSI NotFound contract.
	// A transient backend failure returns an error; a definitively-absent (or
	// index-reused) volume returns (false, nil).
	VolumeExists(ctx context.Context, volumeID string) (bool, error)
	// ExpandVolume grows a volume. It returns the new capacity and whether a
	// subsequent NodeExpandVolume is required to grow the filesystem.
	ExpandVolume(ctx context.Context, volumeID string, capRange CapacityRange, secrets map[string]string) (newBytes int64, nodeExpansionRequired bool, err error)

	// ControllerPublishVolume / Unpublish are only invoked when Capabilities
	// advertises PublishUnpublish. PublishContext flows to NodeStageVolume.
	ControllerPublishVolume(ctx context.Context, volumeID, nodeID string, cap VolumeCapability, readonly bool, volumeContext, secrets map[string]string) (publishContext map[string]string, err error)
	ControllerUnpublishVolume(ctx context.Context, volumeID, nodeID string, secrets map[string]string) error

	// Snapshot RPCs are only invoked when Capabilities advertises Snapshot.
	CreateSnapshot(ctx context.Context, req *CreateSnapshotRequest) (*Snapshot, error)
	DeleteSnapshot(ctx context.Context, snapshotID string, secrets map[string]string) error

	// GetCapacity reports free bytes for the given topology, only invoked when
	// Capabilities advertises GetCapacity.
	GetCapacity(ctx context.Context, topology []Topology, parameters map[string]string) (int64, error)

	// ListVolumes returns all volumes the backend manages, only invoked when
	// Capabilities advertises ListVolumes. v1 is non-paginated.
	ListVolumes(ctx context.Context) ([]VolumeInfo, error)

	// ListSnapshots returns snapshots the backend manages, only invoked when
	// Capabilities advertises ListSnapshots. sourceVolumeID filters to one
	// volume's snapshots ("" = all). v1 is non-paginated.
	ListSnapshots(ctx context.Context, sourceVolumeID string) ([]SnapshotInfo, error)
}

// Shutdowner is an optional capability a Backend may implement to release
// resources on graceful shutdown (e.g. stop a forwarding server, deregister
// from service discovery). The run command invokes it after the gRPC server
// stops, if present.
type Shutdowner interface {
	Shutdown(ctx context.Context) error
}

// NodeBackend implements the node-side RPCs.
type NodeBackend interface {
	// StageVolume attaches the backing block device, idempotently formats it,
	// and mounts it at the staging path.
	StageVolume(ctx context.Context, req *StageRequest) error
	UnstageVolume(ctx context.Context, req *UnstageRequest) error
	// PublishVolume bind-mounts the staging path (or device node) at the target.
	PublishVolume(ctx context.Context, req *PublishRequest) error
	UnpublishVolume(ctx context.Context, req *UnpublishRequest) error
	// ExpandVolume grows the filesystem on a staged/published volume. Only
	// invoked when Capabilities advertises NodeExpand.
	ExpandVolume(ctx context.Context, req *NodeExpandRequest) (newBytes int64, err error)
	// GetInfo returns this node's identity and accessible topology segment.
	GetInfo(ctx context.Context) (*NodeInfo, error)
	// GetVolumeStats returns filesystem usage for a published volume, only
	// invoked when Capabilities advertises NodeVolumeStats.
	GetVolumeStats(ctx context.Context, volumeID, volumePath string) (*VolumeStats, error)
}
