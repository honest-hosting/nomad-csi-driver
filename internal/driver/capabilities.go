package driver

// Capabilities declares what a backend supports. The CSI layer turns these into
// the protobuf capability lists returned by ControllerGetCapabilities and
// NodeGetCapabilities, so each backend advertises exactly what it implements.
type Capabilities struct {
	// Controller-side
	CreateDelete     bool // CREATE_DELETE_VOLUME
	PublishUnpublish bool // PUBLISH_UNPUBLISH_VOLUME (controller publish/unpublish)
	Expand           bool // EXPAND_VOLUME
	Snapshot         bool // CREATE_DELETE_SNAPSHOT
	Clone            bool // CLONE_VOLUME
	GetCapacity      bool // GET_CAPACITY
	ListVolumes      bool // LIST_VOLUMES
	ListSnapshots    bool // LIST_SNAPSHOTS

	// Node-side
	NodeStage       bool // STAGE_UNSTAGE_VOLUME
	NodeExpand      bool // EXPAND_VOLUME (node)
	NodeVolumeStats bool // GET_VOLUME_STATS

	// ExpandOnline reports whether expansion can happen while the volume is
	// published (online). False means offline-only.
	ExpandOnline bool

	// Topology reports whether the plugin constrains volume placement by
	// topology (VOLUME_ACCESSIBILITY_CONSTRAINTS). local=true (node-pinned);
	// qnap=false (reachable from any node).
	Topology bool
}
