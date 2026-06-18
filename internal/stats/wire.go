package stats

// Forward-transport method names for the stats RPCs, shared by both backends so
// the node (server) and controller (client) agree on the wire protocol.
const (
	MethodVolStats     = "volstats"     // per-volume usage for one volume (idArgs{ID})
	MethodVolStatsDump = "volstatsdump" // all readings on the node ([]CSIVolumeStats)
)

// VolStatsArgs is the request body for MethodVolStats.
type VolStatsArgs struct {
	ID string `json:"id"`
}
