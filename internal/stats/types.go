// Package stats computes and caches per-volume filesystem usage (bytes, inodes,
// and file/dir/other counts) for CSI volumes a node currently has staged,
// independently of the storage backend. Each node runs one background supervisor
// per staged volume: it refreshes a cheap statfs reading on a fast cadence and,
// less often, a concurrent directory walk for object counts. The latest reading
// is cached in memory and served to the controller query/metrics layers.
//
// The subsystem is built to degrade to *stale data*, never to block the CSI RPC
// path: reads serve the cache, IO runs lock-free with only a brief swap under
// lock, hung syscalls are abandoned by a watchdog (single-flight bounds leaked
// goroutines), and repeated failures back off and self-heal on recovery.
package stats

import "time"

// Access types for a tracked volume. A block volume has no filesystem, so it is
// recorded for presence only and never statfs'd or walked.
const (
	AccessMount = "mount"
	AccessBlock = "block"
)

// CSIVolumeStats is the per-volume usage record served over the query API and
// emitted to metrics. Bytes/inodes come from statfs; file/dir/other counts come
// from the concurrent tree walk. The timestamps let callers judge freshness for
// themselves.
type CSIVolumeStats struct {
	VolumeID   string `json:"volume_id"`
	Node       string `json:"node"`
	AccessType string `json:"access_type"` // "mount" | "block"

	// statfs (cheap, fast cadence)
	TotalBytes     int64     `json:"total_bytes"`
	UsedBytes      int64     `json:"used_bytes"`
	AvailableBytes int64     `json:"available_bytes"`
	TotalInodes    int64     `json:"total_inodes"`
	UsedInodes     int64     `json:"used_inodes"`
	FreeInodes     int64     `json:"free_inodes"`
	StatfsAt       time.Time `json:"statfs_at"`

	// tree walk (expensive, slow cadence) — counts only, no per-file sizes
	FileCount    int64         `json:"file_count"`
	DirCount     int64         `json:"dir_count"`
	OtherCount   int64         `json:"other_count"` // symlinks/sockets/devices/pipes
	WalkAt       time.Time     `json:"walk_at"`
	WalkDuration time.Duration `json:"walk_duration"`
	WalkComplete bool          `json:"walk_complete"` // false until the first full walk

	// LastError carries the most recent statfs/walk failure ("" when healthy). A
	// coarse health hint; last-good measurements are retained regardless.
	LastError string `json:"last_error,omitempty"`
}

// PublicVolumeStats is the controller's public, Nomad-id-keyed view of a volume's
// usage. It deliberately omits the driver's internal external id — external
// consumers (the klm API, operators) only ever see the Nomad volume `id`. The
// measurement fields mirror CSIVolumeStats.
type PublicVolumeStats struct {
	ID         string `json:"id"`        // Nomad volume id (what operators manage)
	Namespace  string `json:"namespace"` // Nomad namespace
	Node       string `json:"node"`
	AccessType string `json:"access_type"`

	TotalBytes     int64     `json:"total_bytes"`
	UsedBytes      int64     `json:"used_bytes"`
	AvailableBytes int64     `json:"available_bytes"`
	TotalInodes    int64     `json:"total_inodes"`
	UsedInodes     int64     `json:"used_inodes"`
	FreeInodes     int64     `json:"free_inodes"`
	StatfsAt       time.Time `json:"statfs_at"`

	FileCount    int64         `json:"file_count"`
	DirCount     int64         `json:"dir_count"`
	OtherCount   int64         `json:"other_count"`
	WalkAt       time.Time     `json:"walk_at"`
	WalkDuration time.Duration `json:"walk_duration"`
	WalkComplete bool          `json:"walk_complete"`

	LastError string `json:"last_error,omitempty"`
}

// ToPublic projects an internal (external-id-keyed) reading onto the public
// Nomad-id-keyed DTO.
func (cs CSIVolumeStats) ToPublic(nomadID, namespace string) PublicVolumeStats {
	return PublicVolumeStats{
		ID: nomadID, Namespace: namespace, Node: cs.Node, AccessType: cs.AccessType,
		TotalBytes: cs.TotalBytes, UsedBytes: cs.UsedBytes, AvailableBytes: cs.AvailableBytes,
		TotalInodes: cs.TotalInodes, UsedInodes: cs.UsedInodes, FreeInodes: cs.FreeInodes,
		StatfsAt:  cs.StatfsAt,
		FileCount: cs.FileCount, DirCount: cs.DirCount, OtherCount: cs.OtherCount,
		WalkAt: cs.WalkAt, WalkDuration: cs.WalkDuration, WalkComplete: cs.WalkComplete,
		LastError: cs.LastError,
	}
}

// Relabel maps external-id-keyed readings to public DTOs via the
// externalID→nomadID map. Records with no mapping are dropped (not registered in
// Nomad, or in another namespace).
func Relabel(in []CSIVolumeStats, rev map[string]string, namespace string) []PublicVolumeStats {
	out := make([]PublicVolumeStats, 0, len(in))
	for _, cs := range in {
		if nomadID, ok := rev[cs.VolumeID]; ok {
			out = append(out, cs.ToPublic(nomadID, namespace))
		}
	}
	return out
}
