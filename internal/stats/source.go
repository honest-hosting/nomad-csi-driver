package stats

import (
	"context"
	"errors"
	"fmt"
)

// ErrNotMounted is returned by Source.Stats when the volume id IS known to Nomad
// but is not currently staged/mounted on any node, so no usage reading exists
// yet (e.g. a created-but-never-mounted volume, or one whose workload is
// stopped). The query API maps it to 412 Precondition Failed — distinct from a
// 404 for an id Nomad doesn't know at all.
var ErrNotMounted = errors.New("volume is not mounted on any node")

// NotMounted wraps ErrNotMounted with a volume-specific, human-readable message.
func NotMounted(nomadID string) error {
	return fmt.Errorf("%w: volume %q is not mounted on any node — usage stats appear once a workload stages it", ErrNotMounted, nomadID)
}

// Mapper resolves between a Nomad volume id (what the public API speaks) and the
// driver's internal external id (what the cache is keyed by). Implemented by
// cluster.NomadVolumes; defined here so the stats package owns the seam and stays
// import-cycle-free.
type Mapper interface {
	// ExternalID resolves a Nomad volume id within a namespace to the external id.
	// found is false when the namespace has no such volume.
	ExternalID(ctx context.Context, namespace, nomadID string) (externalID string, found bool, err error)
	// Reverse returns externalID→nomadID for a namespace (for list/metrics relabel).
	Reverse(ctx context.Context, namespace string) (map[string]string, error)
}

// Source is the controller-side read interface the query API and metrics
// collector consume. It speaks the **Nomad volume id** (resolution to the
// internal external id happens inside the implementation). Routing differs:
//   - local: Stats resolves id→externalID then forwards to the owning node
//     (node embedded in the external id); All returns this monolith's own
//     readings, relabeled to Nomad ids.
//   - qnap: both serve from a controller-side aggregate, relabeled to Nomad ids.
//
// Both serve from cached data — neither triggers statfs/walk on the call path.
type Source interface {
	// Stats returns the reading for one volume by Nomad id + namespace. found is
	// false when Nomad doesn't know the id or no node currently tracks it (→ 404).
	Stats(ctx context.Context, nomadID, namespace string) (cs PublicVolumeStats, found bool, err error)
	// All returns every reading this controller can see in the namespace, keyed by
	// Nomad id (for /metrics + the list endpoint).
	All(ctx context.Context, namespace string) ([]PublicVolumeStats, error)
}
