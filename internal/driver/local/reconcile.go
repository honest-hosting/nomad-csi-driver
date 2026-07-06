package local

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

// statsEvictGraceSweeps is how many consecutive reconcile passes a cached volume
// must be absent from the host mount table before the reconciler evicts it. Two
// (not one) absorbs the race between a sweep's mount-table snapshot and a volume
// being staged concurrently: a just-staged volume missed by one snapshot is kept
// and picked up next sweep rather than dropped for an interval. Eviction only ever
// removes a *stats* cache entry — never a mount — so a wrong call is cosmetic and
// self-healing (METRICS-RESTART-CONSISTENCY.PLAN.md §4.4).
const statsEvictGraceSweeps = 2

// statsReconciler rehydrates the node's stats registry from the live mount table
// on startup and on a ticker. Nomad does not re-issue NodeStageVolume after a
// plugin-task restart, so the RPC-populated registry would otherwise stay empty
// and nomad_csi_volume_* would under-report every volume staged before the restart
// (METRICS-RESTART-CONSISTENCY.PLAN.md). It is host-only and side-effect-free: it
// only Tracks/Untracks stats, never touching a mount or device.
type statsReconciler struct {
	nd       *node
	reg      *stats.Registry
	interval time.Duration
	log      *zap.Logger
}

// Run sweeps immediately (rehydrating pre-restart state at startup), then every
// interval until ctx is cancelled.
func (sr *statsReconciler) Run(ctx context.Context) {
	iv := sr.interval
	if iv <= 0 {
		iv = stats.DefaultInterval
	}
	sr.sweep(ctx)
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			sr.sweep(ctx)
		}
	}
}

// sweep reconciles the stats registry to the currently-staged volumes. On a mount
// enumeration error it stays add-only (skips eviction) so a transient findmnt blip
// never drops live volumes.
func (sr *statsReconciler) sweep(ctx context.Context) {
	desired, err := sr.nd.stagedVolumes(ctx)
	if err != nil {
		sr.log.Warn("stats reconcile: listing staged volumes failed; skipping this sweep (add-only)", zap.Error(err))
		return
	}
	evicted := sr.reg.Reconcile(desired, true, statsEvictGraceSweeps)
	if len(evicted) > 0 {
		sr.log.Info("stats reconcile: evicted vanished volumes from stats registry", zap.Strings("volume_ids", evicted))
	}
}
