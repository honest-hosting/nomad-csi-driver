package qnap

import (
	"context"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

// statsEvictGraceSweeps mirrors the local reconciler: a cached volume must be
// absent from the host for two consecutive sweeps before eviction, absorbing the
// race between a sweep's session snapshot and a concurrent stage. Eviction only
// drops a stats cache entry — never a session or mount — so a wrong call is
// cosmetic and self-heals next sweep (METRICS-RESTART-CONSISTENCY.PLAN.md §4.4).
const statsEvictGraceSweeps = 2

// statsReconciler rehydrates the node's per-volume stats registry from live iSCSI
// sessions on startup + on a ticker, so nomad_csi_volume_* survives a plugin
// restart with no re-stage (Nomad does not re-issue NodeStageVolume). The registry
// is keyed by external id, which the host does not know, so each staged session's
// (IQN, LUN) is resolved to its external id via the read-only SAN identity cache.
//
// Resolutions are memoized for the process lifetime: a LUN's identity is stable,
// and a reused index rides a fresh target IQN, so a stale memo cannot mis-resolve.
// Steady-state sweeps therefore make ZERO SAN calls — only the post-restart batch
// hydrates (coalesced into one ListTargets+ListLUNs), honoring the cold-only SAN
// posture (STAGED-GAUGE D8). Otherwise host-only and side-effect-free.
type statsReconciler struct {
	nd       *node
	reg      *stats.Registry
	interval time.Duration
	log      *zap.Logger

	resolved map[string]string // "IQN/LUN" → external id (process-lifetime memo)
}

func newStatsReconciler(nd *node, reg *stats.Registry, interval time.Duration) *statsReconciler {
	return &statsReconciler{nd: nd, reg: reg, interval: interval, log: nd.log, resolved: map[string]string{}}
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

// sweep reconciles the stats registry to the currently-staged iSCSI sessions. It
// stays add-only (skips eviction) when the host enumeration fails OR when any
// session's external id could not be resolved, so a transient host/SAN blip never
// drops a live volume's stats.
func (sr *statsReconciler) sweep(ctx context.Context) {
	sessions, err := sr.nd.stagedSessions(ctx)
	if err != nil {
		sr.log.Warn("stats reconcile: listing staged sessions failed; skipping this sweep (add-only)", zap.Error(err))
		return
	}
	desired := make([]stats.TrackSpec, 0, len(sessions))
	resolvedAll := true
	for _, s := range sessions {
		key := s.IQN + "/" + strconv.Itoa(s.LUN)
		extID, ok := sr.resolved[key]
		if !ok {
			extID, ok = sr.nd.san.resolveExternalID(ctx, s.IQN, s.LUN)
			if !ok {
				resolvedAll = false // desired is incomplete → do not evict this sweep
				sr.log.Warn("stats reconcile: could not resolve external id for staged session; skipping",
					zap.String("iqn", s.IQN), zap.Int("lun", s.LUN))
				continue
			}
			sr.resolved[key] = extID
		}
		desired = append(desired, stats.TrackSpec{VolumeID: extID, StagingPath: s.StagingPath, AccessType: s.AccessType})
	}
	evicted := sr.reg.Reconcile(desired, resolvedAll, statsEvictGraceSweeps)
	if len(evicted) > 0 {
		sr.log.Info("stats reconcile: evicted vanished volumes from stats registry", zap.Strings("volume_ids", evicted))
	}
}
