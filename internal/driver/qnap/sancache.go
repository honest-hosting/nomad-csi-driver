package qnap

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	goqnap "github.com/honest-hosting/go-qnap"
)

// sanIdentityCacheTTL bounds how long a hydrated SAN identity map is reused
// before a re-hydration. Short, because it is only consulted on cold-cache block
// unstages (rare).
const sanIdentityCacheTTL = 30 * time.Second

// sanIdentityCache resolves a volume's target IQN from the SAN for cold-cache
// block-volume teardown (tier 3). It hydrates the whole target+LUN identity map
// in ONE ListTargets + ListLUNs pair and reuses it for a short TTL; the mutex
// makes concurrent cold unstages (a node-drain storm) share a single hydration
// instead of each hitting the appliance — OQ4/D8: ~2 SAN calls per storm, not
// O(unstages). All appliance access is read-only. It degrades open: any error
// yields ok=false, and the caller leaves the session for the reconciler.
type sanIdentityCache struct {
	cl    Client
	sm    *sessionManager
	ttl   time.Duration
	nowFn func() time.Time
	log   *zap.Logger

	mu     sync.Mutex
	loaded time.Time
	tgts   map[int]goqnap.Target    // TargetIndex → Target
	luns   map[int]goqnap.LUN       // LUNIndex → LUN
	byIQN  map[string]goqnap.Target // target IQN → Target (for session→volume resolution)
}

func newSANIdentityCache(cl Client, sm *sessionManager, ttl time.Duration, log *zap.Logger) *sanIdentityCache {
	if log == nil {
		log = zap.NewNop()
	}
	return &sanIdentityCache{cl: cl, sm: sm, ttl: ttl, nowFn: time.Now, log: log}
}

// resolveIQN returns the target IQN for the volume identified by eid, verifying
// the LUN name still matches (the index-reuse data-safety guard). ok=false on any
// cache miss, name mismatch, or SAN error (degrade-open).
func (c *sanIdentityCache) resolveIQN(ctx context.Context, eid externalID) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tgts == nil || c.nowFn().Sub(c.loaded) > c.ttl {
		if err := c.hydrate(ctx); err != nil {
			c.log.Warn("SAN identity hydration failed; block teardown degraded", zap.Error(err))
			return "", false
		}
	}
	lun, okL := c.luns[eid.LUNIndex]
	tgt, okT := c.tgts[eid.TargetIndex]
	if !okL || !okT {
		return "", false
	}
	if lun.Name != eid.LUNName {
		c.log.Warn("SAN LUN name mismatch (index reuse?); refusing block teardown identity",
			zap.Int("lun_index", eid.LUNIndex), zap.String("want", eid.LUNName), zap.String("got", lun.Name))
		return "", false
	}
	return tgt.IQN, true
}

// hydrate refreshes the target+LUN maps from the appliance in one pair of list
// calls. Caller holds c.mu.
func (c *sanIdentityCache) hydrate(ctx context.Context) error {
	sess, err := c.sm.get(ctx)
	if err != nil {
		return err
	}
	tgts, err := c.cl.ListTargets(ctx, sess)
	if err != nil {
		return err
	}
	luns, err := c.cl.ListLUNs(ctx, sess)
	if err != nil {
		return err
	}
	tm := make(map[int]goqnap.Target, len(tgts))
	im := make(map[string]goqnap.Target, len(tgts))
	for _, t := range tgts {
		tm[t.Index] = t
		if t.IQN != "" {
			im[t.IQN] = t
		}
	}
	lm := make(map[int]goqnap.LUN, len(luns))
	for _, l := range luns {
		lm[l.Index] = l
	}
	c.tgts, c.luns, c.byIQN, c.loaded = tm, lm, im, c.nowFn()
	return nil
}

// resolveExternalID reconstructs a volume's CSI external id from the SAN given a
// host iSCSI session's target IQN and SCSI LUN number. The node stats registry is
// keyed by external id, but after a plugin restart the host only knows (IQN, LUN);
// this bridges the two so the registry can be rehydrated without a re-stage. The
// reconstruction is exactly what the controller's ListVolumes uses
// (externalID{LUNIndex, TargetIndex, OwnTarget: alias==name, LUNName}). ok=false on
// cache miss, LUN-number out of range, or SAN error (the caller then stays
// add-only). Cold-only + coalesced via the shared hydration + TTL, like resolveIQN.
func (c *sanIdentityCache) resolveExternalID(ctx context.Context, iqn string, lunNumber int) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tgts == nil || c.nowFn().Sub(c.loaded) > c.ttl {
		if err := c.hydrate(ctx); err != nil {
			c.log.Warn("SAN identity hydration failed; stats rehydration degraded", zap.Error(err))
			return "", false
		}
	}
	tgt, ok := c.byIQN[iqn]
	if !ok {
		return "", false
	}
	// 1:1 target (the driver's default): exactly one LUN, mapped at SCSI LUN 0 — use
	// it directly, no ordering assumption. 1:N shared target: index by the SCSI LUN
	// number (ordinal). Bounds-guarded either way.
	var lunIndex int
	switch {
	case len(tgt.LUNs) == 1:
		lunIndex = tgt.LUNs[0]
	case lunNumber >= 0 && lunNumber < len(tgt.LUNs):
		lunIndex = tgt.LUNs[lunNumber]
	default:
		return "", false
	}
	lun, ok := c.luns[lunIndex]
	if !ok {
		return "", false
	}
	eid := externalID{
		LUNIndex:    lunIndex,
		TargetIndex: tgt.Index,
		OwnTarget:   tgt.Alias == lun.Name,
		LUNName:     lun.Name,
	}
	return eid.String(), true
}
