package qnap

import (
	"context"
	"path/filepath"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/iscsi"
	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
	"github.com/honest-hosting/nomad-csi-driver/internal/multipath"
)

const (
	defaultReconcileInterval = 2 * time.Minute
	defaultReconcileGrace    = 5 * time.Minute
)

// reconciler is the qnap node's session-leak / split-brain guard. On a schedule
// it logs out THIS plugin's iSCSI target sessions that no longer back any mount
// or cached stage — sessions leaked by an unstage that could not detach (a
// crash, a SAN outage, a cold-cache block volume). A leaked session left logged
// in while the volume is reassigned to another node lets two initiators write one
// LUN → split-brain corruption (OQ4); the reconciler bounds how long that can
// persist. Host-only: it never touches the SAN.
//
// A target is torn down only when ALL of its LUNs look orphaned across TWO
// consecutive sweeps at least `grace` apart, so an in-flight stage/publish
// (notably a block volume momentarily staged-but-unpublished) is not mistaken
// for a leak.
type reconciler struct {
	iscsi        *iscsi.Connector
	mounter      *mountutil.Mounter
	mpath        *multipath.Manager
	useMultipath bool
	ourPortals   func() map[string]struct{}
	cachedIQNs   func() map[string]struct{}
	interval     time.Duration
	grace        time.Duration
	nowFn        func() time.Time
	resolve      func(string) (string, error) // symlink resolver; filepath.EvalSymlinks in prod
	log          *zap.Logger

	orphanSince map[string]time.Time // IQN → first sweep it looked fully orphaned
}

func newReconciler(n *node, interval, grace time.Duration) *reconciler {
	return &reconciler{
		iscsi:        n.iscsi,
		mounter:      n.mounter,
		mpath:        n.mpath,
		useMultipath: n.useMultipath,
		ourPortals:   n.ourPortals,
		cachedIQNs:   n.cachedIQNs,
		interval:     interval,
		grace:        grace,
		nowFn:        time.Now,
		resolve:      filepath.EvalSymlinks,
		log:          n.log,
		orphanSince:  map[string]time.Time{},
	}
}

// Run sweeps immediately (catching pre-restart leaks at startup), then every
// interval until ctx is cancelled.
func (r *reconciler) Run(ctx context.Context) {
	if len(r.ourPortals()) == 0 {
		r.log.Info("qnap reconciler: no configured portals; treating all host iSCSI sessions as this plugin's " +
			"(set qnap.portals on the node to scope if the host runs other iSCSI workloads)")
	}
	r.sweep(ctx)
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.sweep(ctx)
		}
	}
}

// sweep classifies this plugin's target sessions live vs orphan and logs out
// targets that have stayed fully orphaned past the grace window.
func (r *reconciler) sweep(ctx context.Context) {
	sessions, err := r.iscsi.ListSessions(ctx)
	if err != nil {
		r.log.Warn("reconcile: listing sessions failed", zap.Error(err))
		return
	}
	mounts, err := r.mounter.ListMounts(ctx)
	if err != nil {
		r.log.Warn("reconcile: listing mounts failed", zap.Error(err))
		return
	}
	mounted := make(map[string]struct{}, len(mounts))
	for _, m := range mounts {
		mounted[m.Source] = struct{}{}
	}
	cached := r.cachedIQNs()
	ours := r.ourPortals()

	// Group this plugin's sessions by target IQN. A target is live if the IQN is
	// in the tier-1 cache (staged this lifetime) or ANY of its LUN paths is mounted.
	type tstate struct {
		portals map[string]struct{}
		live    bool
	}
	targets := map[string]*tstate{}
	for _, s := range sessions {
		if !portalOwned(s.Portal, ours) {
			continue // another plugin's SAN
		}
		t := targets[s.IQN]
		if t == nil {
			t = &tstate{portals: map[string]struct{}{}}
			targets[s.IQN] = t
		}
		t.portals[normalizePortal(s.Portal)] = struct{}{}
		if _, ok := cached[s.IQN]; ok {
			t.live = true
		}
		if r.deviceMounted(ctx, s.Device, mounted) {
			t.live = true
		}
	}

	now := r.nowFn()
	for iqn, t := range targets {
		if t.live {
			delete(r.orphanSince, iqn)
			continue
		}
		first, seen := r.orphanSince[iqn]
		if !seen {
			r.orphanSince[iqn] = now // first orphan sighting; hold for grace
			continue
		}
		if now.Sub(first) < r.grace {
			continue
		}
		r.log.Warn("reconcile: logging out leaked iSCSI target (no mount, no cached stage)", zap.String("iqn", iqn))
		for p := range t.portals {
			if err := r.iscsi.Logout(ctx, p, iqn); err != nil {
				r.log.Warn("reconcile: logout failed (retry next sweep)", zap.String("portal", p), zap.String("iqn", iqn), zap.Error(err))
			}
		}
		delete(r.orphanSince, iqn)
	}

	// Forget orphan timers for targets that are gone (logged out / vanished).
	for iqn := range r.orphanSince {
		if _, ok := targets[iqn]; !ok {
			delete(r.orphanSince, iqn)
		}
	}
}

// deviceMounted reports whether a session's path device backs any current mount,
// directly (non-multipath) or via its multipath mapper (multipath).
//
// CRITICAL: a mount source from raw findmnt is the RESOLVED device — /dev/dm-N for
// a multipath map, not the /dev/mapper/<wwid> symlink — so each candidate is
// checked both as-is and symlink-resolved. Without this a live multipath session
// looks orphaned and would be wrongly logged out (data-loss on a running volume).
func (r *reconciler) deviceMounted(ctx context.Context, device string, mounted map[string]struct{}) bool {
	if device == "" {
		return false
	}
	candidates := []string{device}
	if r.useMultipath {
		if wwid, err := r.mpath.MapWWID(ctx, device); err == nil && wwid != "" {
			candidates = append(candidates, r.mpath.MapperPath(wwid))
		}
	}
	for _, cand := range candidates {
		if _, ok := mounted[cand]; ok {
			return true
		}
		if real, err := r.resolve(cand); err == nil {
			if _, ok := mounted[real]; ok {
				return true
			}
		}
	}
	return false
}
