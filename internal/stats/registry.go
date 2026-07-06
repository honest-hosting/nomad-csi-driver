package stats

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
)

// statFunc reads filesystem usage for a path. Injected so tests (and fault
// injection) can substitute a fake; defaults to mountutil.StatFS.
type statFunc func(path string) (mountutil.FSStats, error)

// Registry is the node-side per-volume stats store: one supervisor goroutine per
// staged mount volume keeps an in-memory CSIVolumeStats fresh. All reads serve
// the cache; the registry never blocks on filesystem IO (see worker).
//
// All methods are nil-safe and no-op when Enabled is false, so backends can wire
// Track/Untrack unconditionally.
type Registry struct {
	cfg    Config
	node   string
	log    *zap.Logger
	statFn statFunc
	nowFn  func() time.Time

	pool   *walkPool
	base   context.Context
	cancel context.CancelFunc

	mu    sync.RWMutex
	vols  map[string]*worker
	cache map[string]CSIVolumeStats
	// absent counts, per cached volume, how many consecutive Reconcile passes it
	// has been missing from the desired set — the eviction grace (§4.4 of
	// METRICS-RESTART-CONSISTENCY) so a volume staged concurrently with a sweep is
	// not dropped on first sight. Guarded by mu.
	absent map[string]int
}

// TrackSpec is one volume's rehydration identity: everything Track needs to begin
// (or refresh) measuring it. Reconcile takes a slice of these, reconstructed from
// host truth, so the registry survives a plugin restart without a re-stage.
type TrackSpec struct {
	VolumeID    string
	StagingPath string
	AccessType  string
}

// NewRegistry builds a node-side stats registry. It owns a backend-lifetime
// context; call Close to stop all workers and the walk pool. A registry with
// Enabled=false is fully inert.
func NewRegistry(cfg Config, node string, log *zap.Logger) *Registry {
	cfg = cfg.withDefaults()
	if log == nil {
		log = zap.NewNop()
	}
	base, cancel := context.WithCancel(context.Background())
	r := &Registry{
		cfg:    cfg,
		node:   node,
		log:    log,
		statFn: mountutil.StatFS,
		nowFn:  time.Now,
		base:   base,
		cancel: cancel,
		vols:   map[string]*worker{},
		cache:  map[string]CSIVolumeStats{},
		absent: map[string]int{},
	}
	if cfg.Enabled && cfg.WalkEnabled {
		r.pool = newWalkPool(base, cfg.WalkWorkers)
	}
	return r
}

// Track begins (or, on re-stage, updates) background measurement of a volume
// mounted at stagingPath. Block volumes are recorded for presence only.
func (r *Registry) Track(volumeID, stagingPath, accessType string) {
	if r == nil || !r.cfg.Enabled || volumeID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if w, ok := r.vols[volumeID]; ok {
		w.setPath(stagingPath) // re-stage: keep the worker, update the path
		return
	}
	cur := r.cache[volumeID]
	cur.VolumeID = volumeID
	cur.Node = r.node
	cur.AccessType = accessType
	r.cache[volumeID] = cur

	if accessType == AccessBlock {
		return // no filesystem: presence only, no worker
	}
	ctx, cancel := context.WithCancel(r.base)
	w := &worker{r: r, id: volumeID, path: stagingPath, ctx: ctx, cancel: cancel}
	r.vols[volumeID] = w
	go w.run()
}

// Untrack stops measuring a volume and evicts its cached reading. It is
// non-blocking: it cancels the worker and returns; teardown is asynchronous.
func (r *Registry) Untrack(volumeID string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	w := r.vols[volumeID]
	delete(r.vols, volumeID)
	delete(r.cache, volumeID)
	r.mu.Unlock()
	if w != nil {
		w.cancel()
	}
}

// Reconcile makes the tracked set match the host-derived desired set. It Tracks
// every spec (idempotent — re-tracking keeps the worker and refreshes the path),
// so a plugin restart rehydrates the registry from host truth with no re-stage.
//
// When evict is true it also Untracks any currently-cached volume absent from
// desired — but only after it has been missing for graceSweeps consecutive
// Reconcile calls, so a volume staged concurrently with the caller's host
// enumeration is not dropped on first sight (it reappears next sweep). Callers
// pass evict=false when the host enumeration failed or was partial, so a transient
// error never drops live volumes (add-only). graceSweeps < 1 is treated as 1
// (evict on first confirmed absence). Returns the volume IDs it evicted.
//
// Nil/disabled registry: no-op returning nil.
func (r *Registry) Reconcile(desired []TrackSpec, evict bool, graceSweeps int) []string {
	if r == nil || !r.cfg.Enabled {
		return nil
	}
	want := make(map[string]struct{}, len(desired))
	for _, d := range desired {
		if d.VolumeID == "" {
			continue
		}
		want[d.VolumeID] = struct{}{}
		r.Track(d.VolumeID, d.StagingPath, d.AccessType) // idempotent; takes r.mu itself
	}
	if !evict {
		return nil
	}
	if graceSweeps < 1 {
		graceSweeps = 1
	}

	var toEvict []string
	r.mu.Lock()
	for id := range r.cache {
		if _, ok := want[id]; ok {
			delete(r.absent, id) // present again → reset its grace counter
			continue
		}
		r.absent[id]++
		if r.absent[id] >= graceSweeps {
			toEvict = append(toEvict, id)
		}
	}
	// Forget grace counters for volumes no longer cached (already gone).
	for id := range r.absent {
		if _, ok := r.cache[id]; !ok {
			delete(r.absent, id)
		}
	}
	r.mu.Unlock()

	for _, id := range toEvict {
		r.Untrack(id) // takes r.mu itself; also clears the cache entry
		r.mu.Lock()
		delete(r.absent, id)
		r.mu.Unlock()
	}
	return toEvict
}

// Get returns the cached reading for a volume.
func (r *Registry) Get(volumeID string) (CSIVolumeStats, bool) {
	if r == nil {
		return CSIVolumeStats{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.cache[volumeID]
	return s, ok
}

// Dump returns a snapshot of all cached readings.
func (r *Registry) Dump() []CSIVolumeStats {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]CSIVolumeStats, 0, len(r.cache))
	for _, s := range r.cache {
		out = append(out, s)
	}
	return out
}

// Close cancels the backend-lifetime context, stopping all workers and the walk
// pool. It does not wait for hung IO (a worker abandons its in-flight syscall).
func (r *Registry) Close() {
	if r == nil {
		return
	}
	r.cancel()
}

// update applies fn to a volume's cached reading under the registry lock. It is
// a no-op if the volume was Untracked, so a late worker write after unstage is
// dropped. fn must not perform IO (the lock is held).
func (r *Registry) update(volumeID string, fn func(*CSIVolumeStats)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.cache[volumeID]
	if !ok {
		return
	}
	fn(&s)
	r.cache[volumeID] = s
}

// worker is a per-volume supervisor. It never blocks on filesystem IO: it
// dispatches statfs/walk as bounded, abandonable operations and only ever
// selects on its timer and ctx, so it stays responsive to shutdown/Untrack even
// while the filesystem is hung.
type worker struct {
	r      *Registry
	id     string
	ctx    context.Context
	cancel context.CancelFunc

	mu          sync.Mutex
	path        string
	statfsFails int
	walkFails   int
	nextStatfs  time.Time
	nextWalk    time.Time

	statfsInFlight atomic.Bool // bounds leaked statfs goroutines to one per volume
	walkInFlight   atomic.Bool
	statfsOK       atomic.Bool // last statfs succeeded → walk gate
}

func (w *worker) getPath() string  { w.mu.Lock(); defer w.mu.Unlock(); return w.path }
func (w *worker) setPath(p string) { w.mu.Lock(); w.path = p; w.mu.Unlock() }

func (w *worker) run() {
	cfg := w.r.cfg
	now := w.r.nowFn()
	w.mu.Lock()
	w.nextStatfs = now.Add(jitterFor(w.id+"s", cfg.Interval))
	w.nextWalk = now.Add(jitterFor(w.id+"w", cfg.WalkInterval))
	w.mu.Unlock()

	gran := cfg.Interval
	if cfg.WalkInterval < gran {
		gran = cfg.WalkInterval
	}
	if gran <= 0 {
		gran = time.Second
	}
	t := time.NewTicker(gran)
	defer t.Stop()
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-t.C:
			now := w.r.nowFn()
			w.mu.Lock()
			doStatfs := !now.Before(w.nextStatfs)
			doWalk := cfg.WalkEnabled && w.r.pool != nil && !now.Before(w.nextWalk)
			w.mu.Unlock()
			if doStatfs {
				w.fireStatfs()
			}
			if doWalk {
				w.fireWalk()
			}
		}
	}
}

type statfsResult struct {
	u   mountutil.FSStats
	err error
}

// fireStatfs dispatches a statfs behind a watchdog and returns immediately. The
// real call runs detached and clears the single-flight flag only when it
// actually returns, so a permanently-hung mount leaks at most one goroutine for
// this volume (never one per tick). The watchdog abandons after StatfsTimeout.
func (w *worker) fireStatfs() {
	if !w.statfsInFlight.CompareAndSwap(false, true) {
		return // previous statfs still outstanding
	}
	path := w.getPath()
	timeout := w.r.cfg.StatfsTimeout
	ch := make(chan statfsResult, 1)
	go func() {
		u, err := w.r.statFn(path) // may hang on a dead mount
		w.statfsInFlight.Store(false)
		ch <- statfsResult{u, err}
	}()
	go func() {
		select {
		case res := <-ch:
			w.onStatfs(res.u, res.err)
		case <-time.After(timeout):
			w.onStatfs(mountutil.FSStats{}, fmt.Errorf("statfs timed out after %s", timeout))
		case <-w.ctx.Done():
		}
	}()
}

func (w *worker) onStatfs(u mountutil.FSStats, err error) {
	cfg := w.r.cfg
	now := w.r.nowFn()
	w.mu.Lock()
	if err != nil {
		w.statfsFails++
		w.nextStatfs = now.Add(backoff(cfg.Interval, cfg.MaxFailureBackoff, w.statfsFails))
	} else {
		w.statfsFails = 0
		w.nextStatfs = now.Add(cfg.Interval)
	}
	w.mu.Unlock()
	w.statfsOK.Store(err == nil)
	w.r.update(w.id, func(s *CSIVolumeStats) {
		if err != nil {
			s.LastError = err.Error() // keep last-good measurements
			return
		}
		s.TotalBytes, s.UsedBytes, s.AvailableBytes = u.TotalBytes, u.UsedBytes, u.AvailableBytes
		s.TotalInodes, s.UsedInodes, s.FreeInodes = u.TotalInodes, u.UsedInodes, u.FreeInodes
		s.StatfsAt = now
		s.LastError = ""
	})
}

// fireWalk submits a tree walk to the shared pool, gated on a healthy statfs and
// single-flighted per volume. Runs entirely detached from the supervisor.
func (w *worker) fireWalk() {
	if !w.statfsOK.Load() { // mount not known-healthy: skip
		return
	}
	if !w.walkInFlight.CompareAndSwap(false, true) {
		return
	}
	cfg := w.r.cfg
	path := w.getPath()
	go func() {
		defer w.walkInFlight.Store(false)
		ctx, cancel := context.WithTimeout(w.ctx, cfg.WalkTimeout)
		defer cancel()
		start := w.r.nowFn()
		res := w.r.pool.walk(ctx, path)
		now := w.r.nowFn()
		dur := now.Sub(start)
		w.mu.Lock()
		if res.err != nil {
			w.walkFails++
			w.nextWalk = now.Add(backoff(cfg.WalkInterval, cfg.MaxFailureBackoff, w.walkFails))
		} else {
			w.walkFails = 0
			w.nextWalk = now.Add(cfg.WalkInterval)
		}
		w.mu.Unlock()
		w.r.update(w.id, func(s *CSIVolumeStats) {
			if res.err != nil {
				s.LastError = res.err.Error() // keep last-good counts
				return
			}
			s.FileCount, s.DirCount, s.OtherCount = res.files, res.dirs, res.other
			s.WalkAt = now
			s.WalkDuration = dur
			s.WalkComplete = true
			s.LastError = ""
		})
	}()
}

// backoff returns base*2^(fails-1), capped at max. fails<=1 returns base.
func backoff(base, max time.Duration, fails int) time.Duration {
	d := base
	for i := 1; i < fails && d < max; i++ {
		d *= 2
	}
	if d > max {
		d = max
	}
	return d
}

// jitterFor spreads a worker's first tick deterministically across [0, d) by
// hashing the key, so many volumes staged together don't align on one boundary.
func jitterFor(key string, d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return time.Duration(h.Sum64() % uint64(d))
}
