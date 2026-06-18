package stats

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
)

// dormantCfg keeps the background ticker effectively idle (hour-scale cadences)
// so tests can drive fireStatfs/fireWalk by hand and assert deterministically.
func dormantCfg() Config {
	return Config{
		Enabled:       true,
		WalkEnabled:   true,
		Interval:      time.Hour,
		WalkInterval:  time.Hour,
		StatfsTimeout: 100 * time.Millisecond,
		WalkTimeout:   5 * time.Second,
	}
}

func newReg(t *testing.T, cfg Config) *Registry {
	t.Helper()
	r := NewRegistry(cfg, "nodeA", zap.NewNop())
	t.Cleanup(r.Close)
	return r
}

func getWorker(r *Registry, id string) *worker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.vols[id]
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}

func okStatFn(u mountutil.FSStats) statFunc {
	return func(string) (mountutil.FSStats, error) { return u, nil }
}

func TestRegistry_DisabledIsNoOp(t *testing.T) {
	r := newReg(t, Config{Enabled: false})
	r.Track("v", "/p", AccessMount)
	if _, ok := r.Get("v"); ok {
		t.Fatal("disabled registry should not track")
	}
	if len(r.Dump()) != 0 {
		t.Fatal("disabled registry should have empty dump")
	}
}

func TestRegistry_NilSafe(t *testing.T) {
	var r *Registry
	r.Track("v", "/p", AccessMount) // must not panic
	r.Untrack("v")
	if _, ok := r.Get("v"); ok {
		t.Fatal("nil registry Get should be false")
	}
	if r.Dump() != nil {
		t.Fatal("nil registry Dump should be nil")
	}
	r.Close()
}

func TestRegistry_BlockVolumePresenceOnly(t *testing.T) {
	r := newReg(t, dormantCfg())
	r.Track("blk", "/dev/whatever", AccessBlock)
	s, ok := r.Get("blk")
	if !ok || s.AccessType != AccessBlock {
		t.Fatalf("block volume = %+v ok=%v; want present, access=block", s, ok)
	}
	if getWorker(r, "blk") != nil {
		t.Fatal("block volume should not start a worker")
	}
}

func TestRegistry_StatfsSuccessPopulatesCache(t *testing.T) {
	r := newReg(t, dormantCfg())
	r.statFn = okStatFn(mountutil.FSStats{
		TotalBytes: 100, UsedBytes: 40, AvailableBytes: 60,
		TotalInodes: 10, UsedInodes: 4, FreeInodes: 6,
	})
	r.Track("v", "/p", AccessMount)
	getWorker(r, "v").fireStatfs()

	waitFor(t, time.Second, func() bool { s, _ := r.Get("v"); return !s.StatfsAt.IsZero() })
	s, _ := r.Get("v")
	if s.TotalBytes != 100 || s.UsedBytes != 40 || s.FreeInodes != 6 || s.LastError != "" {
		t.Fatalf("cache = %+v; want bytes 100/40, free inodes 6, no error", s)
	}
}

func TestRegistry_StatfsErrorKeepsLastGood(t *testing.T) {
	r := newReg(t, dormantCfg())
	r.statFn = okStatFn(mountutil.FSStats{TotalBytes: 100, UsedBytes: 40})
	r.Track("v", "/p", AccessMount)
	w := getWorker(r, "v")
	w.fireStatfs()
	waitFor(t, time.Second, func() bool { s, _ := r.Get("v"); return s.TotalBytes == 100 })

	// Now fail: last-good measurements must persist, with LastError set.
	r.statFn = func(string) (mountutil.FSStats, error) { return mountutil.FSStats{}, errors.New("boom") }
	w.fireStatfs()
	waitFor(t, time.Second, func() bool { s, _ := r.Get("v"); return s.LastError != "" })
	s, _ := r.Get("v")
	if s.TotalBytes != 100 || s.UsedBytes != 40 {
		t.Fatalf("last-good lost on error: %+v", s)
	}
}

func TestRegistry_StatfsWatchdogAndSingleFlight(t *testing.T) {
	cfg := dormantCfg()
	cfg.StatfsTimeout = 40 * time.Millisecond
	r := newReg(t, cfg)

	block := make(chan struct{})
	var calls int32
	r.statFn = func(string) (mountutil.FSStats, error) {
		atomic.AddInt32(&calls, 1)
		<-block // hang until released
		return mountutil.FSStats{}, nil
	}
	r.Track("v", "/p", AccessMount)
	w := getWorker(r, "v")

	w.fireStatfs() // call 1: hangs; watchdog should abandon after 40ms
	waitFor(t, time.Second, func() bool { s, _ := r.Get("v"); return s.LastError != "" })
	s, _ := r.Get("v")
	if s.LastError == "" {
		t.Fatal("watchdog did not record a timeout error")
	}

	// While the first statfs is still hung, further fires must be single-flighted
	// (no new statFn invocation) — bounding leaked goroutines to one.
	w.fireStatfs()
	w.fireStatfs()
	time.Sleep(80 * time.Millisecond)
	if n := atomic.LoadInt32(&calls); n != 1 {
		t.Fatalf("single-flight violated: statFn called %d times, want 1", n)
	}

	close(block) // the hung call returns and clears the in-flight flag
	waitFor(t, time.Second, func() bool { return !w.statfsInFlight.Load() })
	w.fireStatfs() // now a fresh call is allowed
	waitFor(t, time.Second, func() bool { return atomic.LoadInt32(&calls) == 2 })
}

func TestRegistry_StatfsFailureCountAndRecovery(t *testing.T) {
	r := newReg(t, dormantCfg())
	r.statFn = okStatFn(mountutil.FSStats{TotalBytes: 1})
	r.Track("v", "/p", AccessMount)
	w := getWorker(r, "v")

	w.onStatfs(mountutil.FSStats{}, errors.New("x"))
	w.onStatfs(mountutil.FSStats{}, errors.New("x"))
	w.mu.Lock()
	fails := w.statfsFails
	w.mu.Unlock()
	if fails != 2 || w.statfsOK.Load() {
		t.Fatalf("after 2 failures: fails=%d statfsOK=%v; want 2/false", fails, w.statfsOK.Load())
	}

	w.onStatfs(mountutil.FSStats{TotalBytes: 1}, nil) // recover
	w.mu.Lock()
	fails = w.statfsFails
	w.mu.Unlock()
	if fails != 0 || !w.statfsOK.Load() {
		t.Fatalf("after recovery: fails=%d statfsOK=%v; want 0/true", fails, w.statfsOK.Load())
	}
}

func TestRegistry_WalkPopulatesCounts(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, root+"/a.txt")
	mustWrite(t, root+"/b.txt")
	mustMkdir(t, root+"/sub")

	r := newReg(t, dormantCfg())
	r.statFn = okStatFn(mountutil.FSStats{TotalBytes: 1})
	r.Track("v", root, AccessMount)
	w := getWorker(r, "v")
	w.fireStatfs() // walk is gated on a healthy statfs
	waitFor(t, time.Second, func() bool { return w.statfsOK.Load() })
	w.fireWalk()

	waitFor(t, 2*time.Second, func() bool { s, _ := r.Get("v"); return s.WalkComplete })
	s, _ := r.Get("v")
	if s.FileCount != 2 || s.DirCount != 1 {
		t.Fatalf("walk counts = files %d dirs %d; want 2/1", s.FileCount, s.DirCount)
	}
}

func TestRegistry_UntrackEvictsAndDropsLateWrites(t *testing.T) {
	r := newReg(t, dormantCfg())
	r.statFn = okStatFn(mountutil.FSStats{TotalBytes: 1})
	r.Track("v", "/p", AccessMount)
	w := getWorker(r, "v")

	r.Untrack("v")
	if _, ok := r.Get("v"); ok {
		t.Fatal("Untrack should evict the cache entry")
	}
	// A late worker write after Untrack must be dropped (no re-add, no panic).
	w.onStatfs(mountutil.FSStats{TotalBytes: 1}, nil)
	if _, ok := r.Get("v"); ok {
		t.Fatal("late write after Untrack must not re-add the volume")
	}
}

func TestRegistry_RecoversViaRunLoop(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, root+"/a.txt")

	cfg := Config{
		Enabled:       true,
		WalkEnabled:   false, // isolate statfs recovery
		Interval:      15 * time.Millisecond,
		WalkInterval:  time.Hour,
		StatfsTimeout: time.Second,
		StaleAfter:    time.Second,
	}
	r := newReg(t, cfg)

	var calls int32
	r.statFn = func(string) (mountutil.FSStats, error) {
		if atomic.AddInt32(&calls, 1) <= 3 {
			return mountutil.FSStats{}, errors.New("backend down")
		}
		return mountutil.FSStats{TotalBytes: 500}, nil
	}
	r.Track("v", root, AccessMount)

	// The run loop should retry (with backoff) and recover automatically once the
	// fake backend "comes back".
	waitFor(t, 5*time.Second, func() bool {
		s, _ := r.Get("v")
		return s.TotalBytes == 500 && s.LastError == ""
	})
}

func TestRegistry_ConcurrentTrackUntrackRace(t *testing.T) {
	r := newReg(t, dormantCfg())
	r.statFn = okStatFn(mountutil.FSStats{TotalBytes: 1})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			vid := "v" + string(rune('a'+id))
			for j := 0; j < 100; j++ {
				r.Track(vid, "/p", AccessMount)
				_, _ = r.Get(vid)
				_ = r.Dump()
				r.Untrack(vid)
			}
		}(i)
	}
	wg.Wait()
}
