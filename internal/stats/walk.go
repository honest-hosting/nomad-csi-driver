package stats

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
)

// unknownDirentType is what os.ReadDir reports for a DT_UNKNOWN entry (all mode
// bits set). It MUST be checked before IsDir/IsRegular, since the all-bits value
// would otherwise read as a directory. (Verified against os/dirent_linux.go.)
const unknownDirentType = ^fs.FileMode(0)

// walkPool is a process-wide, fixed-size pool of directory-traversal workers
// shared by every volume's walk. Sizing it (WalkWorkers) is the single knob that
// bounds concurrent readdir across all volumes. Work items carry their owning
// job, so volumes' directories interleave fairly in one queue.
//
// The backlog is an unbounded in-memory slice (directory paths only — cheap),
// NOT a bounded channel: workers are both producers (enqueue subdirs) and
// consumers, so a bounded channel could deadlock if it filled and every worker
// blocked on enqueue. submit never blocks.
type walkPool struct {
	mu     sync.Mutex
	cond   *sync.Cond
	queue  []walkItem
	closed bool
}

type walkItem struct {
	dir string
	job *walkJob
}

// walkJob accumulates one volume's traversal. wg counts outstanding directory
// items (Add before each submit, Done when processed); it reaching zero means
// the walk finished (or drained after ctx cancellation).
type walkJob struct {
	ctx                context.Context
	files, dirs, other int64 // atomic
	wg                 sync.WaitGroup
	errs               errAgg
}

func newWalkPool(ctx context.Context, workers int) *walkPool {
	if workers <= 0 {
		workers = DefaultWalkWorkers
	}
	p := &walkPool{}
	p.cond = sync.NewCond(&p.mu)
	for i := 0; i < workers; i++ {
		go p.run()
	}
	// Close the pool when the backend-lifetime context is cancelled.
	go func() {
		<-ctx.Done()
		p.mu.Lock()
		p.closed = true
		p.cond.Broadcast()
		p.mu.Unlock()
	}()
	return p
}

// submit appends an item without blocking. If the pool is closed it balances the
// caller's wg.Add(1) immediately so pending walks unblock.
func (p *walkPool) submit(it walkItem) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		it.job.wg.Done()
		return
	}
	p.queue = append(p.queue, it)
	p.cond.Signal()
	p.mu.Unlock()
}

func (p *walkPool) run() {
	for {
		p.mu.Lock()
		for len(p.queue) == 0 && !p.closed {
			p.cond.Wait()
		}
		if p.closed {
			rest := p.queue
			p.queue = nil
			p.mu.Unlock()
			for _, it := range rest { // release waiters so walks complete promptly
				it.job.wg.Done()
			}
			return
		}
		it := p.queue[0]
		p.queue = p.queue[1:]
		p.mu.Unlock()
		p.process(it)
	}
}

func (p *walkPool) process(it walkItem) {
	j := it.job
	defer j.wg.Done()
	if j.ctx.Err() != nil { // cancelled / timed out: drop fast
		return
	}
	entries, err := os.ReadDir(it.dir)
	if err != nil {
		j.errs.add(err)
		return
	}
	for _, de := range entries {
		if j.ctx.Err() != nil {
			return
		}
		switch classify(de) {
		case kindDir:
			atomic.AddInt64(&j.dirs, 1)
			j.wg.Add(1)
			p.submit(walkItem{dir: filepath.Join(it.dir, de.Name()), job: j})
		case kindFile:
			atomic.AddInt64(&j.files, 1)
		default:
			atomic.AddInt64(&j.other, 1)
		}
	}
}

// walkResult is the outcome of one volume walk: partial counts plus an error if
// the deadline tripped or directories failed to read.
type walkResult struct {
	files, dirs, other int64
	err                error
}

// walk traverses root through the shared pool, bounded by ctx (the per-volume
// WalkTimeout). It blocks the calling goroutine (a detached worker goroutine,
// never the supervisor) until the traversal finishes or drains after ctx.
func (p *walkPool) walk(ctx context.Context, root string) walkResult {
	j := &walkJob{ctx: ctx}
	j.wg.Add(1)
	p.submit(walkItem{dir: root, job: j})
	j.wg.Wait()
	res := walkResult{
		files: atomic.LoadInt64(&j.files),
		dirs:  atomic.LoadInt64(&j.dirs),
		other: atomic.LoadInt64(&j.other),
	}
	if ctx.Err() != nil {
		res.err = ctx.Err()
	} else if e := j.errs.err(); e != nil {
		res.err = e
	}
	return res
}

type kind int

const (
	kindOther kind = iota
	kindFile
	kindDir
)

// classify buckets a directory entry by type using the cheap d_type from
// readdir; it only falls back to an Lstat (Info) when d_type is unknown — so on
// ext4/xfs there are zero per-file stat syscalls.
func classify(de os.DirEntry) kind {
	t := de.Type()
	if t == unknownDirentType { // DT_UNKNOWN: must precede IsDir/IsRegular
		info, err := de.Info()
		if err != nil {
			return kindOther
		}
		m := info.Mode()
		switch {
		case m.IsDir():
			return kindDir
		case m.IsRegular():
			return kindFile
		default:
			return kindOther
		}
	}
	switch {
	case t.IsDir():
		return kindDir
	case t.IsRegular():
		return kindFile
	default:
		return kindOther
	}
}

// errAgg collects non-fatal per-directory errors (e.g. permission denied),
// retaining a bounded sample so one unreadable subtree doesn't balloon memory.
type errAgg struct {
	mu    sync.Mutex
	errs  []error
	total int
}

const maxRetainedWalkErrors = 16

func (a *errAgg) add(err error) {
	a.mu.Lock()
	a.total++
	if len(a.errs) < maxRetainedWalkErrors {
		a.errs = append(a.errs, err)
	}
	a.mu.Unlock()
}

func (a *errAgg) err() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.total == 0 {
		return nil
	}
	if a.total == 1 {
		return a.errs[0]
	}
	msgs := make([]string, 0, len(a.errs))
	for _, e := range a.errs {
		msgs = append(msgs, e.Error())
	}
	suffix := ""
	if a.total > len(a.errs) {
		suffix = fmt.Sprintf(" (+%d more)", a.total-len(a.errs))
	}
	return fmt.Errorf("%d walk errors: %s%s", a.total, strings.Join(msgs, "; "), suffix)
}
