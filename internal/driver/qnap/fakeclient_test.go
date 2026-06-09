package qnap

import (
	"context"
	"sync"

	goqnap "github.com/honest-hosting/go-qnap"
)

// fakeClient is an in-memory stand-in for the go-qnap client. It models LUNs,
// targets, snapshots, and pools well enough to exercise the controller's
// lifecycle logic without an appliance.
type fakeClient struct {
	mu sync.Mutex

	luns    map[int]goqnap.LUN
	targets map[int]goqnap.Target
	snaps   map[int64]goqnap.Snapshot
	snapLUN map[int64]int // snapshot id -> source LUN index
	pools   map[int]goqnap.Pool

	nextLUN    int
	nextTarget int
	nextSnap   int64

	// lastBlockReq captures the most recent CreateBlockLUN request so tests can
	// assert on provisioning params (e.g. Thin).
	lastBlockReq goqnap.CreateBlockLUNRequest

	// ghosts[idx] simulates a LUN removed out-of-band: GetLUN returns a ghost row
	// (empty name) and DeleteLUN errors -1. See makeGhost.
	ghosts map[int]bool

	// Injection points for error paths.
	createLUNErr error
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		luns:    map[int]goqnap.LUN{},
		targets: map[int]goqnap.Target{},
		snaps:   map[int64]goqnap.Snapshot{},
		pools:   map[int]goqnap.Pool{1: {ID: 1, Name: "pool1", FreeBytes: 1 << 42}},
	}
}

func notFound(op string) error {
	return &goqnap.APIError{Op: op, Code: -22, Message: "not found"}
}

func (f *fakeClient) Login(context.Context, string, string, ...goqnap.LoginOption) (goqnap.Session, error) {
	return goqnap.Session{SID: "fake-sid"}, nil
}
func (f *fakeClient) Validate(context.Context, goqnap.Session) (bool, error) { return true, nil }

func (f *fakeClient) CreateBlockLUN(_ context.Context, _ goqnap.Session, req goqnap.CreateBlockLUNRequest) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastBlockReq = req
	if f.createLUNErr != nil {
		return 0, f.createLUNErr
	}
	idx := f.nextLUN
	f.nextLUN++
	f.luns[idx] = goqnap.LUN{
		Index: idx, Name: req.Name, PoolID: req.PoolID,
		CapacityBytes: int64(req.SizeGB) * giB, SectorSize: req.SectorSize, Status: goqnap.LUNStatusReady,
	}
	if req.TargetIndex != nil {
		f.mapLocked(idx, *req.TargetIndex)
	}
	return idx, nil
}

func (f *fakeClient) mapLocked(lunIdx, targetIdx int) {
	t := f.targets[targetIdx]
	t.LUNs = append(t.LUNs, lunIdx)
	f.targets[targetIdx] = t
	l := f.luns[lunIdx]
	l.Mapped = true
	f.luns[lunIdx] = l
}

func (f *fakeClient) GetLUN(_ context.Context, _ goqnap.Session, idx int) (goqnap.LUN, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ghosts[idx] {
		// QuTScloud returns a ghost row (empty name, 0 capacity) for a removed index.
		return goqnap.LUN{Index: idx}, nil
	}
	l, ok := f.luns[idx]
	if !ok {
		return goqnap.LUN{}, notFound("GetLUN")
	}
	return l, nil
}

// makeGhost simulates a LUN that was removed out-of-band: remove_lun now errors
// (like QNAP's -1) and GetLUN returns a ghost row.
func (f *fakeClient) makeGhost(idx int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ghosts == nil {
		f.ghosts = map[int]bool{}
	}
	f.ghosts[idx] = true
}

// replaceLUN simulates LUN-index reuse: a different volume's LUN now occupies idx.
func (f *fakeClient) replaceLUN(idx int, name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.luns[idx] = goqnap.LUN{Index: idx, Name: name, CapacityBytes: giB}
}

func (f *fakeClient) lunByIndex(idx int) (goqnap.LUN, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	l, ok := f.luns[idx]
	return l, ok
}

func (f *fakeClient) ListLUNs(_ context.Context, _ goqnap.Session) ([]goqnap.LUN, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]goqnap.LUN, 0, len(f.luns))
	for _, l := range f.luns {
		out = append(out, l)
	}
	return out, nil
}

func (f *fakeClient) DeleteLUN(_ context.Context, _ goqnap.Session, idx int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ghosts[idx] {
		// QNAP's remove_lun on a removed/ghost index returns -1 (which go-qnap
		// surfaces as an APIError, not "invalid username or password").
		return &goqnap.APIError{Op: "DeleteLUN", Code: -1}
	}
	if _, ok := f.luns[idx]; !ok {
		return notFound("DeleteLUN")
	}
	delete(f.luns, idx)
	return nil
}

func (f *fakeClient) UnmapLUN(_ context.Context, _ goqnap.Session, lunIdx, targetIdx int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.targets[targetIdx]
	if !ok {
		return nil
	}
	kept := t.LUNs[:0]
	for _, l := range t.LUNs {
		if l != lunIdx {
			kept = append(kept, l)
		}
	}
	t.LUNs = kept
	f.targets[targetIdx] = t
	return nil
}

func (f *fakeClient) WaitForLUNGone(_ context.Context, _ goqnap.Session, idx int, _ goqnap.WaitOptions) error {
	return nil
}

func (f *fakeClient) ResizeLUN(_ context.Context, _ goqnap.Session, idx, gb int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	l, ok := f.luns[idx]
	if !ok {
		return notFound("ResizeLUN")
	}
	l.CapacityBytes = int64(gb) * giB
	f.luns[idx] = l
	return nil
}

func (f *fakeClient) WaitForResizeComplete(_ context.Context, _ goqnap.Session, idx int, _ int64, _ goqnap.WaitOptions) (goqnap.LUN, error) {
	return f.GetLUN(context.Background(), goqnap.Session{}, idx)
}

func (f *fakeClient) CreateTarget(_ context.Context, _ goqnap.Session, req goqnap.CreateTargetRequest) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := f.nextTarget
	f.nextTarget++
	f.targets[idx] = goqnap.Target{Index: idx, Name: req.Name, Alias: req.Alias, IQN: "iqn.2004-04.com.qnap:" + req.Name}
	return idx, nil
}

func (f *fakeClient) GetTarget(_ context.Context, _ goqnap.Session, idx int) (goqnap.Target, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.targets[idx]
	if !ok {
		return goqnap.Target{}, notFound("GetTarget")
	}
	return t, nil
}

func (f *fakeClient) ListTargets(_ context.Context, _ goqnap.Session) ([]goqnap.Target, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]goqnap.Target, 0, len(f.targets))
	for _, t := range f.targets {
		out = append(out, t)
	}
	return out, nil
}

func (f *fakeClient) DeleteTarget(_ context.Context, _ goqnap.Session, idx int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.targets, idx)
	return nil
}

func (f *fakeClient) ListPools(_ context.Context, _ goqnap.Session) ([]goqnap.Pool, error) {
	return []goqnap.Pool{f.pools[1]}, nil
}

func (f *fakeClient) GetPool(_ context.Context, _ goqnap.Session, id int) (goqnap.Pool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.pools[id]
	if !ok {
		return goqnap.Pool{}, notFound("GetPool")
	}
	return p, nil
}

func (f *fakeClient) CreateSnapshot(_ context.Context, _ goqnap.Session, lunIdx int, req goqnap.CreateSnapshotRequest) (goqnap.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.nextSnap + 1000
	f.nextSnap++
	snap := goqnap.Snapshot{ID: id, Name: req.Name, Status: goqnap.SnapshotStatusReady, ExpireMin: req.ExpireMin, Vital: req.Vital, CreatedAt: "Fri May 29 10:13:33 2026"}
	if l, ok := f.luns[lunIdx]; ok {
		snap.SizeBytes = l.CapacityBytes
	}
	f.snaps[id] = snap
	if f.snapLUN == nil {
		f.snapLUN = map[int64]int{}
	}
	f.snapLUN[id] = lunIdx
	return snap, nil
}

func (f *fakeClient) GetSnapshot(_ context.Context, _ goqnap.Session, id int64) (goqnap.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.snaps[id]
	if !ok {
		return goqnap.Snapshot{}, notFound("GetSnapshot")
	}
	return s, nil
}

func (f *fakeClient) ListSnapshots(_ context.Context, _ goqnap.Session, lunIdx int) ([]goqnap.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]goqnap.Snapshot, 0, len(f.snaps))
	for id, s := range f.snaps {
		if f.snapLUN[id] == lunIdx {
			out = append(out, s)
		}
	}
	return out, nil
}

func (f *fakeClient) DeleteSnapshot(_ context.Context, _ goqnap.Session, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.snaps, id)
	return nil
}

func (f *fakeClient) WaitForSnapshotGone(_ context.Context, _ goqnap.Session, lunIdx int, id int64, _ goqnap.WaitOptions) error {
	return nil
}

func (f *fakeClient) CreateLUNFromSnapshot(_ context.Context, _ goqnap.Session, req goqnap.CreateLUNFromSnapshotRequest) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := f.nextLUN
	f.nextLUN++
	size := giB
	if s, ok := f.snaps[req.SnapshotID]; ok && s.SizeBytes > 0 {
		size = s.SizeBytes
	}
	f.luns[idx] = goqnap.LUN{Index: idx, Name: req.Name, PoolID: req.PoolID, CapacityBytes: size, Status: goqnap.LUNStatusReady}
	if req.TargetIndex != nil {
		f.mapLocked(idx, *req.TargetIndex)
	}
	return idx, nil
}

func (f *fakeClient) CloneLUN(_ context.Context, _ goqnap.Session, req goqnap.CloneLUNRequest) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	idx := f.nextLUN
	f.nextLUN++
	size := giB
	if src, ok := f.luns[req.SourceLUNIndex]; ok {
		size = src.CapacityBytes
	}
	f.luns[idx] = goqnap.LUN{Index: idx, Name: req.Name, PoolID: req.PoolID, CapacityBytes: size, Status: goqnap.LUNStatusReady}
	if req.TargetIndex != nil {
		f.mapLocked(idx, *req.TargetIndex)
	}
	return idx, nil
}
