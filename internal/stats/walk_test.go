package stats

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// newPool starts a walk pool bound to the test's lifetime.
func newPool(t *testing.T, workers int) *walkPool {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return newWalkPool(ctx, workers)
}

func TestWalk_CountsFilesDirsOther(t *testing.T) {
	root := t.TempDir()
	// root/a.txt root/b.txt root/sub1/{c.txt, nested/d.txt} root/sub2 root/link->a.txt
	mustWrite(t, filepath.Join(root, "a.txt"))
	mustWrite(t, filepath.Join(root, "b.txt"))
	mustMkdir(t, filepath.Join(root, "sub1"))
	mustWrite(t, filepath.Join(root, "sub1", "c.txt"))
	mustMkdir(t, filepath.Join(root, "sub1", "nested"))
	mustWrite(t, filepath.Join(root, "sub1", "nested", "d.txt"))
	mustMkdir(t, filepath.Join(root, "sub2"))
	if err := os.Symlink(filepath.Join(root, "a.txt"), filepath.Join(root, "link")); err != nil {
		t.Fatalf("symlink: %v", err)
	}

	res := newPool(t, 4).walk(context.Background(), root)
	if res.err != nil {
		t.Fatalf("walk err: %v", res.err)
	}
	if res.files != 4 || res.dirs != 3 || res.other != 1 {
		t.Fatalf("counts = files %d dirs %d other %d; want 4/3/1", res.files, res.dirs, res.other)
	}
}

func TestWalk_Empty(t *testing.T) {
	res := newPool(t, 2).walk(context.Background(), t.TempDir())
	if res.err != nil || res.files != 0 || res.dirs != 0 || res.other != 0 {
		t.Fatalf("empty walk = files %d dirs %d other %d err %v; want 0/0/0/nil", res.files, res.dirs, res.other, res.err)
	}
}

func TestWalk_DeepNesting(t *testing.T) {
	root := t.TempDir()
	const depth = 200
	p := root
	for i := 0; i < depth; i++ {
		p = filepath.Join(p, "d"+strconv.Itoa(i))
	}
	if err := os.MkdirAll(p, 0o755); err != nil {
		t.Fatalf("mkdirall: %v", err)
	}
	mustWrite(t, filepath.Join(p, "leaf.txt"))

	res := newPool(t, 4).walk(context.Background(), root)
	if res.err != nil {
		t.Fatalf("walk err: %v", res.err)
	}
	if res.dirs != depth || res.files != 1 {
		t.Fatalf("deep walk = dirs %d files %d; want %d/1", res.dirs, res.files, depth)
	}
}

func TestWalk_MissingRootErrors(t *testing.T) {
	res := newPool(t, 2).walk(context.Background(), filepath.Join(t.TempDir(), "does-not-exist"))
	if res.err == nil {
		t.Fatal("expected error walking a missing root")
	}
}

func TestWalk_CancelledContext(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 50; i++ {
		mustMkdir(t, filepath.Join(root, "d"+strconv.Itoa(i)))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	res := newPool(t, 4).walk(ctx, root)
	if res.err == nil {
		t.Fatal("expected ctx error from a cancelled walk")
	}
}

func TestWalk_PermissionDeniedAggregates(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: chmod 000 is still readable")
	}
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "ok.txt"))
	denied := filepath.Join(root, "denied")
	mustMkdir(t, denied)
	mustWrite(t, filepath.Join(denied, "hidden.txt"))
	if err := os.Chmod(denied, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(denied, 0o755) })

	res := newPool(t, 4).walk(context.Background(), root)
	if res.err == nil {
		t.Fatal("expected an aggregated error for the unreadable subdir")
	}
	// The readable file + the denied dir itself are still counted.
	if res.files != 1 || res.dirs != 1 {
		t.Fatalf("partial counts = files %d dirs %d; want 1/1", res.files, res.dirs)
	}
}

func mustWrite(t *testing.T, path string) {
	t.Helper()
	if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
}
