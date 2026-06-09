package mountutil

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
)

// responder builds a FakeRunner Responder from per-command behavior.
type cmdResult struct {
	stdout string
	err    error
}

func runnerFor(results map[string]cmdResult) *cexec.FakeRunner {
	return &cexec.FakeRunner{
		Responder: func(c cexec.Command) (cexec.Output, error) {
			if r, ok := results[c.Name]; ok {
				return cexec.Output{Stdout: []byte(r.stdout)}, r.err
			}
			return cexec.Output{}, nil
		},
	}
}

func ctx() context.Context { return context.Background() }

type fakeMountMetrics struct {
	ops     []string // "op:outcome"
	skipped int
}

func (f *fakeMountMetrics) MountOp(op, outcome string, _ time.Duration) {
	f.ops = append(f.ops, op+":"+outcome)
}
func (f *fakeMountMetrics) FormatSkipped() { f.skipped++ }
func (f *fakeMountMetrics) has(s string) bool {
	for _, o := range f.ops {
		if o == s {
			return true
		}
	}
	return false
}

func TestMounterMetrics(t *testing.T) {
	t.Run("format ok", func(t *testing.T) {
		fm := &fakeMountMetrics{}
		fr := runnerFor(map[string]cmdResult{"blkid": {err: &cexec.Error{Name: "blkid", ExitCode: 2}}})
		_, err := New(fr, nil).WithMetrics(fm).FormatIfEmpty(ctx(), "/dev/x", "ext4", nil)
		require.NoError(t, err)
		assert.True(t, fm.has("format:ok"), "got %v", fm.ops)
		assert.Equal(t, 0, fm.skipped)
	})
	t.Run("format skipped is the safety counter, not a mount op", func(t *testing.T) {
		fm := &fakeMountMetrics{}
		fr := runnerFor(map[string]cmdResult{"blkid": {stdout: "ext4"}})
		_, err := New(fr, nil).WithMetrics(fm).FormatIfEmpty(ctx(), "/dev/x", "ext4", nil)
		require.NoError(t, err)
		assert.Equal(t, 1, fm.skipped)
		assert.Empty(t, fm.ops)
	})
	t.Run("format refused (different fs) is a distinct outcome", func(t *testing.T) {
		fm := &fakeMountMetrics{}
		fr := runnerFor(map[string]cmdResult{"blkid": {stdout: "xfs"}})
		_, err := New(fr, nil).WithMetrics(fm).FormatIfEmpty(ctx(), "/dev/x", "ext4", nil)
		require.Error(t, err)
		assert.True(t, fm.has("format:refused"), "got %v", fm.ops)
	})
	t.Run("mount error", func(t *testing.T) {
		fm := &fakeMountMetrics{}
		fr := runnerFor(map[string]cmdResult{
			"findmnt": {err: &cexec.Error{Name: "findmnt", ExitCode: 1}}, // not mounted
			"mount":   {err: &cexec.Error{Name: "mount", ExitCode: 32}},
		})
		err := New(fr, nil).WithMetrics(fm).Mount(ctx(), "/dev/x", filepath.Join(t.TempDir(), "s"), "ext4", nil)
		require.Error(t, err)
		assert.True(t, fm.has("mount:error"), "got %v", fm.ops)
	})
	t.Run("unmount ok (idempotent)", func(t *testing.T) {
		fm := &fakeMountMetrics{}
		fr := runnerFor(map[string]cmdResult{"findmnt": {err: &cexec.Error{Name: "findmnt", ExitCode: 1}}})
		require.NoError(t, New(fr, nil).WithMetrics(fm).Unmount(ctx(), filepath.Join(t.TempDir(), "s")))
		assert.True(t, fm.has("unmount:ok"), "got %v", fm.ops)
	})
}

func TestDetectFilesystem(t *testing.T) {
	t.Run("formatted", func(t *testing.T) {
		fr := runnerFor(map[string]cmdResult{"blkid": {stdout: "ext4\n"}})
		fs, err := New(fr, nil).DetectFilesystem(ctx(), "/dev/x")
		require.NoError(t, err)
		assert.Equal(t, "ext4", fs)
	})
	t.Run("empty device (blkid exit 2)", func(t *testing.T) {
		fr := runnerFor(map[string]cmdResult{"blkid": {err: &cexec.Error{Name: "blkid", ExitCode: 2}}})
		fs, err := New(fr, nil).DetectFilesystem(ctx(), "/dev/x")
		require.NoError(t, err)
		assert.Equal(t, "", fs)
	})
}

func TestFormatIfEmpty(t *testing.T) {
	t.Run("empty -> formats", func(t *testing.T) {
		fr := runnerFor(map[string]cmdResult{"blkid": {err: &cexec.Error{Name: "blkid", ExitCode: 2}}})
		formatted, err := New(fr, nil).FormatIfEmpty(ctx(), "/dev/x", "ext4", nil)
		require.NoError(t, err)
		assert.True(t, formatted)
		assert.Contains(t, strings.Join(fr.Commands(), "|"), "mkfs.ext4 -F /dev/x")
	})

	t.Run("already same fs -> skips mkfs (idempotent)", func(t *testing.T) {
		fr := runnerFor(map[string]cmdResult{"blkid": {stdout: "ext4"}})
		formatted, err := New(fr, nil).FormatIfEmpty(ctx(), "/dev/x", "ext4", nil)
		require.NoError(t, err)
		assert.False(t, formatted)
		for _, cmd := range fr.Commands() {
			assert.NotContains(t, cmd, "mkfs", "must not format an already-formatted device")
		}
	})

	t.Run("different fs -> refuses (data safety)", func(t *testing.T) {
		fr := runnerFor(map[string]cmdResult{"blkid": {stdout: "xfs"}})
		_, err := New(fr, nil).FormatIfEmpty(ctx(), "/dev/x", "ext4", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "refusing to format")
		for _, cmd := range fr.Commands() {
			assert.NotContains(t, cmd, "mkfs")
		}
	})

	t.Run("unsupported fs", func(t *testing.T) {
		fr := runnerFor(map[string]cmdResult{"blkid": {err: &cexec.Error{ExitCode: 2}}})
		_, err := New(fr, nil).FormatIfEmpty(ctx(), "/dev/x", "btrfs", nil)
		require.Error(t, err)
	})
}

func TestMountIdempotent(t *testing.T) {
	target := filepath.Join(t.TempDir(), "stage")

	t.Run("not mounted -> mounts", func(t *testing.T) {
		fr := runnerFor(map[string]cmdResult{"findmnt": {err: &cexec.Error{Name: "findmnt", ExitCode: 1}}})
		err := New(fr, nil).Mount(ctx(), "/dev/x", target, "ext4", []string{"noatime"})
		require.NoError(t, err)
		joined := strings.Join(fr.Commands(), "|")
		assert.Contains(t, joined, "mount -t ext4 -o noatime /dev/x "+target)
	})

	t.Run("already mounted -> no-op", func(t *testing.T) {
		fr := runnerFor(map[string]cmdResult{"findmnt": {stdout: target}}) // exit 0, target itself
		err := New(fr, nil).Mount(ctx(), "/dev/x", target, "ext4", nil)
		require.NoError(t, err)
		for _, cmd := range fr.Commands() {
			assert.NotContains(t, cmd, "mount -t")
		}
	})

	// findmnt --target returns the CONTAINING mountpoint (a parent) for a path
	// that is not itself a mountpoint; that must NOT count as mounted, else a
	// later umount fails "exit 32: not mounted".
	t.Run("path under a mounted fs is not a mountpoint", func(t *testing.T) {
		fr := runnerFor(map[string]cmdResult{"findmnt": {stdout: "/local\n"}}) // parent, not target
		mounted, err := New(fr, nil).IsMounted(ctx(), target)
		require.NoError(t, err)
		assert.False(t, mounted, "containing mountpoint must not be reported as mounted")
	})

	t.Run("unmount of a non-mountpoint is a no-op", func(t *testing.T) {
		fr := runnerFor(map[string]cmdResult{"findmnt": {stdout: "/local\n"}}) // not a mountpoint
		require.NoError(t, New(fr, nil).Unmount(ctx(), target))
		for _, cmd := range fr.Commands() {
			assert.NotContains(t, cmd, "umount", "umount must not run on a non-mountpoint")
		}
	})
}

func TestResize(t *testing.T) {
	t.Run("ext4 -> resize2fs device", func(t *testing.T) {
		fr := runnerFor(nil)
		require.NoError(t, New(fr, nil).Resize(ctx(), "/dev/x", "/mnt/x", "ext4"))
		assert.Equal(t, []string{"resize2fs /dev/x"}, fr.Commands())
	})
	t.Run("xfs -> xfs_growfs mountpoint", func(t *testing.T) {
		fr := runnerFor(nil)
		require.NoError(t, New(fr, nil).Resize(ctx(), "/dev/x", "/mnt/x", "xfs"))
		assert.Equal(t, []string{"xfs_growfs /mnt/x"}, fr.Commands())
	})
}
