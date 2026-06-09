// Package mountutil is the shared, backend-agnostic block-device → filesystem
// layer. Both backends produce a block device by different means (qnap:
// multipath /dev/mapper/<wwid>; local: /dev/zvol/<pool>/<vol>) and then hand it
// here to be (idempotently) formatted and mounted. All host commands go through
// the exec.Runner seam so this is unit-testable without root or real devices.
//
// DATA SAFETY: FormatIfEmpty never runs mkfs on a device that already carries a
// filesystem. If the existing filesystem matches the requested type the format
// is skipped (idempotent re-stage); if it differs, the operation is refused
// rather than clobbering data.
package mountutil

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"

	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
)

// Metrics is the optional node mount-layer observation sink. It is satisfied by
// *metrics.NodeMetrics; mountutil stays decoupled by depending on this local
// interface instead of the metrics package.
type Metrics interface {
	MountOp(op, outcome string, dur time.Duration)
	FormatSkipped()
}

// Mounter performs format/mount operations via a command Runner.
type Mounter struct {
	run     cexec.Runner
	log     *zap.Logger
	metrics Metrics // optional; nil = no-op
}

// New returns a Mounter. log may be nil.
func New(run cexec.Runner, log *zap.Logger) *Mounter {
	if log == nil {
		log = zap.NewNop()
	}
	return &Mounter{run: run, log: log}
}

// WithMetrics attaches a metrics sink (optional) and returns the Mounter for
// chaining: mountutil.New(r, log).WithMetrics(nodeMetrics).
func (m *Mounter) WithMetrics(metrics Metrics) *Mounter {
	m.metrics = metrics
	return m
}

// recordOp records op with an ok|error outcome derived from err.
func (m *Mounter) recordOp(op string, start time.Time, err error) {
	if m.metrics == nil {
		return
	}
	outcome := "ok"
	if err != nil {
		outcome = "error"
	}
	m.metrics.MountOp(op, outcome, time.Since(start))
}

// mountOp records op with an explicit outcome (e.g. "refused").
func (m *Mounter) mountOp(op, outcome string, start time.Time) {
	if m.metrics != nil {
		m.metrics.MountOp(op, outcome, time.Since(start))
	}
}

func (m *Mounter) formatSkipped() {
	if m.metrics != nil {
		m.metrics.FormatSkipped()
	}
}

// DetectFilesystem returns the filesystem type on device, or "" if the device
// carries no recognizable filesystem. Uses blkid, whose exit code 2 means
// "nothing found" (not an error).
func (m *Mounter) DetectFilesystem(ctx context.Context, device string) (string, error) {
	out, err := m.run.Run(ctx, cexec.Command{
		Name: "blkid",
		Args: []string{"-o", "value", "-s", "TYPE", device},
	})
	if err != nil {
		var ce *cexec.Error
		if errors.As(err, &ce) && ce.ExitCode == 2 {
			return "", nil // no filesystem
		}
		return "", fmt.Errorf("blkid %s: %w", device, err)
	}
	return strings.TrimSpace(string(out.Stdout)), nil
}

// FormatIfEmpty formats device with fsType only if it is currently empty.
// Returns formatted=true only when mkfs actually ran. If the device already has
// a filesystem of fsType, it returns (false, nil). If it has a *different*
// filesystem, it refuses with an error to protect existing data.
func (m *Mounter) FormatIfEmpty(ctx context.Context, device, fsType string, mkfsArgs []string) (bool, error) {
	start := time.Now()
	existing, err := m.DetectFilesystem(ctx, device)
	if err != nil {
		m.recordOp("format", start, err)
		return false, err
	}
	if existing != "" {
		if existing == fsType {
			m.log.Debug("device already formatted; skipping mkfs",
				zap.String("device", device), zap.String("fsType", fsType))
			m.formatSkipped()
			return false, nil
		}
		// Data-safety refusal: a different filesystem is present. Distinct from a
		// format error so it's alertable on its own.
		m.mountOp("format", "refused", start)
		return false, fmt.Errorf("device %s already carries filesystem %q, refusing to format as %q", device, existing, fsType)
	}

	args, err := mkfsCommand(fsType, device, mkfsArgs)
	if err != nil {
		m.recordOp("format", start, err)
		return false, err
	}
	if _, err := m.run.Run(ctx, cexec.Command{Name: args[0], Args: args[1:]}); err != nil {
		werr := fmt.Errorf("formatting %s as %s: %w", device, fsType, err)
		m.recordOp("format", start, werr)
		return false, werr
	}
	m.log.Info("formatted device", zap.String("device", device), zap.String("fsType", fsType))
	m.recordOp("format", start, nil)
	return true, nil
}

// mkfsCommand builds the mkfs argv for a supported filesystem. The force flags
// are safe here because FormatIfEmpty only calls this after confirming the
// device is empty; they exist solely to keep mkfs non-interactive.
func mkfsCommand(fsType, device string, extra []string) ([]string, error) {
	switch fsType {
	case "ext4":
		return append([]string{"mkfs.ext4", "-F", device}, extra...), nil
	case "xfs":
		return append([]string{"mkfs.xfs", "-f", device}, extra...), nil
	default:
		return nil, fmt.Errorf("unsupported filesystem %q (want ext4|xfs)", fsType)
	}
}

// Mount mounts device at target with fsType and flags, creating the target
// directory if needed. It is idempotent: if target is already a mountpoint it
// returns nil.
func (m *Mounter) Mount(ctx context.Context, device, target, fsType string, flags []string) (err error) {
	start := time.Now()
	defer func() { m.recordOp("mount", start, err) }()
	if err = os.MkdirAll(target, 0o750); err != nil {
		return fmt.Errorf("creating mount target %s: %w", target, err)
	}
	mounted, err := m.IsMounted(ctx, target)
	if err != nil {
		return err
	}
	if mounted {
		return nil
	}
	args := []string{"-t", fsType}
	if len(flags) > 0 {
		args = append(args, "-o", strings.Join(flags, ","))
	}
	args = append(args, device, target)
	if _, err := m.run.Run(ctx, cexec.Command{Name: "mount", Args: args}); err != nil {
		return fmt.Errorf("mounting %s at %s: %w", device, target, err)
	}
	return nil
}

// BindMount bind-mounts source at target. For block-device volumes source is a
// device node and target must be a file; for filesystem volumes both are
// directories. The caller indicates which via isDir.
func (m *Mounter) BindMount(ctx context.Context, source, target string, isDir, readonly bool) (err error) {
	start := time.Now()
	defer func() { m.recordOp("bind", start, err) }()
	if err := ensureTarget(target, isDir); err != nil {
		return err
	}
	mounted, err := m.IsMounted(ctx, target)
	if err != nil {
		return err
	}
	if !mounted {
		if _, err := m.run.Run(ctx, cexec.Command{Name: "mount", Args: []string{"--bind", source, target}}); err != nil {
			return fmt.Errorf("bind-mounting %s at %s: %w", source, target, err)
		}
	}
	if readonly {
		if _, err := m.run.Run(ctx, cexec.Command{Name: "mount", Args: []string{"-o", "remount,ro,bind", target}}); err != nil {
			return fmt.Errorf("remounting %s read-only: %w", target, err)
		}
	}
	return nil
}

// Unmount unmounts target. It is idempotent: unmounting a path that is not
// mounted returns nil.
func (m *Mounter) Unmount(ctx context.Context, target string) (err error) {
	start := time.Now()
	defer func() { m.recordOp("unmount", start, err) }()
	mounted, err := m.IsMounted(ctx, target)
	if err != nil {
		return err
	}
	if !mounted {
		return nil
	}
	if _, err := m.run.Run(ctx, cexec.Command{Name: "umount", Args: []string{target}}); err != nil {
		// Tolerate the "already unmounted" race: if it's no longer a mountpoint
		// (e.g. unmounted concurrently, or a stale stage that was never mounted),
		// treat the unmount as successful rather than erroring.
		if again, ierr := m.IsMounted(ctx, target); ierr == nil && !again {
			return nil
		}
		return fmt.Errorf("unmounting %s: %w", target, err)
	}
	return nil
}

// IsMounted reports whether target itself is a mountpoint. It uses
// `findmnt --target`, which for a NON-mountpoint path returns the containing
// filesystem's mountpoint (exit 0) — so we must compare the reported mountpoint
// to target rather than trusting the exit code alone (otherwise every path under
// a mounted fs looks "mounted", and a later umount fails with exit 32).
func (m *Mounter) IsMounted(ctx context.Context, target string) (bool, error) {
	out, err := m.run.Run(ctx, cexec.Command{
		Name: "findmnt",
		Args: []string{"-n", "-o", "TARGET", "--target", target},
	})
	if err != nil {
		var ce *cexec.Error
		if errors.As(err, &ce) && ce.ExitCode == 1 {
			return false, nil
		}
		return false, fmt.Errorf("findmnt %s: %w", target, err)
	}
	return strings.TrimSpace(string(out.Stdout)) == target, nil
}

// Resize grows the filesystem on a device after the underlying block device has
// been enlarged. ext4 uses resize2fs on the device; xfs uses xfs_growfs on the
// mountpoint (xfs can only grow while mounted).
func (m *Mounter) Resize(ctx context.Context, device, mountpoint, fsType string) (err error) {
	start := time.Now()
	defer func() { m.recordOp("resize", start, err) }()
	switch fsType {
	case "ext4":
		if _, err := m.run.Run(ctx, cexec.Command{Name: "resize2fs", Args: []string{device}}); err != nil {
			return fmt.Errorf("resize2fs %s: %w", device, err)
		}
	case "xfs":
		if _, err := m.run.Run(ctx, cexec.Command{Name: "xfs_growfs", Args: []string{mountpoint}}); err != nil {
			return fmt.Errorf("xfs_growfs %s: %w", mountpoint, err)
		}
	default:
		return fmt.Errorf("unsupported filesystem %q for resize (want ext4|xfs)", fsType)
	}
	return nil
}

// SourceDevice returns the backing device of the filesystem mounted at path,
// via findmnt. Used by node-side expansion to locate the device to grow.
func (m *Mounter) SourceDevice(ctx context.Context, path string) (string, error) {
	out, err := m.run.Run(ctx, cexec.Command{
		Name: "findmnt",
		Args: []string{"-n", "-o", "SOURCE", "--target", path},
	})
	if err != nil {
		return "", fmt.Errorf("findmnt source of %s: %w", path, err)
	}
	dev := strings.TrimSpace(string(out.Stdout))
	if dev == "" {
		return "", fmt.Errorf("no source device for %s", path)
	}
	return dev, nil
}

// ensureTarget creates the bind-mount target as a directory or an empty file.
func ensureTarget(target string, isDir bool) error {
	if isDir {
		if err := os.MkdirAll(target, 0o750); err != nil {
			return fmt.Errorf("creating bind target dir %s: %w", target, err)
		}
		return nil
	}
	if _, err := os.Stat(target); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat bind target %s: %w", target, err)
	}
	f, err := os.OpenFile(target, os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("creating bind target file %s: %w", target, err)
	}
	return f.Close()
}
