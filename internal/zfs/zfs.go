// Package zfs wraps the zfs/zpool CLIs for the --driver=local backend: thick
// zvol create/destroy, resize, snapshot, independent clone (send|recv), and
// pool/health queries. All commands go through the exec.Runner seam so the
// command sequence is unit-testable without ZFS installed.
package zfs

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
)

// ZFS issues zfs/zpool commands via a Runner.
type ZFS struct {
	run cexec.Runner
}

// New returns a ZFS backed by the given Runner.
func New(run cexec.Runner) *ZFS { return &ZFS{run: run} }

// Zvol is a listed zvol with its provisioned size.
type Zvol struct {
	Name     string
	SizeByte int64
}

// SnapshotEntry is one row from ListSnapshots: the full "<dataset>@<name>", the
// volsize it captured, and its creation time (Unix epoch from `-p`).
type SnapshotEntry struct {
	Name         string
	VolsizeBytes int64
	CreationUnix int64
}

// CreateZvol creates a thick zvol of sizeBytes at dataset with the given
// volblocksize (e.g. "16K"). Thick (non-sparse) is the default; no -s flag is
// passed, so ZFS sets a refreservation automatically. -p creates any missing
// parent datasets (the <pool>/<parent_dataset> namespace), so the operator only
// has to provision the zpool — the driver owns its own dataset hierarchy.
func (z *ZFS) CreateZvol(ctx context.Context, dataset string, sizeBytes int64, volblocksize string) error {
	args := []string{"create", "-p", "-V", strconv.FormatInt(sizeBytes, 10)}
	if volblocksize != "" {
		args = append(args, "-b", volblocksize)
	}
	args = append(args, dataset)
	if _, err := z.run.Run(ctx, cexec.Command{Name: "zfs", Args: args}); err != nil {
		return fmt.Errorf("zfs create %s: %w", dataset, err)
	}
	return nil
}

// DestroyZvol destroys a single zvol. It deliberately does NOT pass -r or -f:
// destroying a dataset with unexpected children/snapshots should fail loudly
// rather than cascade (data safety).
func (z *ZFS) DestroyZvol(ctx context.Context, dataset string) error {
	if _, err := z.run.Run(ctx, cexec.Command{Name: "zfs", Args: []string{"destroy", dataset}}); err != nil {
		return fmt.Errorf("zfs destroy %s: %w", dataset, err)
	}
	return nil
}

// Exists reports whether a dataset (zvol or snapshot) exists.
func (z *ZFS) Exists(ctx context.Context, dataset string) (bool, error) {
	_, err := z.run.Run(ctx, cexec.Command{Name: "zfs", Args: []string{"list", "-H", "-o", "name", dataset}})
	if err != nil {
		// zfs list of a nonexistent dataset exits 1.
		if cexec.IsExit(err, 1) {
			return false, nil
		}
		return false, fmt.Errorf("zfs list %s: %w", dataset, err)
	}
	return true, nil
}

// GetVolsize returns the provisioned size of a zvol in bytes.
func (z *ZFS) GetVolsize(ctx context.Context, dataset string) (int64, error) {
	out, err := z.run.Run(ctx, cexec.Command{Name: "zfs", Args: []string{"get", "-Hp", "-o", "value", "volsize", dataset}})
	if err != nil {
		return 0, fmt.Errorf("zfs get volsize %s: %w", dataset, err)
	}
	return parseInt(out.Stdout, "volsize")
}

// GetVolblocksize returns a zvol's volume block size in bytes (the fixed-at-
// creation property expansion must round to).
func (z *ZFS) GetVolblocksize(ctx context.Context, dataset string) (int64, error) {
	out, err := z.run.Run(ctx, cexec.Command{Name: "zfs", Args: []string{"get", "-Hp", "-o", "value", "volblocksize", dataset}})
	if err != nil {
		return 0, fmt.Errorf("zfs get volblocksize %s: %w", dataset, err)
	}
	return parseInt(out.Stdout, "volblocksize")
}

// GetCreation returns the authoritative creation time of a dataset/snapshot,
// read from ZFS's `creation` property (`-p` yields Unix epoch seconds).
func (z *ZFS) GetCreation(ctx context.Context, dataset string) (time.Time, error) {
	out, err := z.run.Run(ctx, cexec.Command{Name: "zfs", Args: []string{"get", "-Hp", "-o", "value", "creation", dataset}})
	if err != nil {
		return time.Time{}, fmt.Errorf("zfs get creation %s: %w", dataset, err)
	}
	secs, err := parseInt(out.Stdout, "creation")
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(secs, 0), nil
}

// SetVolsize grows a zvol to sizeBytes (grow-only is enforced by the caller).
func (z *ZFS) SetVolsize(ctx context.Context, dataset string, sizeBytes int64) error {
	arg := "volsize=" + strconv.FormatInt(sizeBytes, 10)
	if _, err := z.run.Run(ctx, cexec.Command{Name: "zfs", Args: []string{"set", arg, dataset}}); err != nil {
		return fmt.Errorf("zfs set %s %s: %w", arg, dataset, err)
	}
	return nil
}

// SetUserProp sets a namespaced user property (e.g. "nomad-csi:source") on a
// dataset. Used to record a clone/restore's provenance so an idempotent retry
// can verify it.
func (z *ZFS) SetUserProp(ctx context.Context, dataset, prop, value string) error {
	arg := prop + "=" + value
	if _, err := z.run.Run(ctx, cexec.Command{Name: "zfs", Args: []string{"set", arg, dataset}}); err != nil {
		return fmt.Errorf("zfs set %s %s: %w", arg, dataset, err)
	}
	return nil
}

// GetUserProp reads a user property. ZFS reports "-" for an unset property,
// which is normalized to "".
func (z *ZFS) GetUserProp(ctx context.Context, dataset, prop string) (string, error) {
	out, err := z.run.Run(ctx, cexec.Command{Name: "zfs", Args: []string{"get", "-H", "-o", "value", prop, dataset}})
	if err != nil {
		return "", fmt.Errorf("zfs get %s %s: %w", prop, dataset, err)
	}
	v := strings.TrimSpace(string(out.Stdout))
	if v == "-" {
		return "", nil
	}
	return v, nil
}

// Snapshot creates dataset@name.
func (z *ZFS) Snapshot(ctx context.Context, dataset, name string) error {
	snap := dataset + "@" + name
	if _, err := z.run.Run(ctx, cexec.Command{Name: "zfs", Args: []string{"snapshot", snap}}); err != nil {
		return fmt.Errorf("zfs snapshot %s: %w", snap, err)
	}
	return nil
}

// DestroySnapshot destroys dataset@name.
func (z *ZFS) DestroySnapshot(ctx context.Context, dataset, name string) error {
	snap := dataset + "@" + name
	if _, err := z.run.Run(ctx, cexec.Command{Name: "zfs", Args: []string{"destroy", snap}}); err != nil {
		return fmt.Errorf("zfs destroy %s: %w", snap, err)
	}
	return nil
}

// CloneIndependent creates a fully independent copy of srcSnapshot at
// destDataset via `zfs send | zfs recv` (no origin dependency, unlike
// `zfs clone`), so the source can later be destroyed.
func (z *ZFS) CloneIndependent(ctx context.Context, srcDataset, srcSnapshot, destDataset string) error {
	src := srcDataset + "@" + srcSnapshot
	// Stream `zfs send <src> | zfs recv <dest>` without a shell. A shell pipe
	// reports only the recv side's exit status, so a failed `zfs send` (e.g. a
	// truncated stream) would be masked and silently produce a corrupt/empty
	// clone. RunPipe checks both legs, and passing argv directly avoids any shell
	// quoting concern.
	if err := z.run.RunPipe(ctx,
		cexec.Command{Name: "zfs", Args: []string{"send", src}},
		cexec.Command{Name: "zfs", Args: []string{"recv", destDataset}},
	); err != nil {
		return fmt.Errorf("zfs send|recv %s -> %s: %w", src, destDataset, err)
	}
	// zfs recv reconstitutes the snapshot on the destination (destDataset@srcSnapshot).
	// Drop it so the clone is a clean, snapshot-free independent dataset — otherwise
	// that inherited snapshot would block the clone's own deletion.
	if err := z.DestroySnapshot(ctx, destDataset, srcSnapshot); err != nil {
		return fmt.Errorf("dropping received snapshot on clone: %w", err)
	}
	return nil
}

// ListZvols lists zvols under parent with their sizes.
func (z *ZFS) ListZvols(ctx context.Context, parent string) ([]Zvol, error) {
	out, err := z.run.Run(ctx, cexec.Command{
		Name: "zfs",
		Args: []string{"list", "-Hp", "-t", "volume", "-r", "-o", "name,volsize", parent},
	})
	if err != nil {
		if cexec.IsExit(err, 1) {
			return nil, nil
		}
		return nil, fmt.Errorf("zfs list -t volume %s: %w", parent, err)
	}
	var vols []Zvol
	for _, line := range strings.Split(strings.TrimSpace(string(out.Stdout)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		size, _ := strconv.ParseInt(fields[1], 10, 64)
		vols = append(vols, Zvol{Name: fields[0], SizeByte: size})
	}
	return vols, nil
}

// ListSnapshots lists snapshots under target (a pool's parent dataset for all
// of them, or a single zvol dataset for just its own). Returns nil when target
// has none / is gone.
func (z *ZFS) ListSnapshots(ctx context.Context, target string) ([]SnapshotEntry, error) {
	out, err := z.run.Run(ctx, cexec.Command{
		Name: "zfs",
		Args: []string{"list", "-Hp", "-t", "snapshot", "-r", "-o", "name,volsize,creation", target},
	})
	if err != nil {
		if cexec.IsExit(err, 1) {
			return nil, nil
		}
		return nil, fmt.Errorf("zfs list -t snapshot %s: %w", target, err)
	}
	var snaps []SnapshotEntry
	for _, line := range strings.Split(strings.TrimSpace(string(out.Stdout)), "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		size, _ := strconv.ParseInt(fields[1], 10, 64)
		created, _ := strconv.ParseInt(fields[2], 10, 64)
		snaps = append(snaps, SnapshotEntry{Name: fields[0], VolsizeBytes: size, CreationUnix: created})
	}
	return snaps, nil
}

// PoolFree returns free bytes in the pool: `zpool get free`, which counts
// physically allocated (written) blocks only — it does NOT account for dataset
// reservations. Useful as a diagnostic of real on-disk usage, but NOT the right
// signal for placement; use PoolAvailable for that.
func (z *ZFS) PoolFree(ctx context.Context, pool string) (int64, error) {
	out, err := z.run.Run(ctx, cexec.Command{Name: "zpool", Args: []string{"get", "-Hp", "-o", "value", "free", pool}})
	if err != nil {
		return 0, fmt.Errorf("zpool get free %s: %w", pool, err)
	}
	return parseInt(out.Stdout, "free")
}

// PoolAvailable returns the pool root dataset's available bytes (`zfs get
// available`). Unlike PoolFree (pool-level physical free), the dataset-level
// `available` property is reduced by descendant reservations. Because the driver
// creates THICK zvols (CreateZvol passes no -s, so each carries a refreservation
// equal to its volsize), this value reflects PROVISIONED/allocated space — the
// sum of zvol sizes is charged against the pool the moment each zvol is created,
// regardless of how much has actually been written. This is the correct signal
// for placement and the create-time capacity guard.
func (z *ZFS) PoolAvailable(ctx context.Context, pool string) (int64, error) {
	out, err := z.run.Run(ctx, cexec.Command{Name: "zfs", Args: []string{"get", "-Hp", "-o", "value", "available", pool}})
	if err != nil {
		return 0, fmt.Errorf("zfs get available %s: %w", pool, err)
	}
	return parseInt(out.Stdout, "available")
}

// PoolHealthy reports whether the pool's health is ONLINE.
func (z *ZFS) PoolHealthy(ctx context.Context, pool string) (bool, error) {
	out, err := z.run.Run(ctx, cexec.Command{Name: "zpool", Args: []string{"list", "-H", "-o", "health", pool}})
	if err != nil {
		return false, fmt.Errorf("zpool list health %s: %w", pool, err)
	}
	return strings.TrimSpace(string(out.Stdout)) == "ONLINE", nil
}

// PoolStatus reports whether the pool is imported on this node (present) and
// whether its health is ONLINE. A missing pool (`zpool list` exit 1) is
// reported as present=false with a nil error; a non-exit-1 failure (e.g. zfs
// not installed) returns the error so callers can distinguish "absent pool"
// from "ZFS unavailable".
func (z *ZFS) PoolStatus(ctx context.Context, pool string) (present, online bool, err error) {
	out, err := z.run.Run(ctx, cexec.Command{Name: "zpool", Args: []string{"list", "-H", "-o", "health", pool}})
	if err != nil {
		if cexec.IsExit(err, 1) {
			return false, false, nil // pool not imported on this node
		}
		return false, false, fmt.Errorf("zpool list health %s: %w", pool, err)
	}
	return true, strings.TrimSpace(string(out.Stdout)) == "ONLINE", nil
}

// PoolSize returns the pool's total size in bytes.
func (z *ZFS) PoolSize(ctx context.Context, pool string) (int64, error) {
	out, err := z.run.Run(ctx, cexec.Command{Name: "zpool", Args: []string{"get", "-Hp", "-o", "value", "size", pool}})
	if err != nil {
		return 0, fmt.Errorf("zpool get size %s: %w", pool, err)
	}
	return parseInt(out.Stdout, "size")
}

// DevicePath returns the device node for a zvol dataset.
func DevicePath(dataset string) string { return "/dev/zvol/" + dataset }

func parseInt(b []byte, what string) (int64, error) {
	s := strings.TrimSpace(string(b))
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parsing %s %q: %w", what, s, err)
	}
	return n, nil
}
