//go:build linux

package mountutil

import (
	"fmt"
	"syscall"
)

// FSStats is filesystem usage from statfs, in bytes and inodes.
type FSStats struct {
	TotalBytes     int64
	UsedBytes      int64
	AvailableBytes int64
	TotalInodes    int64
	UsedInodes     int64
	FreeInodes     int64
}

// StatFS returns usage for the filesystem containing path (NodeGetVolumeStats).
func StatFS(path string) (FSStats, error) {
	var s syscall.Statfs_t
	if err := syscall.Statfs(path, &s); err != nil {
		return FSStats{}, fmt.Errorf("statfs %s: %w", path, err)
	}
	bs := int64(s.Bsize)
	total := int64(s.Blocks) * bs
	free := int64(s.Bfree) * bs
	return FSStats{
		TotalBytes:     total,
		AvailableBytes: int64(s.Bavail) * bs,
		UsedBytes:      total - free,
		TotalInodes:    int64(s.Files),
		FreeInodes:     int64(s.Ffree),
		UsedInodes:     int64(s.Files) - int64(s.Ffree),
	}, nil
}
