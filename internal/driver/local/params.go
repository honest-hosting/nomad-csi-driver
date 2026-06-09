package local

import (
	"strconv"
	"strings"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// hostAuto is the placement value meaning "the controller picks the
// fewest-volumes node". An explicit node name pins to that node.
const hostAuto = "auto"

// volumeParams are the per-volume settings resolved for a local volume.
type volumeParams struct {
	fsType       string
	block        bool
	volblocksize string
	host         string // "auto" or an explicit node name
	pool         string // selected zpool name (from the allowlist)
}

func resolveParams(cfg *config.LocalConfig, p map[string]string, caps []driver.VolumeCapability) (volumeParams, error) {
	vp := volumeParams{
		volblocksize: cfg.DefaultVolblocksize,
		host:         hostAuto,
		pool:         cfg.DefaultPool,
	}
	if vp.volblocksize == "" {
		vp.volblocksize = "16K"
	}

	if len(caps) > 0 && caps[0].AccessType == driver.AccessTypeBlock {
		vp.block = true
	} else {
		vp.fsType = "ext4"
		if len(caps) > 0 && caps[0].FsType != "" {
			vp.fsType = caps[0].FsType
		}
		if v := p["fsType"]; v != "" {
			vp.fsType = v
		}
		if vp.fsType != "ext4" && vp.fsType != "xfs" {
			return vp, driver.InvalidArgument("unsupported fsType %q (want ext4|xfs)", vp.fsType)
		}
	}

	if v := p["volblocksize"]; v != "" {
		vp.volblocksize = v
	}
	if _, err := parseSizeBytes(vp.volblocksize); err != nil {
		return vp, driver.InvalidArgument("invalid volblocksize %q: %v", vp.volblocksize, err)
	}

	if v := p["host"]; v != "" {
		vp.host = v
	}

	// Pool selection (checkpoint 1): a bare name, from the allowlist. Reject
	// path-like values and unknown pools here, before any forwarding.
	if v := p["pool"]; v != "" {
		vp.pool = v
	}
	if strings.ContainsRune(vp.pool, '/') {
		return vp, driver.InvalidArgument("parameters.pool %q must be a bare zpool name (no '/')", vp.pool)
	}
	if _, ok := cfg.PoolByName(vp.pool); !ok {
		return vp, driver.InvalidArgument("parameters.pool %q is not an allowed pool (configured: %v)", vp.pool, cfg.PoolNames())
	}
	return vp, nil
}

// roundUpToBlock rounds required bytes up to a whole multiple of the
// volblocksize, reporting OUT_OF_RANGE if the rounded size exceeds the limit.
func roundUpToBlock(cr driver.CapacityRange, volblocksize string) (int64, error) {
	req := cr.RequiredBytes
	if req <= 0 {
		return 0, driver.InvalidArgument("capacity required_bytes must be > 0")
	}
	block, err := parseSizeBytes(volblocksize)
	if err != nil {
		return 0, driver.InvalidArgument("invalid volblocksize %q: %v", volblocksize, err)
	}
	rounded := ((req + block - 1) / block) * block
	if cr.LimitBytes > 0 && rounded > cr.LimitBytes {
		return 0, driver.OutOfRange("required %d rounds up to %d which exceeds limit %d", req, rounded, cr.LimitBytes)
	}
	return rounded, nil
}

// parseSizeBytes parses a size like "16K", "1M", "2G", or a plain byte count,
// using binary (1024-based) units to match ZFS volblocksize semantics.
func parseSizeBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errInvalidSize
	}
	mult := int64(1)
	switch last := s[len(s)-1]; last {
	case 'K', 'k':
		mult = 1 << 10
	case 'M', 'm':
		mult = 1 << 20
	case 'G', 'g':
		mult = 1 << 30
	case 'T', 't':
		mult = 1 << 40
	}
	num := s
	if mult != 1 {
		num = s[:len(s)-1]
	}
	n, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
	if err != nil || n <= 0 {
		return 0, errInvalidSize
	}
	return n * mult, nil
}

type sizeError struct{}

func (sizeError) Error() string { return "invalid size" }

var errInvalidSize = sizeError{}
