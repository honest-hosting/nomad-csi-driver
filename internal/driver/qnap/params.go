package qnap

import (
	"strconv"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

const giB = int64(1) << 30

// absDiff returns the absolute difference between two byte counts.
func absDiff(a, b int64) int64 {
	if a > b {
		return a - b
	}
	return b - a
}

// volumeParams are the per-volume settings resolved from the CSI request
// parameters, the volume capability, and the controller defaults.
type volumeParams struct {
	fsType      string
	block       bool // block-device access (no filesystem)
	sectorSize  int
	poolID      int
	thin        bool // thin-provisioned LUN (default); thick is the slow opt-in
	targetIndex *int // set for 1:N (attach to an existing/shared target)
}

// resolveParams merges request parameters with controller defaults and
// validates them.
func resolveParams(cfg *config.QNAPConfig, p map[string]string, caps []driver.VolumeCapability) (volumeParams, error) {
	// Thin is the default: a thick LUN pre-allocates on the appliance, which can
	// add ~seconds to create while it initializes (the LUN must reach Ready before
	// it can be mapped). Opt into thick with parameters.thin = "false".
	vp := volumeParams{sectorSize: 512, poolID: cfg.DefaultPoolID, thin: true}

	// Access type / filesystem come from the (validated, single-node) capability.
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

	if cfg.DefaultSectorSize != 0 {
		vp.sectorSize = cfg.DefaultSectorSize
	}
	if v := p["sectorSize"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return vp, driver.InvalidArgument("invalid sectorSize %q", v)
		}
		vp.sectorSize = n
	}
	if vp.sectorSize != 512 && vp.sectorSize != 4096 {
		return vp, driver.InvalidArgument("unsupported sectorSize %d (want 512 or 4096; 4096 is Windows-only)", vp.sectorSize)
	}

	if v := p["pool"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return vp, driver.InvalidArgument("invalid pool %q", v)
		}
		vp.poolID = n
	}
	if vp.poolID <= 0 {
		return vp, driver.InvalidArgument("pool is required (set parameters.pool or qnap.default_pool_id)")
	}

	if v := p["thin"]; v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return vp, driver.InvalidArgument("invalid thin %q (want true|false)", v)
		}
		vp.thin = b
	}

	if v := p["targetIndex"]; v != "" {
		// 1:N shared-target mapping is gated off for v1: the node addresses a LUN
		// by its per-target iSCSI LUN number, but the controller does not yet read
		// back and record that number (it assembles every volume at LUN 0). Until
		// that is plumbed through, attaching a LUN to a shared target would bind
		// the wrong device. Each volume gets its own 1:1 target instead.
		return vp, driver.InvalidArgument("shared-target (1:N) mapping via parameters.targetIndex is not supported in this release; omit it to get a dedicated 1:1 target")
	}
	return vp, nil
}

// sizeGiBExact converts a CSI capacity range to a whole-GiB LUN size. QNAP only
// provisions in whole binary GiB, so a non-GiB-aligned required_bytes is
// rejected (rather than silently rounded), which effectively forces operators
// to write capacity_min = "NGiB".
func sizeGiBExact(cr driver.CapacityRange) (int, error) {
	req := cr.RequiredBytes
	if req <= 0 {
		return 0, driver.InvalidArgument("capacity required_bytes must be > 0")
	}
	if req%giB != 0 {
		return 0, driver.InvalidArgument("qnap provisions in whole GiB; %d bytes is not a multiple of 1 GiB (use capacity_min = \"NGiB\")", req)
	}
	if cr.LimitBytes > 0 && req > cr.LimitBytes {
		return 0, driver.OutOfRange("required_bytes %d exceeds limit_bytes %d", req, cr.LimitBytes)
	}
	return int(req / giB), nil
}

// sanitizeLUNName maps a CSI volume name to a QNAP-acceptable LUN name
// ([A-Za-z0-9_-], reasonable length). QNAP LUN names are constrained, so this
// keeps allocations deterministic from the requested name.
func sanitizeLUNName(name string) string {
	b := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-':
			b = append(b, r)
		default:
			b = append(b, '-')
		}
	}
	out := string(b)
	if len(out) > 64 {
		out = out[:64]
	}
	return out
}
