package csi

import (
	csipb "github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// toVolumeCapabilities converts and validates a slice of CSI volume
// capabilities. It rejects multi-node access modes (ext4/xfs are not cluster
// filesystems) up front, so backends only ever see single-node modes.
func toVolumeCapabilities(in []*csipb.VolumeCapability) ([]driver.VolumeCapability, error) {
	if len(in) == 0 {
		return nil, driver.InvalidArgument("volume capabilities are required")
	}
	out := make([]driver.VolumeCapability, 0, len(in))
	for _, c := range in {
		dc, err := toVolumeCapability(c)
		if err != nil {
			return nil, err
		}
		out = append(out, dc)
	}
	return out, nil
}

func toVolumeCapability(c *csipb.VolumeCapability) (driver.VolumeCapability, error) {
	var dc driver.VolumeCapability
	if c == nil {
		return dc, driver.InvalidArgument("volume capability is nil")
	}

	mode, err := toAccessMode(c.GetAccessMode().GetMode())
	if err != nil {
		return dc, err
	}
	dc.AccessMode = mode

	switch {
	case c.GetBlock() != nil:
		dc.AccessType = driver.AccessTypeBlock
	case c.GetMount() != nil:
		dc.AccessType = driver.AccessTypeMount
		dc.FsType = c.GetMount().GetFsType()
		dc.MountFlags = c.GetMount().GetMountFlags()
	default:
		return dc, driver.InvalidArgument("volume capability must specify mount or block access type")
	}
	return dc, nil
}

func toAccessMode(m csipb.VolumeCapability_AccessMode_Mode) (driver.AccessMode, error) {
	switch m {
	case csipb.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		csipb.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER,
		csipb.VolumeCapability_AccessMode_SINGLE_NODE_MULTI_WRITER:
		return driver.AccessModeSingleNodeWriter, nil
	case csipb.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY:
		return driver.AccessModeSingleNodeReaderOnly, nil
	case csipb.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
		csipb.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
		csipb.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
		return driver.AccessModeUnknown, driver.InvalidArgument("multi-node access modes are not supported (single-node only)")
	default:
		return driver.AccessModeUnknown, driver.InvalidArgument("unknown access mode %v", m)
	}
}

func toCapacityRange(r *csipb.CapacityRange) driver.CapacityRange {
	if r == nil {
		return driver.CapacityRange{}
	}
	return driver.CapacityRange{RequiredBytes: r.GetRequiredBytes(), LimitBytes: r.GetLimitBytes()}
}

func toContentSource(s *csipb.VolumeContentSource) *driver.ContentSource {
	if s == nil {
		return nil
	}
	if snap := s.GetSnapshot(); snap != nil {
		return &driver.ContentSource{SnapshotID: snap.GetSnapshotId()}
	}
	if vol := s.GetVolume(); vol != nil {
		return &driver.ContentSource{VolumeID: vol.GetVolumeId()}
	}
	return nil
}

func toDriverTopologies(in []*csipb.Topology) []driver.Topology {
	if len(in) == 0 {
		return nil
	}
	out := make([]driver.Topology, 0, len(in))
	for _, t := range in {
		out = append(out, driver.Topology{Segments: t.GetSegments()})
	}
	return out
}

func fromDriverTopologies(in []driver.Topology) []*csipb.Topology {
	if len(in) == 0 {
		return nil
	}
	out := make([]*csipb.Topology, 0, len(in))
	for _, t := range in {
		out = append(out, &csipb.Topology{Segments: t.Segments})
	}
	return out
}

func fromContentSource(s *driver.ContentSource) *csipb.VolumeContentSource {
	if s == nil {
		return nil
	}
	if s.SnapshotID != "" {
		return &csipb.VolumeContentSource{
			Type: &csipb.VolumeContentSource_Snapshot{
				Snapshot: &csipb.VolumeContentSource_SnapshotSource{SnapshotId: s.SnapshotID},
			},
		}
	}
	if s.VolumeID != "" {
		return &csipb.VolumeContentSource{
			Type: &csipb.VolumeContentSource_Volume{
				Volume: &csipb.VolumeContentSource_VolumeSource{VolumeId: s.VolumeID},
			},
		}
	}
	return nil
}
