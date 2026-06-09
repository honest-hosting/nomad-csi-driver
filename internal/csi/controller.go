package csi

import (
	"context"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// controllerServer adapts the CSI ControllerService to a
// driver.ControllerBackend. Unsupported RPCs fall through to the embedded
// Unimplemented base.
type controllerServer struct {
	csipb.UnimplementedControllerServer
	backend driver.Backend
	ctrl    driver.ControllerBackend
	caps    driver.Capabilities
	locks   *keyedLocker
}

func newControllerServer(b driver.Backend) *controllerServer {
	return &controllerServer{backend: b, ctrl: b.Controller(), caps: b.Capabilities(), locks: newKeyedLocker()}
}

func (s *controllerServer) CreateVolume(ctx context.Context, req *csipb.CreateVolumeRequest) (*csipb.CreateVolumeResponse, error) {
	if !s.caps.CreateDelete {
		return nil, driver.Unimplemented("CreateVolume is not supported by the %s backend", s.backend.Name())
	}
	if req.GetName() == "" {
		return nil, driver.InvalidArgument("CreateVolume: name is required")
	}
	// Serialize per volume name (idempotency key): a duplicate in-flight create
	// for the same name fails fast rather than racing on provisioning state.
	release, err := s.locks.acquire("volume", req.GetName())
	if err != nil {
		return nil, err
	}
	defer release()
	caps, err := toVolumeCapabilities(req.GetVolumeCapabilities())
	if err != nil {
		return nil, err
	}

	vol, err := s.ctrl.CreateVolume(ctx, &driver.CreateVolumeRequest{
		Name:                req.GetName(),
		CapacityRange:       toCapacityRange(req.GetCapacityRange()),
		VolumeCapabilities:  caps,
		Parameters:          req.GetParameters(),
		Secrets:             req.GetSecrets(),
		ContentSource:       toContentSource(req.GetVolumeContentSource()),
		TopologyRequirement: topologyRequirement(req.GetAccessibilityRequirements()),
	})
	if err != nil {
		return nil, err
	}

	return &csipb.CreateVolumeResponse{
		Volume: &csipb.Volume{
			VolumeId:           vol.VolumeID,
			CapacityBytes:      vol.CapacityBytes,
			VolumeContext:      vol.VolumeContext,
			AccessibleTopology: fromDriverTopologies(vol.AccessibleTopology),
			ContentSource:      fromContentSource(vol.ContentSource),
		},
	}, nil
}

func (s *controllerServer) DeleteVolume(ctx context.Context, req *csipb.DeleteVolumeRequest) (*csipb.DeleteVolumeResponse, error) {
	if !s.caps.CreateDelete {
		return nil, driver.Unimplemented("DeleteVolume is not supported by the %s backend", s.backend.Name())
	}
	if req.GetVolumeId() == "" {
		return nil, driver.InvalidArgument("DeleteVolume: volume_id is required")
	}
	release, err := s.locks.acquire("volume", req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.ctrl.DeleteVolume(ctx, req.GetVolumeId(), req.GetSecrets()); err != nil {
		return nil, err
	}
	return &csipb.DeleteVolumeResponse{}, nil
}

func (s *controllerServer) ControllerPublishVolume(ctx context.Context, req *csipb.ControllerPublishVolumeRequest) (*csipb.ControllerPublishVolumeResponse, error) {
	if !s.caps.PublishUnpublish {
		return nil, driver.Unimplemented("ControllerPublishVolume is not supported by the %s backend", s.backend.Name())
	}
	if req.GetVolumeId() == "" || req.GetNodeId() == "" {
		return nil, driver.InvalidArgument("ControllerPublishVolume: volume_id and node_id are required")
	}
	release, err := s.locks.acquire("volume", req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	defer release()
	cap, err := toVolumeCapability(req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}
	pubCtx, err := s.ctrl.ControllerPublishVolume(ctx, req.GetVolumeId(), req.GetNodeId(), cap, req.GetReadonly(), req.GetVolumeContext(), req.GetSecrets())
	if err != nil {
		return nil, err
	}
	return &csipb.ControllerPublishVolumeResponse{PublishContext: pubCtx}, nil
}

func (s *controllerServer) ControllerUnpublishVolume(ctx context.Context, req *csipb.ControllerUnpublishVolumeRequest) (*csipb.ControllerUnpublishVolumeResponse, error) {
	if !s.caps.PublishUnpublish {
		return nil, driver.Unimplemented("ControllerUnpublishVolume is not supported by the %s backend", s.backend.Name())
	}
	if req.GetVolumeId() == "" {
		return nil, driver.InvalidArgument("ControllerUnpublishVolume: volume_id is required")
	}
	release, err := s.locks.acquire("volume", req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.ctrl.ControllerUnpublishVolume(ctx, req.GetVolumeId(), req.GetNodeId(), req.GetSecrets()); err != nil {
		return nil, err
	}
	return &csipb.ControllerUnpublishVolumeResponse{}, nil
}

func (s *controllerServer) ValidateVolumeCapabilities(ctx context.Context, req *csipb.ValidateVolumeCapabilitiesRequest) (*csipb.ValidateVolumeCapabilitiesResponse, error) {
	if req.GetVolumeId() == "" {
		return nil, driver.InvalidArgument("ValidateVolumeCapabilities: volume_id is required")
	}
	// CSI requires NotFound for a volume that does not exist.
	exists, err := s.ctrl.VolumeExists(ctx, req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, driver.NotFound("volume %q does not exist", req.GetVolumeId())
	}
	// All single-node modes are supported; multi-node is rejected during
	// conversion. If conversion succeeds, confirm the capabilities.
	if _, err := toVolumeCapabilities(req.GetVolumeCapabilities()); err != nil {
		return &csipb.ValidateVolumeCapabilitiesResponse{Message: err.Error()}, nil
	}
	return &csipb.ValidateVolumeCapabilitiesResponse{
		Confirmed: &csipb.ValidateVolumeCapabilitiesResponse_Confirmed{
			VolumeCapabilities: req.GetVolumeCapabilities(),
		},
	}, nil
}

func (s *controllerServer) GetCapacity(ctx context.Context, req *csipb.GetCapacityRequest) (*csipb.GetCapacityResponse, error) {
	if !s.caps.GetCapacity {
		return nil, driver.Unimplemented("GetCapacity is not supported by the %s backend", s.backend.Name())
	}
	avail, err := s.ctrl.GetCapacity(ctx, toDriverTopologies(topologyList(req.GetAccessibleTopology())), req.GetParameters())
	if err != nil {
		return nil, err
	}
	return &csipb.GetCapacityResponse{AvailableCapacity: avail}, nil
}

func (s *controllerServer) CreateSnapshot(ctx context.Context, req *csipb.CreateSnapshotRequest) (*csipb.CreateSnapshotResponse, error) {
	if !s.caps.Snapshot {
		return nil, driver.Unimplemented("CreateSnapshot is not supported by the %s backend", s.backend.Name())
	}
	if req.GetSourceVolumeId() == "" || req.GetName() == "" {
		return nil, driver.InvalidArgument("CreateSnapshot: source_volume_id and name are required")
	}
	// Serialize by snapshot name (the create idempotency key).
	release, err := s.locks.acquire("snapshot", req.GetName())
	if err != nil {
		return nil, err
	}
	defer release()
	snap, err := s.ctrl.CreateSnapshot(ctx, &driver.CreateSnapshotRequest{
		SourceVolumeID: req.GetSourceVolumeId(),
		Name:           req.GetName(),
		Parameters:     req.GetParameters(),
		Secrets:        req.GetSecrets(),
	})
	if err != nil {
		return nil, err
	}
	return &csipb.CreateSnapshotResponse{Snapshot: fromSnapshot(snap)}, nil
}

func (s *controllerServer) DeleteSnapshot(ctx context.Context, req *csipb.DeleteSnapshotRequest) (*csipb.DeleteSnapshotResponse, error) {
	if !s.caps.Snapshot {
		return nil, driver.Unimplemented("DeleteSnapshot is not supported by the %s backend", s.backend.Name())
	}
	if req.GetSnapshotId() == "" {
		return nil, driver.InvalidArgument("DeleteSnapshot: snapshot_id is required")
	}
	release, err := s.locks.acquire("snapshot", req.GetSnapshotId())
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.ctrl.DeleteSnapshot(ctx, req.GetSnapshotId(), req.GetSecrets()); err != nil {
		return nil, err
	}
	return &csipb.DeleteSnapshotResponse{}, nil
}

func (s *controllerServer) ControllerExpandVolume(ctx context.Context, req *csipb.ControllerExpandVolumeRequest) (*csipb.ControllerExpandVolumeResponse, error) {
	if !s.caps.Expand {
		return nil, driver.Unimplemented("ControllerExpandVolume is not supported by the %s backend", s.backend.Name())
	}
	if req.GetVolumeId() == "" {
		return nil, driver.InvalidArgument("ControllerExpandVolume: volume_id is required")
	}
	release, err := s.locks.acquire("volume", req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	defer release()
	newBytes, nodeExpand, err := s.ctrl.ExpandVolume(ctx, req.GetVolumeId(), toCapacityRange(req.GetCapacityRange()), req.GetSecrets())
	if err != nil {
		return nil, err
	}
	return &csipb.ControllerExpandVolumeResponse{
		CapacityBytes:         newBytes,
		NodeExpansionRequired: nodeExpand,
	}, nil
}

func (s *controllerServer) ListVolumes(ctx context.Context, _ *csipb.ListVolumesRequest) (*csipb.ListVolumesResponse, error) {
	if !s.caps.ListVolumes {
		return nil, driver.Unimplemented("ListVolumes is not supported by the %s backend", s.backend.Name())
	}
	vols, err := s.ctrl.ListVolumes(ctx)
	if err != nil {
		return nil, err
	}
	entries := make([]*csipb.ListVolumesResponse_Entry, 0, len(vols))
	for _, v := range vols {
		entries = append(entries, &csipb.ListVolumesResponse_Entry{
			Volume: &csipb.Volume{
				VolumeId:           v.VolumeID,
				CapacityBytes:      v.CapacityBytes,
				AccessibleTopology: fromDriverTopologies(v.AccessibleTopology),
			},
		})
	}
	// v1 is non-paginated: all entries, no next token.
	return &csipb.ListVolumesResponse{Entries: entries}, nil
}

func (s *controllerServer) ListSnapshots(ctx context.Context, req *csipb.ListSnapshotsRequest) (*csipb.ListSnapshotsResponse, error) {
	if !s.caps.ListSnapshots {
		return nil, driver.Unimplemented("ListSnapshots is not supported by the %s backend", s.backend.Name())
	}
	snaps, err := s.ctrl.ListSnapshots(ctx, req.GetSourceVolumeId())
	if err != nil {
		return nil, err
	}
	wantSnap := req.GetSnapshotId() // optional exact-match filter
	entries := make([]*csipb.ListSnapshotsResponse_Entry, 0, len(snaps))
	for _, sn := range snaps {
		if wantSnap != "" && sn.SnapshotID != wantSnap {
			continue
		}
		snap := &csipb.Snapshot{
			SnapshotId:     sn.SnapshotID,
			SourceVolumeId: sn.SourceVolumeID,
			SizeBytes:      sn.SizeBytes,
			ReadyToUse:     sn.ReadyToUse,
		}
		if !sn.CreationTime.IsZero() {
			snap.CreationTime = timestamppb.New(sn.CreationTime)
		}
		entries = append(entries, &csipb.ListSnapshotsResponse_Entry{Snapshot: snap})
	}
	// v1 is non-paginated: all entries, no next token.
	return &csipb.ListSnapshotsResponse{Entries: entries}, nil
}

func (s *controllerServer) ControllerGetCapabilities(_ context.Context, _ *csipb.ControllerGetCapabilitiesRequest) (*csipb.ControllerGetCapabilitiesResponse, error) {
	return &csipb.ControllerGetCapabilitiesResponse{Capabilities: controllerCapabilities(s.caps)}, nil
}

// --- helpers ---

func controllerCapabilities(c driver.Capabilities) []*csipb.ControllerServiceCapability {
	var rpcs []csipb.ControllerServiceCapability_RPC_Type
	if c.CreateDelete {
		rpcs = append(rpcs, csipb.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME)
	}
	if c.PublishUnpublish {
		rpcs = append(rpcs, csipb.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME)
	}
	if c.Expand {
		rpcs = append(rpcs, csipb.ControllerServiceCapability_RPC_EXPAND_VOLUME)
	}
	if c.Snapshot {
		rpcs = append(rpcs, csipb.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT)
	}
	if c.Clone {
		rpcs = append(rpcs, csipb.ControllerServiceCapability_RPC_CLONE_VOLUME)
	}
	if c.GetCapacity {
		rpcs = append(rpcs, csipb.ControllerServiceCapability_RPC_GET_CAPACITY)
	}
	if c.ListVolumes {
		rpcs = append(rpcs, csipb.ControllerServiceCapability_RPC_LIST_VOLUMES)
	}
	if c.ListSnapshots {
		rpcs = append(rpcs, csipb.ControllerServiceCapability_RPC_LIST_SNAPSHOTS)
	}
	out := make([]*csipb.ControllerServiceCapability, 0, len(rpcs))
	for _, t := range rpcs {
		out = append(out, &csipb.ControllerServiceCapability{
			Type: &csipb.ControllerServiceCapability_Rpc{
				Rpc: &csipb.ControllerServiceCapability_RPC{Type: t},
			},
		})
	}
	return out
}

func fromSnapshot(s *driver.Snapshot) *csipb.Snapshot {
	if s == nil {
		return nil
	}
	out := &csipb.Snapshot{
		SnapshotId:     s.SnapshotID,
		SourceVolumeId: s.SourceVolumeID,
		SizeBytes:      s.SizeBytes,
		ReadyToUse:     s.ReadyToUse,
	}
	if !s.CreationTime.IsZero() {
		out.CreationTime = timestamppb.New(s.CreationTime)
	}
	return out
}

// topologyRequirement flattens a CSI TopologyRequirement to the preferred (then
// requisite) topology list the backend sees.
func topologyRequirement(r *csipb.TopologyRequirement) []driver.Topology {
	if r == nil {
		return nil
	}
	if t := toDriverTopologies(r.GetPreferred()); len(t) > 0 {
		return t
	}
	return toDriverTopologies(r.GetRequisite())
}

// topologyList wraps a single optional topology into a slice for GetCapacity.
func topologyList(t *csipb.Topology) []*csipb.Topology {
	if t == nil {
		return nil
	}
	return []*csipb.Topology{t}
}
