package csi

import (
	"context"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
)

// nodeServer adapts the CSI NodeService to a driver.NodeBackend.
type nodeServer struct {
	csipb.UnimplementedNodeServer
	backend driver.Backend
	node    driver.NodeBackend
	caps    driver.Capabilities
	locks   *keyedLocker
}

func newNodeServer(b driver.Backend) *nodeServer {
	return &nodeServer{backend: b, node: b.Node(), caps: b.Capabilities(), locks: newKeyedLocker()}
}

func (s *nodeServer) NodeStageVolume(ctx context.Context, req *csipb.NodeStageVolumeRequest) (*csipb.NodeStageVolumeResponse, error) {
	if !s.caps.NodeStage {
		return nil, driver.Unimplemented("NodeStageVolume is not supported by the %s backend", s.backend.Name())
	}
	if req.GetVolumeId() == "" || req.GetStagingTargetPath() == "" {
		return nil, driver.InvalidArgument("NodeStageVolume: volume_id and staging_target_path are required")
	}
	// Serialize all node ops per volume id: concurrent stage/unstage/publish on
	// the same volume would race on the iSCSI session, multipath map, and mount.
	release, err := s.locks.acquire("volume", req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	defer release()
	cap, err := toVolumeCapability(req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}
	if err := s.node.StageVolume(ctx, &driver.StageRequest{
		VolumeID:          req.GetVolumeId(),
		StagingTargetPath: req.GetStagingTargetPath(),
		VolumeCapability:  cap,
		VolumeContext:     req.GetVolumeContext(),
		PublishContext:    req.GetPublishContext(),
		Secrets:           req.GetSecrets(),
	}); err != nil {
		return nil, err
	}
	return &csipb.NodeStageVolumeResponse{}, nil
}

func (s *nodeServer) NodeUnstageVolume(ctx context.Context, req *csipb.NodeUnstageVolumeRequest) (*csipb.NodeUnstageVolumeResponse, error) {
	if !s.caps.NodeStage {
		return nil, driver.Unimplemented("NodeUnstageVolume is not supported by the %s backend", s.backend.Name())
	}
	if req.GetVolumeId() == "" || req.GetStagingTargetPath() == "" {
		return nil, driver.InvalidArgument("NodeUnstageVolume: volume_id and staging_target_path are required")
	}
	release, err := s.locks.acquire("volume", req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.node.UnstageVolume(ctx, &driver.UnstageRequest{
		VolumeID:          req.GetVolumeId(),
		StagingTargetPath: req.GetStagingTargetPath(),
	}); err != nil {
		return nil, err
	}
	return &csipb.NodeUnstageVolumeResponse{}, nil
}

func (s *nodeServer) NodePublishVolume(ctx context.Context, req *csipb.NodePublishVolumeRequest) (*csipb.NodePublishVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetTargetPath() == "" {
		return nil, driver.InvalidArgument("NodePublishVolume: volume_id and target_path are required")
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
	if err := s.node.PublishVolume(ctx, &driver.PublishRequest{
		VolumeID:          req.GetVolumeId(),
		StagingTargetPath: req.GetStagingTargetPath(),
		TargetPath:        req.GetTargetPath(),
		VolumeCapability:  cap,
		VolumeContext:     req.GetVolumeContext(),
		PublishContext:    req.GetPublishContext(),
		Readonly:          req.GetReadonly(),
	}); err != nil {
		return nil, err
	}
	return &csipb.NodePublishVolumeResponse{}, nil
}

func (s *nodeServer) NodeUnpublishVolume(ctx context.Context, req *csipb.NodeUnpublishVolumeRequest) (*csipb.NodeUnpublishVolumeResponse, error) {
	if req.GetVolumeId() == "" || req.GetTargetPath() == "" {
		return nil, driver.InvalidArgument("NodeUnpublishVolume: volume_id and target_path are required")
	}
	release, err := s.locks.acquire("volume", req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	defer release()
	if err := s.node.UnpublishVolume(ctx, &driver.UnpublishRequest{
		VolumeID:   req.GetVolumeId(),
		TargetPath: req.GetTargetPath(),
	}); err != nil {
		return nil, err
	}
	return &csipb.NodeUnpublishVolumeResponse{}, nil
}

func (s *nodeServer) NodeExpandVolume(ctx context.Context, req *csipb.NodeExpandVolumeRequest) (*csipb.NodeExpandVolumeResponse, error) {
	if !s.caps.NodeExpand {
		return nil, driver.Unimplemented("NodeExpandVolume is not supported by the %s backend", s.backend.Name())
	}
	if req.GetVolumeId() == "" || req.GetVolumePath() == "" {
		return nil, driver.InvalidArgument("NodeExpandVolume: volume_id and volume_path are required")
	}
	release, err := s.locks.acquire("volume", req.GetVolumeId())
	if err != nil {
		return nil, err
	}
	defer release()
	var cap driver.VolumeCapability
	if vc := req.GetVolumeCapability(); vc != nil {
		c, err := toVolumeCapability(vc)
		if err != nil {
			return nil, err
		}
		cap = c
	}
	newBytes, err := s.node.ExpandVolume(ctx, &driver.NodeExpandRequest{
		VolumeID:          req.GetVolumeId(),
		VolumePath:        req.GetVolumePath(),
		StagingTargetPath: req.GetStagingTargetPath(),
		CapacityRange:     toCapacityRange(req.GetCapacityRange()),
		VolumeCapability:  cap,
	})
	if err != nil {
		return nil, err
	}
	return &csipb.NodeExpandVolumeResponse{CapacityBytes: newBytes}, nil
}

func (s *nodeServer) NodeGetVolumeStats(ctx context.Context, req *csipb.NodeGetVolumeStatsRequest) (*csipb.NodeGetVolumeStatsResponse, error) {
	if !s.caps.NodeVolumeStats {
		return nil, driver.Unimplemented("NodeGetVolumeStats is not supported by the %s backend", s.backend.Name())
	}
	if req.GetVolumeId() == "" || req.GetVolumePath() == "" {
		return nil, driver.InvalidArgument("NodeGetVolumeStats: volume_id and volume_path are required")
	}
	st, err := s.node.GetVolumeStats(ctx, req.GetVolumeId(), req.GetVolumePath())
	if err != nil {
		return nil, err
	}
	return &csipb.NodeGetVolumeStatsResponse{
		Usage: []*csipb.VolumeUsage{
			{Unit: csipb.VolumeUsage_BYTES, Total: st.TotalBytes, Used: st.UsedBytes, Available: st.AvailableBytes},
			{Unit: csipb.VolumeUsage_INODES, Total: st.TotalInodes, Used: st.UsedInodes, Available: st.FreeInodes},
		},
	}, nil
}

func (s *nodeServer) NodeGetCapabilities(_ context.Context, _ *csipb.NodeGetCapabilitiesRequest) (*csipb.NodeGetCapabilitiesResponse, error) {
	return &csipb.NodeGetCapabilitiesResponse{Capabilities: nodeCapabilities(s.caps)}, nil
}

func (s *nodeServer) NodeGetInfo(ctx context.Context, _ *csipb.NodeGetInfoRequest) (*csipb.NodeGetInfoResponse, error) {
	info, err := s.node.GetInfo(ctx)
	if err != nil {
		return nil, err
	}
	resp := &csipb.NodeGetInfoResponse{
		NodeId:            info.NodeID,
		MaxVolumesPerNode: info.MaxVolumesPerNode,
	}
	if info.AccessibleTopology != nil {
		resp.AccessibleTopology = &csipb.Topology{Segments: info.AccessibleTopology.Segments}
	}
	return resp, nil
}

func nodeCapabilities(c driver.Capabilities) []*csipb.NodeServiceCapability {
	var rpcs []csipb.NodeServiceCapability_RPC_Type
	if c.NodeStage {
		rpcs = append(rpcs, csipb.NodeServiceCapability_RPC_STAGE_UNSTAGE_VOLUME)
	}
	if c.NodeExpand {
		rpcs = append(rpcs, csipb.NodeServiceCapability_RPC_EXPAND_VOLUME)
	}
	if c.NodeVolumeStats {
		rpcs = append(rpcs, csipb.NodeServiceCapability_RPC_GET_VOLUME_STATS)
	}
	out := make([]*csipb.NodeServiceCapability, 0, len(rpcs))
	for _, t := range rpcs {
		out = append(out, &csipb.NodeServiceCapability{
			Type: &csipb.NodeServiceCapability_Rpc{
				Rpc: &csipb.NodeServiceCapability_RPC{Type: t},
			},
		})
	}
	return out
}
