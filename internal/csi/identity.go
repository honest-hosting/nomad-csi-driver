package csi

import (
	"context"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	"github.com/honest-hosting/nomad-csi-driver/internal/version"
)

// identityServer implements the CSI Identity service for the active backend.
type identityServer struct {
	csipb.UnimplementedIdentityServer
	backend driver.Backend
}

func newIdentityServer(b driver.Backend) *identityServer {
	return &identityServer{backend: b}
}

func (s *identityServer) GetPluginInfo(_ context.Context, _ *csipb.GetPluginInfoRequest) (*csipb.GetPluginInfoResponse, error) {
	return &csipb.GetPluginInfoResponse{
		Name:          s.backend.PluginName(),
		VendorVersion: version.Version,
	}, nil
}

func (s *identityServer) GetPluginCapabilities(_ context.Context, _ *csipb.GetPluginCapabilitiesRequest) (*csipb.GetPluginCapabilitiesResponse, error) {
	caps := s.backend.Capabilities()
	var out []*csipb.PluginCapability

	if caps.CreateDelete || caps.PublishUnpublish || caps.Expand || caps.Snapshot {
		out = append(out, &csipb.PluginCapability{
			Type: &csipb.PluginCapability_Service_{
				Service: &csipb.PluginCapability_Service{Type: csipb.PluginCapability_Service_CONTROLLER_SERVICE},
			},
		})
	}
	if caps.Topology {
		out = append(out, &csipb.PluginCapability{
			Type: &csipb.PluginCapability_Service_{
				Service: &csipb.PluginCapability_Service{Type: csipb.PluginCapability_Service_VOLUME_ACCESSIBILITY_CONSTRAINTS},
			},
		})
	}
	if caps.Expand {
		out = append(out, &csipb.PluginCapability{
			Type: &csipb.PluginCapability_VolumeExpansion_{
				VolumeExpansion: &csipb.PluginCapability_VolumeExpansion{Type: expansionType(caps.ExpandOnline)},
			},
		})
	}
	return &csipb.GetPluginCapabilitiesResponse{Capabilities: out}, nil
}

func expansionType(online bool) csipb.PluginCapability_VolumeExpansion_Type {
	if online {
		return csipb.PluginCapability_VolumeExpansion_ONLINE
	}
	return csipb.PluginCapability_VolumeExpansion_OFFLINE
}

func (s *identityServer) Probe(ctx context.Context, _ *csipb.ProbeRequest) (*csipb.ProbeResponse, error) {
	if err := s.backend.Probe(ctx); err != nil {
		return nil, err
	}
	return &csipb.ProbeResponse{Ready: wrapperspb.Bool(true)}, nil
}
