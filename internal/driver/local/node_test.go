package local

import (
	"context"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
	"github.com/honest-hosting/nomad-csi-driver/internal/zfs"
)

// gaugeValue gathers reg and returns the value of a single-series gauge by name.
func gaugeValue(t *testing.T, reg *prometheus.Registry, name string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		require.Len(t, mf.GetMetric(), 1)
		return mf.GetMetric()[0].GetGauge().GetValue()
	}
	t.Fatalf("gauge %q not found", name)
	return 0
}

func newTestNode(fr cexec.Runner) *node {
	return &node{
		cfg:         testConfig(),
		z:           zfs.New(fr),
		nodeID:      "A",
		mounter:     mountutil.New(fr, zap.NewNop()),
		log:         zap.NewNop(),
		waitForPath: func(_ context.Context, p string) (string, error) { return p, nil },
	}
}

func TestNodeStage_FormatsAndMounts(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		switch c.Name {
		case "blkid":
			return cexec.Output{}, &cexec.Error{ExitCode: 2} // empty
		case "findmnt":
			return cexec.Output{}, &cexec.Error{ExitCode: 1} // not mounted
		case "zpool": // pool status probe (checkpoint 3)
			return cexec.Output{Stdout: []byte("ONLINE\n")}, nil
		}
		return cexec.Output{}, nil
	}}
	n := newTestNode(fr)
	err := n.StageVolume(context.Background(), &driver.StageRequest{
		VolumeID:          externalID{Node: "A", Dataset: "tank/csi/v1"}.String(),
		StagingTargetPath: "/tmp/csi-local-stage",
		VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeMount, FsType: "ext4"},
		VolumeContext:     map[string]string{ctxKeyDataset: "tank/csi/v1", ctxKeyFsType: "ext4", ctxKeyNode: "A"},
	})
	require.NoError(t, err)
	joined := strings.Join(fr.Commands(), "\n")
	assert.Contains(t, joined, "mkfs.ext4 -F /dev/zvol/tank/csi/v1")
	assert.Contains(t, joined, "mount -t ext4 /dev/zvol/tank/csi/v1 /tmp/csi-local-stage")
}

func TestNodeStage_WrongNodeGuard(t *testing.T) {
	n := newTestNode(&cexec.FakeRunner{})
	err := n.StageVolume(context.Background(), &driver.StageRequest{
		VolumeID:          externalID{Node: "B", Dataset: "tank/csi/v1"}.String(),
		StagingTargetPath: "/tmp/x",
		VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeMount, FsType: "ext4"},
		VolumeContext:     map[string]string{ctxKeyDataset: "tank/csi/v1", ctxKeyNode: "B"}, // owner B != local A
	})
	require.Error(t, err)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeFailedPrecondition, de.Code)
}

func TestNodeStage_StagedGauge(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		switch c.Name {
		case "blkid":
			return cexec.Output{}, &cexec.Error{ExitCode: 2} // empty → format
		case "findmnt":
			return cexec.Output{}, &cexec.Error{ExitCode: 1} // not mounted
		case "zpool":
			return cexec.Output{Stdout: []byte("ONLINE\n")}, nil
		}
		return cexec.Output{}, nil
	}}
	reg := prometheus.NewRegistry()
	nm := metrics.NewNodeMetrics(reg)
	n := newTestNode(fr)
	n.nodeM = nm
	n.mounter = mountutil.New(fr, zap.NewNop()).WithMetrics(nm)

	const name = "nomad_csi_node_staged_volumes"
	stage := &driver.StageRequest{
		VolumeID:          externalID{Node: "A", Dataset: "tank/csi/v1"}.String(),
		StagingTargetPath: "/tmp/csi-local-stage",
		VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeMount, FsType: "ext4"},
		VolumeContext:     map[string]string{ctxKeyDataset: "tank/csi/v1", ctxKeyFsType: "ext4", ctxKeyNode: "A"},
	}
	require.NoError(t, n.StageVolume(context.Background(), stage))
	assert.Equal(t, 1.0, gaugeValue(t, reg, name), "staged gauge should be 1 after a successful stage")

	require.NoError(t, n.UnstageVolume(context.Background(), &driver.UnstageRequest{
		VolumeID:          stage.VolumeID,
		StagingTargetPath: stage.StagingTargetPath,
	}))
	assert.Equal(t, 0.0, gaugeValue(t, reg, name), "staged gauge should return to 0 after unstage")
}

func TestNodeStage_StagedGaugeUnchangedOnFailure(t *testing.T) {
	reg := prometheus.NewRegistry()
	nm := metrics.NewNodeMetrics(reg)
	n := newTestNode(&cexec.FakeRunner{})
	n.nodeM = nm
	// Wrong-node guard fires before any mount work → stage fails, gauge stays 0.
	err := n.StageVolume(context.Background(), &driver.StageRequest{
		VolumeID:          externalID{Node: "B", Dataset: "tank/csi/v1"}.String(),
		StagingTargetPath: "/tmp/x",
		VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeMount, FsType: "ext4"},
		VolumeContext:     map[string]string{ctxKeyDataset: "tank/csi/v1", ctxKeyNode: "B"},
	})
	require.Error(t, err)
	assert.Equal(t, 0.0, gaugeValue(t, reg, "nomad_csi_node_staged_volumes"))
}

func TestNodeGetInfo_AdvertisesTopology(t *testing.T) {
	n := newTestNode(&cexec.FakeRunner{})
	info, err := n.GetInfo(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "A", info.NodeID)
	require.NotNil(t, info.AccessibleTopology)
	assert.Equal(t, "A", info.AccessibleTopology.Segments[topologyKey])
}
