package local

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
	"github.com/honest-hosting/nomad-csi-driver/internal/zfs"
)

func newTestNode(fr cexec.Runner) *node {
	return &node{
		cfg:           testConfig(),
		z:             zfs.New(fr),
		nodeID:        "A",
		parentDataset: "nomad-csi", // deployment default; testConfig's tank overrides to "csi"
		mounter:       mountutil.New(fr, zap.NewNop()),
		log:           zap.NewNop(),
		waitForPath:   func(_ context.Context, p string) (string, error) { return p, nil },
		zvolDevices:   func() map[string]struct{} { return nil }, // overridden per-test
	}
}

func TestStagedCount(t *testing.T) {
	// This plugin's zvol device set: v1 by its /dev/zvol symlink, v2 by its
	// resolved /dev/zdN device (the form findmnt actually reports). Mounts: v1's
	// staging mount (symlink form), v1's publish bind-mount (source = staging dir,
	// not the zvol → counted once), v2's staging mount (/dev/zd0 form), the root
	// fs, and a FOREIGN plugin's zvol that is NOT in our set.
	const findmnt = `/ /dev/vda1
/opt/nomad/.../staging/v1/rw-file-system /dev/zvol/tank/csi/v1
/opt/nomad/.../per-alloc/a/v1/rw /opt/nomad/.../staging/v1/rw-file-system
/opt/nomad/.../staging/v2/rw-file-system /dev/zd0
/opt/nomad/.../staging/other/rw-file-system /dev/zvol/tank/other-plugin/z9
`
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if c.Name == "findmnt" {
			return cexec.Output{Stdout: []byte(findmnt)}, nil
		}
		return cexec.Output{}, nil
	}}
	n := newTestNode(fr)
	n.zvolDevices = func() map[string]struct{} {
		return map[string]struct{}{"/dev/zvol/tank/csi/v1": {}, "/dev/zd0": {}}
	}
	got, err := n.StagedCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, got, "count this plugin's two staged zvols (symlink + resolved form) once each; exclude bind-mount, root fs, and foreign zvol")
}

func TestStagedCountEmptyAndError(t *testing.T) {
	// No mounts → 0.
	empty := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		return cexec.Output{}, &cexec.Error{Name: "findmnt", ExitCode: 1}
	}}
	got, err := newTestNode(empty).StagedCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, got)

	// findmnt hard error → surfaced (the gauge holds last-good).
	boom := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		return cexec.Output{}, &cexec.Error{Name: "findmnt", ExitCode: 127}
	}}
	_, err = newTestNode(boom).StagedCount(context.Background())
	require.Error(t, err)
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

func TestNodeGetInfo_AdvertisesTopology(t *testing.T) {
	n := newTestNode(&cexec.FakeRunner{})
	info, err := n.GetInfo(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "A", info.NodeID)
	require.NotNil(t, info.AccessibleTopology)
	assert.Equal(t, "A", info.AccessibleTopology.Segments[topologyKey])
}
