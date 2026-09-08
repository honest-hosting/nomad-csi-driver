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
		zvolDatasets:  func() map[string]string { return nil }, // overridden per-test
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
	n.zvolDatasets = func() map[string]string {
		return map[string]string{"/dev/zvol/tank/csi/v1": "tank/csi/v1", "/dev/zd0": "tank/csi/v2"}
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

// --- volume-context loss (NOMAD-CSI-DRIVER-MISSING-CTX-FIX.PLAN.md) ----------
//
// Nomad erases a volume's CSI context when `nomad volume create` is run against
// an already-existing volume: the server merges the submitted spec instead of
// calling the plugin, and before Nomad 1.9.6 (hashicorp/nomad#24922) that merge
// assigned the spec's (empty) context unconditionally. The zvol is untouched, so
// the node must rebuild what it needs from the external id rather than refuse.

// stageRunner fakes just enough for a filesystem stage to reach mkfs/mount.
func stageRunner() *cexec.FakeRunner {
	return &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		switch c.Name {
		case "blkid":
			return cexec.Output{}, &cexec.Error{ExitCode: 2} // empty device
		case "findmnt":
			return cexec.Output{}, &cexec.Error{ExitCode: 1} // not mounted
		case "zpool":
			return cexec.Output{Stdout: []byte("ONLINE\n")}, nil
		}
		return cexec.Output{}, nil
	}}
}

func TestNodeStage_RebuildsIdentityFromExternalID(t *testing.T) {
	// Every shape of a lost/partial context. In each case the external id names
	// node A (the local node) and dataset tank/csi/v1, so the mount must land on
	// exactly the device a healthy context would have produced.
	cases := []struct {
		name string
		vctx map[string]string
	}{
		{"context wiped (nil map)", nil},
		{"context wiped (empty map)", map[string]string{}},
		{"dataset only", map[string]string{ctxKeyDataset: "tank/csi/v1"}},
		{"node only", map[string]string{ctxKeyNode: "A"}},
		{"unrelated keys only", map[string]string{"someOtherKey": "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fr := stageRunner()
			n := newTestNode(fr)
			err := n.StageVolume(context.Background(), &driver.StageRequest{
				VolumeID:          externalID{Node: "A", Dataset: "tank/csi/v1"}.String(),
				StagingTargetPath: "/tmp/csi-local-stage",
				VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeMount, FsType: "ext4"},
				VolumeContext:     tc.vctx,
			})
			require.NoError(t, err, "a wiped context must not break a healthy volume")
			joined := strings.Join(fr.Commands(), "\n")
			assert.Contains(t, joined, "mkfs.ext4 -F /dev/zvol/tank/csi/v1")
			assert.Contains(t, joined, "mount -t ext4 /dev/zvol/tank/csi/v1 /tmp/csi-local-stage")
		})
	}
}

func TestNodeStage_RebuiltOwnerStillEnforcesWrongNodeGuard(t *testing.T) {
	// The regression this closes: the guard used to be skipped entirely when the
	// context carried no owner, which is precisely the state a context wipe
	// leaves behind. With the owner rebuilt from the external id it must fire.
	n := newTestNode(stageRunner())
	err := n.StageVolume(context.Background(), &driver.StageRequest{
		VolumeID:          externalID{Node: "B", Dataset: "tank/csi/v1"}.String(), // owned by B, we are A
		StagingTargetPath: "/tmp/x",
		VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeMount, FsType: "ext4"},
		VolumeContext:     map[string]string{}, // wiped
	})
	require.Error(t, err)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeFailedPrecondition, de.Code, "must refuse to stage another node's volume even with no context")
}

func TestNodeStage_UnparseableIDWithNoContextIsDiagnosable(t *testing.T) {
	n := newTestNode(stageRunner())
	err := n.StageVolume(context.Background(), &driver.StageRequest{
		VolumeID:          "not-a-local-volume-id",
		StagingTargetPath: "/tmp/x",
		VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeMount, FsType: "ext4"},
		VolumeContext:     map[string]string{},
	})
	require.Error(t, err)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeInvalidArgument, de.Code)
	assert.Contains(t, err.Error(), ctxKeyDataset, "error should name the missing context key")
	assert.Contains(t, err.Error(), "not-a-local-volume-id", "and the id it could not parse")
}

func TestNodeStage_ContextWinsOverExternalID(t *testing.T) {
	// A populated context is authoritative and must not be second-guessed: the
	// rebuild is a fallback, not a cross-check.
	fr := stageRunner()
	n := newTestNode(fr)
	err := n.StageVolume(context.Background(), &driver.StageRequest{
		VolumeID:          externalID{Node: "A", Dataset: "tank/csi/from-id"}.String(),
		StagingTargetPath: "/tmp/csi-local-stage",
		VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeMount, FsType: "ext4"},
		VolumeContext:     map[string]string{ctxKeyDataset: "tank/csi/from-ctx", ctxKeyNode: "A"},
	})
	require.NoError(t, err)
	joined := strings.Join(fr.Commands(), "\n")
	assert.Contains(t, joined, "/dev/zvol/tank/csi/from-ctx")
	assert.NotContains(t, joined, "from-id")
}

func TestNodePublishBlock_RebuildsDatasetFromExternalID(t *testing.T) {
	// The block publish path read the dataset straight from the context with no
	// empty check, so a wipe produced a bare device-not-found instead of a mount.
	fr := stageRunner()
	n := newTestNode(fr)
	err := n.PublishVolume(context.Background(), &driver.PublishRequest{
		VolumeID:          externalID{Node: "A", Dataset: "tank/csi/v1"}.String(),
		StagingTargetPath: "/tmp/csi-local-stage",
		TargetPath:        "/tmp/csi-local-target",
		VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeBlock},
		VolumeContext:     map[string]string{},
	})
	require.NoError(t, err)
	assert.Contains(t, strings.Join(fr.Commands(), "\n"), "/dev/zvol/tank/csi/v1")
}

func TestResolveVolumeIdentity_FsTypeStillFallsBackToCapability(t *testing.T) {
	// fsType is the one context key with no external-id equivalent; it already
	// degrades to the capability's value. Guard that behaviour.
	fr := stageRunner()
	n := newTestNode(fr)
	err := n.StageVolume(context.Background(), &driver.StageRequest{
		VolumeID:          externalID{Node: "A", Dataset: "tank/csi/v1"}.String(),
		StagingTargetPath: "/tmp/csi-local-stage",
		VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeMount, FsType: "xfs"},
		VolumeContext:     map[string]string{}, // no fsType either
	})
	require.NoError(t, err)
	assert.Contains(t, strings.Join(fr.Commands(), "\n"), "mkfs.xfs")
}

func TestNodeStage_ProbesFsTypeWhenContextAndCapabilityHaveNone(t *testing.T) {
	// The full ncdl-database shape after a context wipe: no fsType in the context
	// (erased) and none in the capability (the volume spec has no
	// mount_options.fs_type, so Nomad sends an empty one). Without the probe,
	// FormatIfEmpty compares the existing ext4 against "" and REFUSES, and Mount
	// would run `mount -t ""`.
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		switch c.Name {
		case "blkid":
			return cexec.Output{Stdout: []byte("ext4\n")}, nil // already formatted
		case "findmnt":
			return cexec.Output{}, &cexec.Error{ExitCode: 1} // not mounted
		case "zpool":
			return cexec.Output{Stdout: []byte("ONLINE\n")}, nil
		}
		return cexec.Output{}, nil
	}}
	n := newTestNode(fr)
	err := n.StageVolume(context.Background(), &driver.StageRequest{
		VolumeID:          externalID{Node: "A", Dataset: "tank/csi/v1"}.String(),
		StagingTargetPath: "/tmp/csi-local-stage",
		VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeMount}, // no FsType
		VolumeContext:     map[string]string{},                                         // wiped
	})
	require.NoError(t, err, "a wiped context plus an fs_type-less spec must still mount")
	joined := strings.Join(fr.Commands(), "\n")
	assert.Contains(t, joined, "mount -t ext4 /dev/zvol/tank/csi/v1 /tmp/csi-local-stage",
		"the probed filesystem must be used for the mount")
	assert.NotContains(t, joined, "mkfs", "an already-formatted zvol must never be reformatted")
}

func TestNodeStage_UnformattedDeviceStillUsesCapabilityFsType(t *testing.T) {
	// Guard the probe against overreach: on a brand-new zvol blkid reports
	// nothing, so the capability's fsType must still drive the format.
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		switch c.Name {
		case "blkid":
			return cexec.Output{}, &cexec.Error{ExitCode: 2} // empty device
		case "findmnt":
			return cexec.Output{}, &cexec.Error{ExitCode: 1}
		case "zpool":
			return cexec.Output{Stdout: []byte("ONLINE\n")}, nil
		}
		return cexec.Output{}, nil
	}}
	n := newTestNode(fr)
	err := n.StageVolume(context.Background(), &driver.StageRequest{
		VolumeID:          externalID{Node: "A", Dataset: "tank/csi/v1"}.String(),
		StagingTargetPath: "/tmp/csi-local-stage",
		VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeMount, FsType: "ext4"},
		VolumeContext:     map[string]string{},
	})
	require.NoError(t, err)
	assert.Contains(t, strings.Join(fr.Commands(), "\n"), "mkfs.ext4 -F /dev/zvol/tank/csi/v1")
}
