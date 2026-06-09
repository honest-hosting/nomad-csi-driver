package qnap

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
	"github.com/honest-hosting/nomad-csi-driver/internal/iscsi"
	"github.com/honest-hosting/nomad-csi-driver/internal/metrics"
	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
	"github.com/honest-hosting/nomad-csi-driver/internal/multipath"
)

// stagedGauge gathers reg and returns the single-series node_staged_volumes value.
func stagedGauge(t *testing.T, reg *prometheus.Registry) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "nomad_csi_node_staged_volumes" {
			continue
		}
		require.Len(t, mf.GetMetric(), 1)
		return mf.GetMetric()[0].GetGauge().GetValue()
	}
	t.Fatal("node_staged_volumes gauge not found")
	return 0
}

type memMetaStore struct{ m map[string]stageMeta }

func newMemMetaStore() *memMetaStore { return &memMetaStore{m: map[string]stageMeta{}} }
func (s *memMetaStore) Save(id string, sm stageMeta) error {
	s.m[id] = sm
	return nil
}
func (s *memMetaStore) Load(id string) (stageMeta, error) {
	v, ok := s.m[id]
	if !ok {
		return stageMeta{}, os.ErrNotExist
	}
	return v, nil
}
func (s *memMetaStore) Delete(id string) error {
	delete(s.m, id)
	return nil
}

func newTestNode(fr cexec.Runner, store metaStore) *node {
	return &node{
		cfg:          &config.QNAPConfig{},
		nodeID:       "n1",
		iscsi:        iscsi.New(fr),
		mpath:        multipath.New(fr, ""),
		mounter:      mountutil.New(fr, zap.NewNop()),
		meta:         store,
		useMultipath: true,
		log:          zap.NewNop(),
		waitForPath: func(_ context.Context, p string) (string, error) {
			if strings.Contains(p, "*") {
				return "/dev/sdg", nil // by-path glob -> resolved raw device
			}
			return p, nil
		},
	}
}

func mountStageReq() *driver.StageRequest {
	return &driver.StageRequest{
		VolumeID:          "v1",
		StagingTargetPath: "/tmp/csi-stage-v1",
		VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeMount, FsType: "ext4"},
		VolumeContext: map[string]string{
			ctxKeyPortal: "10.0.0.1:3260", ctxKeyIQN: "iqn.test:tgt", ctxKeyLUNNumber: "0", ctxKeyFsType: "ext4",
		},
	}
}

func TestNodeStage_FormatsMountsAndSavesMeta(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		switch c.Name {
		case "multipathd": // show paths format "%d %w" -> device wwid
			return cexec.Output{Stdout: []byte("sdg 3600wwid\n")}, nil
		case "blkid":
			return cexec.Output{}, &cexec.Error{Name: "blkid", ExitCode: 2} // empty device
		case "findmnt":
			return cexec.Output{}, &cexec.Error{Name: "findmnt", ExitCode: 1} // not mounted
		}
		return cexec.Output{}, nil
	}}
	store := newMemMetaStore()
	n := newTestNode(fr, store)

	require.NoError(t, n.StageVolume(context.Background(), mountStageReq()))

	joined := strings.Join(fr.Commands(), "\n")
	assert.Contains(t, joined, "iscsiadm -m discovery")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.test:tgt -p 10.0.0.1:3260 --login")
	assert.Contains(t, joined, "mkfs.ext4 -F /dev/mapper/3600wwid")
	assert.Contains(t, joined, "mount -t ext4 /dev/mapper/3600wwid /tmp/csi-stage-v1")

	meta, err := store.Load("v1")
	require.NoError(t, err)
	assert.Equal(t, "3600wwid", meta.WWID)
	assert.Equal(t, "iqn.test:tgt", meta.IQN)
}

func TestNodeUnstage_FlushesAndLogsOut(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if c.Name == "findmnt" {
			return cexec.Output{Stdout: []byte("/tmp/csi-stage-v1")}, nil // mounted
		}
		return cexec.Output{}, nil
	}}
	store := newMemMetaStore()
	_ = store.Save("v1", stageMeta{Portal: "10.0.0.1:3260", IQN: "iqn.test:tgt", WWID: "3600wwid"})
	n := newTestNode(fr, store)

	require.NoError(t, n.UnstageVolume(context.Background(), &driver.UnstageRequest{
		VolumeID: "v1", StagingTargetPath: "/tmp/csi-stage-v1",
	}))

	joined := strings.Join(fr.Commands(), "\n")
	assert.Contains(t, joined, "umount /tmp/csi-stage-v1")
	assert.Contains(t, joined, "multipath -f 3600wwid")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.test:tgt -p 10.0.0.1:3260 --logout")

	_, err := store.Load("v1")
	assert.Error(t, err, "metadata removed after unstage")
}

func TestNodeStage_StagedGauge(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		switch c.Name {
		case "multipathd":
			return cexec.Output{Stdout: []byte("sdg 3600wwid\n")}, nil
		case "blkid":
			return cexec.Output{}, &cexec.Error{Name: "blkid", ExitCode: 2}
		case "findmnt":
			return cexec.Output{}, &cexec.Error{Name: "findmnt", ExitCode: 1}
		}
		return cexec.Output{}, nil
	}}
	reg := prometheus.NewRegistry()
	nm := metrics.NewNodeMetrics(reg)
	store := newMemMetaStore()
	n := newTestNode(fr, store)
	n.nodeM = nm
	n.mounter = mountutil.New(fr, zap.NewNop()).WithMetrics(nm)

	require.NoError(t, n.StageVolume(context.Background(), mountStageReq()))
	assert.Equal(t, 1.0, stagedGauge(t, reg), "staged gauge should be 1 after stage")

	require.NoError(t, n.UnstageVolume(context.Background(), &driver.UnstageRequest{
		VolumeID: "v1", StagingTargetPath: "/tmp/csi-stage-v1",
	}))
	assert.Equal(t, 0.0, stagedGauge(t, reg), "staged gauge should return to 0 after unstage")
}

// Two portals in the volume context => log into both (two paths for multipath),
// and record both in the stage metadata.
func TestNodeStage_MultiPortalLogsIntoAll(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		switch c.Name {
		case "multipathd":
			return cexec.Output{Stdout: []byte("sdg 3600wwid\n")}, nil
		case "blkid":
			return cexec.Output{}, &cexec.Error{Name: "blkid", ExitCode: 2}
		case "findmnt":
			return cexec.Output{}, &cexec.Error{Name: "findmnt", ExitCode: 1}
		}
		return cexec.Output{}, nil
	}}
	store := newMemMetaStore()
	n := newTestNode(fr, store)
	req := mountStageReq()
	req.VolumeContext[ctxKeyPortal] = "10.0.0.1:3260,10.0.1.1:3260"

	require.NoError(t, n.StageVolume(context.Background(), req))

	joined := strings.Join(fr.Commands(), "\n")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.test:tgt -p 10.0.0.1:3260 --login")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.test:tgt -p 10.0.1.1:3260 --login")

	meta, err := store.Load("v1")
	require.NoError(t, err)
	assert.Equal(t, []string{"10.0.0.1:3260", "10.0.1.1:3260"}, meta.Portals, "both portals recorded")
}

// stageRunner answers the iscsi/multipath/blkid/findmnt commands a stage issues.
// failLoginPortal (if set) makes that portal's --login fail; failAllLogins makes
// every login fail.
func stageRunner(failLoginPortal string, failAllLogins bool) *cexec.FakeRunner {
	return &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		joined := c.Name + " " + strings.Join(c.Args, " ")
		switch {
		case c.Name == "multipathd":
			return cexec.Output{Stdout: []byte("sdg 3600wwid\n")}, nil
		case c.Name == "blkid":
			return cexec.Output{}, &cexec.Error{Name: "blkid", ExitCode: 2} // empty device
		case c.Name == "findmnt":
			return cexec.Output{}, &cexec.Error{Name: "findmnt", ExitCode: 1} // not mounted
		case strings.Contains(joined, "--login") && (failAllLogins || (failLoginPortal != "" && strings.Contains(joined, failLoginPortal))):
			return cexec.Output{}, &cexec.Error{Name: "iscsiadm", ExitCode: 8}
		}
		return cexec.Output{}, nil
	}}
}

func TestNodeMetrics_StageOutcomes(t *testing.T) {
	twoPortals := func() *driver.StageRequest {
		r := mountStageReq()
		r.VolumeContext[ctxKeyPortal] = "10.0.0.1:3260,10.0.1.1:3260"
		return r
	}

	t.Run("all paths up -> stage ok", func(t *testing.T) {
		n := newTestNode(stageRunner("", false), newMemMetaStore())
		n.metrics = newQNAPNodeMetrics(prometheus.NewRegistry())
		require.NoError(t, n.StageVolume(context.Background(), twoPortals()))
		assert.Equal(t, 2.0, testutil.ToFloat64(n.metrics.iscsiLogin.WithLabelValues("ok")))
		assert.Equal(t, 1.0, testutil.ToFloat64(n.metrics.stage.WithLabelValues("ok")))
	})

	t.Run("one portal down -> stage degraded", func(t *testing.T) {
		n := newTestNode(stageRunner("10.0.1.1", false), newMemMetaStore())
		n.metrics = newQNAPNodeMetrics(prometheus.NewRegistry())
		require.NoError(t, n.StageVolume(context.Background(), twoPortals()), "still mounts on the one good path")
		assert.Equal(t, 1.0, testutil.ToFloat64(n.metrics.iscsiLogin.WithLabelValues("ok")))
		assert.Equal(t, 1.0, testutil.ToFloat64(n.metrics.iscsiLogin.WithLabelValues("fail")))
		assert.Equal(t, 1.0, testutil.ToFloat64(n.metrics.stage.WithLabelValues("degraded")))
	})

	t.Run("all portals down -> stage failed", func(t *testing.T) {
		n := newTestNode(stageRunner("", true), newMemMetaStore())
		n.metrics = newQNAPNodeMetrics(prometheus.NewRegistry())
		require.Error(t, n.StageVolume(context.Background(), twoPortals()))
		assert.Equal(t, 2.0, testutil.ToFloat64(n.metrics.iscsiLogin.WithLabelValues("fail")))
		assert.Equal(t, 1.0, testutil.ToFloat64(n.metrics.stage.WithLabelValues("failed")))
	})
}

func TestNodeUnstage_MultiPortalLogsOutAll(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if c.Name == "findmnt" {
			return cexec.Output{Stdout: []byte("/tmp/csi-stage-v1")}, nil
		}
		return cexec.Output{}, nil
	}}
	store := newMemMetaStore()
	_ = store.Save("v1", stageMeta{Portals: []string{"10.0.0.1:3260", "10.0.1.1:3260"}, IQN: "iqn.test:tgt", WWID: "3600wwid"})
	n := newTestNode(fr, store)

	require.NoError(t, n.UnstageVolume(context.Background(), &driver.UnstageRequest{
		VolumeID: "v1", StagingTargetPath: "/tmp/csi-stage-v1",
	}))

	joined := strings.Join(fr.Commands(), "\n")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.test:tgt -p 10.0.0.1:3260 --logout")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.test:tgt -p 10.0.1.1:3260 --logout")
}

// When a stage fails after the iSCSI session is up (here: the multipath mapper
// device never appears), the established session and the claimed map must be
// torn down so retries don't leak sessions/devices — and no metadata is saved.
func TestNodeStage_CleansUpSessionsOnFailure(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if c.Name == "multipathd" {
			return cexec.Output{Stdout: []byte("sdg 3600wwid\n")}, nil
		}
		return cexec.Output{}, nil
	}}
	store := newMemMetaStore()
	n := newTestNode(fr, store)
	// Raw device resolves; the multipath mapper device never does.
	n.waitForPath = func(_ context.Context, p string) (string, error) {
		switch {
		case strings.Contains(p, "*"):
			return "/dev/sdg", nil // raw by-path glob
		case strings.Contains(p, "/dev/mapper/"):
			return "", context.DeadlineExceeded // mapper never appears
		default:
			return p, nil
		}
	}

	err := n.StageVolume(context.Background(), mountStageReq())
	require.Error(t, err)

	joined := strings.Join(fr.Commands(), "\n")
	assert.Contains(t, joined, "multipath -f 3600wwid", "claimed multipath map must be flushed on failure")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.test:tgt -p 10.0.0.1:3260 --logout", "session must be logged out on failure")

	_, lerr := store.Load("v1")
	assert.Error(t, lerr, "no stage metadata is persisted on failure")
}

func TestNodePublishFilesystem_BindMounts(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if c.Name == "findmnt" {
			return cexec.Output{}, &cexec.Error{Name: "findmnt", ExitCode: 1} // target not mounted
		}
		return cexec.Output{}, nil
	}}
	n := newTestNode(fr, newMemMetaStore())
	target := t.TempDir() + "/target"
	err := n.PublishVolume(context.Background(), &driver.PublishRequest{
		VolumeID:          "v1",
		StagingTargetPath: t.TempDir() + "/stage",
		TargetPath:        target,
		VolumeCapability:  driver.VolumeCapability{AccessType: driver.AccessTypeMount, FsType: "ext4"},
	})
	require.NoError(t, err)
	assert.Contains(t, strings.Join(fr.Commands(), "\n"), "mount --bind")
}

func TestNodeGetInfo_NoTopology(t *testing.T) {
	n := newTestNode(&cexec.FakeRunner{}, newMemMetaStore())
	info, err := n.GetInfo(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "n1", info.NodeID)
	assert.Nil(t, info.AccessibleTopology, "qnap advertises no topology constraint")
}

func TestResolvePath_GlobMatchesByIQNAndLUN(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "sdg")
	require.NoError(t, os.WriteFile(target, []byte{}, 0o600))
	want, err := filepath.EvalSymlinks(target)
	require.NoError(t, err)

	// udev-style by-path link named with the portal IP (not the hostname).
	link := filepath.Join(dir, "ip-172.16.46.69:3260-iscsi-iqn.test:tgt-lun-0")
	require.NoError(t, os.Symlink(target, link))

	// Exact path resolves.
	got, ok := resolvePath(link)
	require.True(t, ok)
	assert.Equal(t, want, got)

	// Glob by IQN+LUN matches the IP-named link (portal-agnostic).
	got, ok = resolvePath(filepath.Join(dir, "*-iscsi-iqn.test:tgt-lun-0"))
	require.True(t, ok)
	assert.Equal(t, want, got)

	// No match returns false.
	_, ok = resolvePath(filepath.Join(dir, "*-iscsi-iqn.nope-lun-9"))
	assert.False(t, ok)
}
