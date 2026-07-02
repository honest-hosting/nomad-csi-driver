package qnap

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	goqnap "github.com/honest-hosting/go-qnap"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/honest-hosting/nomad-csi-driver/internal/config"
	"github.com/honest-hosting/nomad-csi-driver/internal/driver"
	cexec "github.com/honest-hosting/nomad-csi-driver/internal/exec"
	"github.com/honest-hosting/nomad-csi-driver/internal/iscsi"
	"github.com/honest-hosting/nomad-csi-driver/internal/mountutil"
	"github.com/honest-hosting/nomad-csi-driver/internal/multipath"
)

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

func TestStagedCount(t *testing.T) {
	// This plugin's portals are the two below; a THIRD target reached over a
	// portal we don't own (another qnap plugin's SAN) must be excluded. The owned
	// target is multipathed (two portals, LUN 0) → counted once; a second owned
	// LUN on a shared target counts separately.
	const sessP3 = `Target: iqn.qnap:mine.vol-a (non-flash)
	Current Portal: 10.0.0.1:3260,1
		Attached SCSI devices:
		scsi5 Channel 00 Id 0 Lun: 0
			Attached scsi disk sda		State: running
	Current Portal: 10.0.1.1:3260,1
		Attached SCSI devices:
		scsi6 Channel 00 Id 0 Lun: 0
			Attached scsi disk sdb		State: running
Target: iqn.qnap:mine.shared (non-flash)
	Current Portal: 10.0.0.1:3260,1
		Attached SCSI devices:
		scsi7 Channel 00 Id 0 Lun: 1
			Attached scsi disk sdc		State: running
Target: iqn.qnap:other.vol-z (non-flash)
	Current Portal: 192.168.9.9:3260,1
		Attached SCSI devices:
		scsi8 Channel 00 Id 0 Lun: 0
			Attached scsi disk sdz		State: running
`
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if c.Name == "iscsiadm" && len(c.Args) >= 2 && c.Args[1] == "session" {
			return cexec.Output{Stdout: []byte(sessP3)}, nil
		}
		return cexec.Output{}, nil
	}}
	n := newTestNode(fr, newMemMetaStore())
	// Config the portals WITHOUT ports to exercise normalization (session reports
	// ip:port; config here omits :3260).
	n.cfg = &config.QNAPConfig{Portals: []string{"10.0.0.1", "10.0.1.1"}}

	got, err := n.StagedCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, got, "two owned LUNs (multipathed vol-a once + shared LUN 1); foreign SAN excluded")
}

// The qnap NODE normally has no configured portals (it reads them per-volume),
// so scoping is disabled and every session counts.
func TestStagedCount_NoPortalsCountsAll(t *testing.T) {
	const sess = `Target: iqn.qnap:a (non-flash)
	Current Portal: 10.9.9.1:3260,1
		Attached SCSI devices:
		scsi5 Channel 00 Id 0 Lun: 0
			Attached scsi disk sda		State: running
Target: iqn.qnap:b (non-flash)
	Current Portal: 10.9.9.2:3260,1
		Attached SCSI devices:
		scsi6 Channel 00 Id 0 Lun: 0
			Attached scsi disk sdb		State: running
`
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		if c.Name == "iscsiadm" && len(c.Args) >= 2 && c.Args[1] == "session" {
			return cexec.Output{Stdout: []byte(sess)}, nil
		}
		return cexec.Output{}, nil
	}}
	n := newTestNode(fr, newMemMetaStore())
	n.cfg = &config.QNAPConfig{} // no portals — the real node config
	got, err := n.StagedCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, got, "with no configured portals, all host sessions count")
}

// Cold-cache fs teardown must also work with no configured portals (the real
// node config) — the leak the integration test caught.
func TestUnstage_Tier2_NoPortals(t *testing.T) {
	const sess = `Target: iqn.qnap:mine.vol-a (non-flash)
	Current Portal: 10.0.0.1:3260,1
		Attached SCSI devices:
		scsi5 Channel 00 Id 0 Lun: 0
			Attached scsi disk sda		State: running
`
	fr := teardownRunner("/dev/mapper/3600wwid", "/tmp/csi-stage-v1 /dev/mapper/3600wwid\n", sess, "sda 3600wwid\n")
	n := newTestNode(fr, newMemMetaStore())
	n.cfg = &config.QNAPConfig{} // no portals

	require.NoError(t, n.UnstageVolume(context.Background(), &driver.UnstageRequest{
		VolumeID: "v1", StagingTargetPath: "/tmp/csi-stage-v1",
	}))
	joined := strings.Join(fr.Commands(), "\n")
	assert.Contains(t, joined, "multipath -f 3600wwid")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.qnap:mine.vol-a -p 10.0.0.1:3260 --logout")
}

func TestStagedCountNoSessions(t *testing.T) {
	fr := &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		// exit 21 = no active sessions
		return cexec.Output{}, &cexec.Error{Name: "iscsiadm", ExitCode: 21}
	}}
	n := newTestNode(fr, newMemMetaStore())
	n.cfg = &config.QNAPConfig{Portals: []string{"10.0.0.1"}}
	got, err := n.StagedCount(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, got)
}

// teardownRunner answers the findmnt/iscsiadm/multipathd commands an unstage
// issues. sourceDev is what findmnt reports as the staging mount's SOURCE (empty
// → not mounted). listMounts is the raw `findmnt -rn -o TARGET,SOURCE` body.
// sessions is the `iscsiadm -m session -P 3` body. members is the `multipathd
// show paths` body.
func teardownRunner(sourceDev, listMounts, sessions, members string) *cexec.FakeRunner {
	return &cexec.FakeRunner{Responder: func(c cexec.Command) (cexec.Output, error) {
		joined := c.Name + " " + strings.Join(c.Args, " ")
		switch {
		case c.Name == "findmnt" && strings.Contains(joined, "SOURCE --target"): // SourceDevice
			if sourceDev == "" {
				return cexec.Output{}, &cexec.Error{Name: "findmnt", ExitCode: 1}
			}
			return cexec.Output{Stdout: []byte(sourceDev)}, nil
		case c.Name == "findmnt" && strings.Contains(joined, "TARGET,SOURCE"): // ListMounts
			return cexec.Output{Stdout: []byte(listMounts)}, nil
		case c.Name == "findmnt": // IsMounted (TARGET --target)
			if sourceDev == "" {
				return cexec.Output{}, &cexec.Error{Name: "findmnt", ExitCode: 1}
			}
			// IsMounted compares reported mountpoint to target; echo the target back.
			return cexec.Output{Stdout: []byte(c.Args[len(c.Args)-1])}, nil
		case c.Name == "iscsiadm" && len(c.Args) >= 2 && c.Args[1] == "session":
			return cexec.Output{Stdout: []byte(sessions)}, nil
		case c.Name == "multipathd":
			return cexec.Output{Stdout: []byte(members)}, nil
		}
		return cexec.Output{}, nil
	}}
}

// TestUnstage_Tier2_ReconstructsFromMount: cold cache, multipath fs staging
// mount → identity reconstructed from host, session logged out + map flushed.
func TestUnstage_Tier2_ReconstructsFromMount(t *testing.T) {
	const sess = `Target: iqn.qnap:mine.vol-a (non-flash)
	Current Portal: 10.0.0.1:3260,1
		Attached SCSI devices:
		scsi5 Channel 00 Id 0 Lun: 7
			Attached scsi disk sda		State: running
	Current Portal: 10.0.1.1:3260,1
		Attached SCSI devices:
		scsi6 Channel 00 Id 0 Lun: 7
			Attached scsi disk sdb		State: running
`
	fr := teardownRunner(
		"/dev/mapper/3600wwid",
		"/tmp/csi-stage-v1 /dev/mapper/3600wwid\n", // only the staging mount
		sess,
		"sda 3600wwid\nsdb 3600wwid\nsdz otherwwid\n",
	)
	n := newTestNode(fr, newMemMetaStore()) // empty cache → tier 2
	n.cfg = &config.QNAPConfig{Portals: []string{"10.0.0.1", "10.0.1.1"}}

	require.NoError(t, n.UnstageVolume(context.Background(), &driver.UnstageRequest{
		VolumeID: "v1", StagingTargetPath: "/tmp/csi-stage-v1",
	}))
	joined := strings.Join(fr.Commands(), "\n")
	assert.Contains(t, joined, "umount /tmp/csi-stage-v1")
	assert.Contains(t, joined, "multipath -f 3600wwid")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.qnap:mine.vol-a -p 10.0.0.1:3260 --logout")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.qnap:mine.vol-a -p 10.0.1.1:3260 --logout")
}

// TestUnstage_SharedTarget_RefcountsLogout: a 1:N target still serving another
// LUN must NOT be logged out (OQ2); our map is still flushed.
func TestUnstage_SharedTarget_RefcountsLogout(t *testing.T) {
	const sess = `Target: iqn.qnap:shared (non-flash)
	Current Portal: 10.0.0.1:3260,1
		Attached SCSI devices:
		scsi5 Channel 00 Id 0 Lun: 0
			Attached scsi disk sda		State: running
		scsi5 Channel 00 Id 0 Lun: 1
			Attached scsi disk sdb		State: running
`
	fr := teardownRunner("/dev/mapper/3600wwid", "/tmp/csi-stage-v1 /dev/mapper/3600wwid\n", sess, "sda 3600wwid\n")
	store := newMemMetaStore()
	_ = store.Save("v1", stageMeta{IQN: "iqn.qnap:shared", LUNNumber: 0, WWID: "3600wwid", Portals: []string{"10.0.0.1:3260"}})
	n := newTestNode(fr, store)
	n.cfg = &config.QNAPConfig{Portals: []string{"10.0.0.1"}}

	require.NoError(t, n.UnstageVolume(context.Background(), &driver.UnstageRequest{
		VolumeID: "v1", StagingTargetPath: "/tmp/csi-stage-v1",
	}))
	joined := strings.Join(fr.Commands(), "\n")
	assert.Contains(t, joined, "multipath -f 3600wwid", "our map is flushed")
	assert.NotContains(t, joined, "--logout", "shared target with another live LUN stays logged in")
}

// TestUnstage_InUse_FailsPrecondition: a still-active publish bind-mount blocks
// teardown; we refuse without unmounting or logging out (safe under #25813).
func TestUnstage_InUse_FailsPrecondition(t *testing.T) {
	list := "/tmp/csi-stage-v1 /dev/mapper/3600wwid\n/opt/target-a /tmp/csi-stage-v1\n"
	fr := teardownRunner("/dev/mapper/3600wwid", list, "", "")
	store := newMemMetaStore()
	_ = store.Save("v1", stageMeta{IQN: "iqn.q:t", LUNNumber: 0, WWID: "3600wwid", Portals: []string{"10.0.0.1:3260"}})
	n := newTestNode(fr, store)
	n.cfg = &config.QNAPConfig{Portals: []string{"10.0.0.1"}}

	err := n.UnstageVolume(context.Background(), &driver.UnstageRequest{
		VolumeID: "v1", StagingTargetPath: "/tmp/csi-stage-v1",
	})
	require.Error(t, err)
	var de *driver.Error
	require.ErrorAs(t, err, &de)
	assert.Equal(t, driver.CodeFailedPrecondition, de.Code)
	joined := strings.Join(fr.Commands(), "\n")
	assert.NotContains(t, joined, "umount", "must not unmount while still in use")
	assert.NotContains(t, joined, "--logout")
}

// TestUnstage_ColdBlock_DegradesOpen: cold cache + no staging mount (block) →
// tier-3 stub can't identify it, so we unmount (no-op) and return OK, leaving
// the session for the reconciler rather than erroring.
func TestUnstage_ColdBlock_DegradesOpen(t *testing.T) {
	fr := teardownRunner("", "", "", "") // sourceDev "" → not mounted
	n := newTestNode(fr, newMemMetaStore())
	n.cfg = &config.QNAPConfig{Portals: []string{"10.0.0.1"}}

	require.NoError(t, n.UnstageVolume(context.Background(), &driver.UnstageRequest{
		VolumeID: "vblock", StagingTargetPath: "/tmp/blk",
	}))
	assert.NotContains(t, strings.Join(fr.Commands(), "\n"), "--logout")
}

// TestUnstage_Tier3_BlockViaSAN: cold cache + no staging mount (block volume).
// The SAN supplies the target IQN (verified by LUN name), this node's live
// session grounds the LUN/WWID, and the session is logged out + flushed.
func TestUnstage_Tier3_BlockViaSAN(t *testing.T) {
	const sess = `Target: iqn.qnap:t10 (non-flash)
	Current Portal: 10.0.0.1:3260,1
		Attached SCSI devices:
		scsi5 Channel 00 Id 0 Lun: 3
			Attached scsi disk sdx		State: running
`
	// sourceDev "" → staging path not mounted (block); members maps sdx→wwidX.
	fr := teardownRunner("", "", sess, "sdx wwidX\n")
	n := newTestNode(fr, newMemMetaStore())
	n.cfg = &config.QNAPConfig{Portals: []string{"10.0.0.1"}}

	fc := newFakeClient()
	fc.targets[10] = goqnap.Target{Index: 10, IQN: "iqn.qnap:t10"}
	fc.luns[42] = goqnap.LUN{Index: 42, Name: "vol-a"}
	n.san = newSANIdentityCache(fc, newSessionManager(fc, "u", "p"), sanIdentityCacheTTL, zap.NewNop())

	// volume-id encodes LUNIndex=42, TargetIndex=10, own="t", LUNName="vol-a".
	volID := externalID{LUNIndex: 42, TargetIndex: 10, OwnTarget: true, LUNName: "vol-a"}.String()
	require.NoError(t, n.UnstageVolume(context.Background(), &driver.UnstageRequest{
		VolumeID: volID, StagingTargetPath: "/tmp/blk",
	}))

	joined := strings.Join(fr.Commands(), "\n")
	assert.Contains(t, joined, "multipath -f wwidX")
	assert.Contains(t, joined, "iscsiadm -m node -T iqn.qnap:t10 -p 10.0.0.1:3260 --logout")
}

// TestUnstage_Tier3_NoSANDegradesOpen: cold block with no node SAN client →
// no identity, unmount no-op, return OK (session left for the reconciler).
func TestUnstage_Tier3_NoSANDegradesOpen(t *testing.T) {
	fr := teardownRunner("", "", "", "")
	n := newTestNode(fr, newMemMetaStore()) // n.san stays nil
	n.cfg = &config.QNAPConfig{Portals: []string{"10.0.0.1"}}
	require.NoError(t, n.UnstageVolume(context.Background(), &driver.UnstageRequest{
		VolumeID: externalID{LUNIndex: 1, TargetIndex: 2, LUNName: "z"}.String(), StagingTargetPath: "/tmp/blk",
	}))
	assert.NotContains(t, strings.Join(fr.Commands(), "\n"), "--logout")
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
