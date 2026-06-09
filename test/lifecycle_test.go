//go:build integration

package e2e

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The full CSI volume lifecycle, exercised end-to-end through Nomad and run
// IDENTICALLY against both backends so the two are easy to compare stage by
// stage. Each stage is its own subtest, so a run reads as a checklist:
//
//	TestIntegration_Local/01_create
//	TestIntegration_Local/02_mount
//	TestIntegration_Local/03_read_write
//	TestIntegration_Local/04_snapshot_restore
//	TestIntegration_Local/05_clone
//	TestIntegration_Local/06_expand          (skips on Nomad < 1.8.0)
//	TestIntegration_Local/07_persist_across_remount
//	TestIntegration_Local/08_delete
//	TestIntegration_QNAP/01_create ...
//
// This is the single source of lifecycle coverage for BOTH --driver values; the
// old in-process QNAP controller tests were retired in favor of driving the real
// Nomad lifecycle. Every data-bearing stage writes/reads a marker so we prove
// the *data* survives — not just that an API call succeeded.
//
// The only backend-specific behavior is placement: local pins each volume (and
// its snapshots/clones) to a node via topology, so its consumer must land on
// that exact node; qnap is network storage, so its consumer may land on any
// ready node and the LUN follows. Local-only placement behaviors (host=auto,
// pool selection, forwarded delete) live in placement_test.go.

// backend describes a CSI driver under test through Nomad.
type backend struct {
	name       string
	pluginID   string
	capMin     string // initial capacity ("64MiB" local, "1GiB" qnap)
	expandTo   string // grown capacity for the expand stage
	volID      string
	pinsToNode bool                     // local pins to its create-node; qnap is node-mobile
	params     func(node string) string // parameters{} body; node is the placement hint
}

func localBackend(c *client) backend {
	return backend{
		name: "local", pluginID: c.localPluginID,
		capMin: "64MiB", expandTo: "256MiB",
		volID: "ncd-e2e-local", pinsToNode: true,
		params: func(node string) string {
			if node == "" {
				node = "auto"
			}
			return fmt.Sprintf("  host   = %q\n  fsType = \"ext4\"\n", node)
		},
	}
}

func qnapBackend(c *client) backend {
	return backend{
		name: "qnap", pluginID: c.qnapPluginID,
		capMin: "1GiB", expandTo: "2GiB",
		volID: "ncd-e2e-qnap", pinsToNode: false,
		params: func(string) string { return "  fsType = \"ext4\"\n" },
	}
}

func TestIntegration_Local(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.localPluginID, 1, false)
	runLifecycle(t, c, localBackend(c))
}

func TestIntegration_QNAP(t *testing.T) {
	c := newClient(t)
	// Optional: skips cleanly where no QNAP appliance/plugin is deployed.
	c.requirePluginHealthy(t, c.qnapPluginID, 1, true)
	runLifecycle(t, c, qnapBackend(c))
}

func runLifecycle(t *testing.T, c *client, b backend) {
	nodes := c.readyNodes(t)
	require.NotEmpty(t, nodes, "need at least one ready node")

	// local pins to a specific node; qnap places anywhere (network storage).
	node := ""
	if b.pinsToNode {
		node = nodes[len(nodes)-1]
	}
	id := b.volID
	restoreID := id + "-restore"
	cloneID := id + "-clone"
	token := uniqueToken()

	// Guaranteed teardown, in dependency order: stop the consumer (release
	// claims), delete every snapshot we created (dependent → must precede their
	// volumes), then delete every volume. All best-effort + idempotent, so a
	// clean run leaves zero orphaned volumes or snapshots even if a stage failed.
	t.Cleanup(func() {
		_ = c.nomad("job", "stop", "-purge", consumerJob)
		for _, s := range c.createdSnaps {
			_ = c.nomad("volume", "snapshot", "delete", s.plugin, s.id)
		}
		c.deleteVolume(id)
		c.deleteVolume(restoreID)
		c.deleteVolume(cloneID)
	})

	if !t.Run("01_create", func(t *testing.T) {
		c.createCSIVolume(t, b, id, node, "")
	}) {
		return
	}

	if !t.Run("02_mount", func(t *testing.T) {
		c.runConsumer(t, id)
		assertPlacement(t, c, b, nodes, node)
	}) {
		return
	}

	// QNAP only: the LUN must be served through dm-multipath, with one active
	// path per configured portal. Proves the driver took the multipath code path
	// (not a raw /dev/sd fallback); a two-portal env additionally proves redundancy.
	if b.name == "qnap" {
		if !t.Run("02b_multipath", func(t *testing.T) {
			out := c.nodePluginMultipath(t, c.qnapPluginID+"-node", c.consumerNode(t))
			require.Contains(t, out, "QNAP", "LUN served through a dm-multipath map (QNAP)")
			paths := countActivePaths(out)
			t.Logf("qnap multipath active paths: %d", paths)
			assert.GreaterOrEqual(t, paths, 1, "at least one active multipath path")
			if os.Getenv("QNAP_INTEGRATION_PORTAL2") != "" {
				assert.GreaterOrEqual(t, paths, 2,
					"two portals configured (QNAP_INTEGRATION_PORTAL2 set) -> expect >=2 paths for redundancy")
			}
		}) {
			return
		}
	}

	if !t.Run("03_read_write", func(t *testing.T) {
		c.writeMarker(t, token)
		assert.Equal(t, token, c.readMarker(t), "data written to the mounted filesystem reads back")
	}) {
		return
	}

	// Snapshot the quiesced source, restore into a new volume, verify the data.
	if !t.Run("04_snapshot_restore", func(t *testing.T) {
		c.stopConsumer(t, id)                            // unmount first so the snapshot is consistent
		snapID := c.snapshotCreate(t, id, token+"-snap") // unique name avoids cross-run leak collisions
		c.createdSnaps = append(c.createdSnaps, snapRef{plugin: b.pluginID, id: snapID})
		// In-flow delete at end of this stage so stage 08 can delete the source;
		// the teardown sweep above is the safety net if this is skipped.
		t.Cleanup(func() { c.snapshotDelete(b.pluginID, snapID) })

		// It shows up in `volume snapshot list`, and the source can't be deleted
		// while it exists (snapshots are dependent → FailedPrecondition).
		snapOut, err := c.nomadOut("volume", "snapshot", "list", "-plugin", b.pluginID)
		require.NoError(t, err)
		assert.Contains(t, snapOut, snapID, "snapshot appears in `volume snapshot list`")
		assert.Error(t, c.nomad("volume", "delete", id), "source can't be deleted while a snapshot exists")

		c.createCSIVolume(t, b, restoreID, "", fmt.Sprintf("snapshot_id = %q", snapID))
		c.runConsumer(t, restoreID)
		assertPlacement(t, c, b, nodes, node) // local: restore pins to the source node
		assert.Equal(t, token, c.readMarker(t), "restored snapshot carries the source data")

		c.stopConsumer(t, restoreID)
		c.requireVolumeDeleted(t, restoreID) // must be independent of the snapshot
	}) {
		return
	}

	// Clone the source volume, verify the data came across. clone_id is the
	// source's CSI external id (forwarded verbatim to the driver), not its Nomad id.
	if !t.Run("05_clone", func(t *testing.T) {
		c.createCSIVolume(t, b, cloneID, "", fmt.Sprintf("clone_id = %q", c.externalID(t, id)))
		c.runConsumer(t, cloneID)
		assertPlacement(t, c, b, nodes, node) // local: clone pins to the source node
		assert.Equal(t, token, c.readMarker(t), "clone carries the source data")

		c.stopConsumer(t, cloneID)
		c.requireVolumeDeleted(t, cloneID) // must be independent of the source
	}) {
		return
	}

	// Expand (grow-only). Nomad didn't wire CSI expansion until 1.8.0, so this
	// skips below that. NOTE: the re-register-with-larger-capacity trigger and the
	// node-side grow timing should be re-validated on the first >= 1.8 run.
	if !t.Run("06_expand", func(t *testing.T) {
		if ver := c.agentVersion(t); !versionAtLeast(ver, 1, 8) {
			t.Skipf("CSI volume expand requires Nomad >= 1.8.0 (cluster reports %q)", ver)
		}
		c.runConsumer(t, id)
		before := c.mountSizeKB(t)
		// Re-register the same volume id with a larger capacity to trigger expansion.
		c.createVolumeSpec(t, id, b.pluginID, b.expandTo, "", b.params(node))
		// Restart the workload so it remounts the grown volume.
		c.stopConsumer(t, id)
		c.runConsumer(t, id)
		c.poll(t, "filesystem grew after expand", 90*time.Second, func() bool {
			return c.mountSizeKB(t) > before
		})
		assert.Equal(t, token, c.readMarker(t), "data survived the expand")
		c.stopConsumer(t, id)
	}) {
		return
	}

	// Persist across a plain unmount/remount: the existing filesystem must be
	// reused (never re-formatted), with data intact.
	if !t.Run("07_persist_across_remount", func(t *testing.T) {
		c.runConsumer(t, id)
		assertPlacement(t, c, b, nodes, node)
		assert.Equal(t, token, c.readMarker(t),
			"data survived unmount/remount — existing filesystem reused, never re-formatted")
		c.stopConsumer(t, id)
	}) {
		return
	}

	t.Run("08_delete", func(t *testing.T) {
		// QNAP snapshot deletion is asynchronous (the clone fork defers the
		// transient's cleanup), so the source's snapshots may still be clearing
		// here. Re-nudge the tracked ones and retry until the delete is accepted —
		// a clean delete is itself the signal that no dependent snapshot remains.
		for _, s := range c.createdSnaps {
			_ = c.nomad("volume", "snapshot", "delete", s.plugin, s.id)
		}
		c.requireVolumeDeleted(t, id)
	})
}

// assertPlacement checks where the consumer landed: local pins to its volume's
// node; qnap (network storage) may be on any ready node.
func assertPlacement(t *testing.T, c *client, b backend, nodes []string, node string) {
	t.Helper()
	if b.pinsToNode {
		assert.Equal(t, node, c.consumerNode(t), "consumer on the volume's pinned node")
	} else {
		assert.Contains(t, nodes, c.consumerNode(t), "consumer on a ready node")
	}
}
