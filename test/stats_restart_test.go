//go:build integration

package e2e

// Stats restart-consistency suite (INT-2 analogue for per-volume stats). Proves
// the property unit tests with fakes cannot: that nomad_csi_volume_* survives a
// plugin-task restart that wipes the plugin's in-memory stats registry, WITHOUT a
// re-stage — because the stats reconciler rehydrates the registry from the live
// mount table on startup (METRICS-RESTART-CONSISTENCY.PLAN.md §4, §6).
//
// Before this fix the registry was populated only by NodeStageVolume→Track, and
// Nomad does not re-issue NodeStageVolume for an alloc that keeps running across a
// plugin bounce — so every pre-restart volume's stats went missing (the reported
// "2 of 70 measured" symptom). Here the stats must reappear on their own.

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

// restartLocalPlugin bounces the local monolith plugin task on node in place (the
// alloc and its host mounts persist; only the plugin process restarts — exactly
// the case Nomad does NOT re-stage).
func (c *client) restartLocalPlugin(t *testing.T, node string) {
	t.Helper()
	alloc := c.allocOnNode(t, c.localPluginID, node)
	require.NoError(t, c.nomad("alloc", "restart", alloc), "restart local plugin task on %s", node)
	c.requirePluginHealthy(t, c.localPluginID, 1, false)
}

// TestIntegration_VolumeStats_Local_SurvivesPluginRestart stages a local volume,
// waits for its stats to hydrate, restarts the owning node's plugin task (wiping
// the in-memory registry), and asserts the stats reappear with no intervening
// re-stage — proving the reconciler rehydrated the registry from host truth.
func TestIntegration_VolumeStats_Local_SurvivesPluginRestart(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.localPluginID, 1, false)

	port := envOr("STATS_QUERY_PORT", "9610")
	hosts := c.reachableMetricsHosts(t, port, stats.QueryPathPrefix) // skips if none answer
	mport := envOr("METRICS_PORT", "9503")

	node := c.pickNode(t)
	volID := fmt.Sprintf("ncd-stats-restart-%d", time.Now().UnixNano())
	c.createLocalVolume(t, volID, node, "")
	t.Cleanup(func() { c.deleteVolume(volID) })
	c.runConsumer(t, volID)
	t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })
	require.Equal(t, node, c.consumerNode(t), "consumer should pin to the volume's node")

	// Baseline: stats hydrate before the restart (statfs bytes present). Tolerate
	// transient GET errors (flaky network / listener not up yet) — poll, don't fail.
	c.poll(t, "stats hydrate before restart", 2*time.Minute, func() bool {
		code, cs, err := c.statsByIDErr(hosts[0], port, volID)
		return err == nil && code == 200 && !cs.StatfsAt.IsZero() && cs.TotalBytes > 0
	})
	c.poll(t, "per-volume metric exposed before restart", time.Minute, func() bool {
		return c.volMetricSum(hosts, mport, "nomad_csi_volume_used_bytes") > 0
	})

	// Bounce the plugin on the owning node: the alloc (and its staging mount) keeps
	// running, only the plugin process restarts, so Nomad issues NO NodeStageVolume.
	c.restartLocalPlugin(t, node)

	// Without any re-stage, the reconciler must rehydrate the registry from the
	// mount table and the workers must re-measure — the volume's stats reappear.
	// The restart bounces this node's :9610 query listener, so tolerate transient
	// errors and capture the reading from the successful poll iteration.
	var got stats.PublicVolumeStats
	c.poll(t, "stats rehydrate after restart (no re-stage)", 4*time.Minute, func() bool {
		code, cs, err := c.statsByIDErr(hosts[0], port, volID)
		if err != nil || code != 200 || cs.StatfsAt.IsZero() || cs.TotalBytes == 0 {
			return false
		}
		got = cs
		return true
	})
	require.Equal(t, volID, got.ID, "rehydrated stats keyed by the Nomad volume id")
	require.Equal(t, node, got.Node, "rehydrated stats report the owning node")
	require.Equal(t, stats.AccessMount, got.AccessType)
	require.Empty(t, got.LastError, "a healthy remounted volume should have no error")
	c.poll(t, "per-volume metric re-exposed after restart (the regression this fixes)", time.Minute, func() bool {
		return c.volMetricSum(hosts, mport, "nomad_csi_volume_used_bytes") > 0
	})
}

// TestIntegration_VolumeStats_QNAP_SurvivesPluginRestart is the qnap analogue:
// stage a LUN, wait for the controller aggregate to report it, restart the owning
// node's qnap node plugin (wiping the node's in-memory stats registry), and assert
// the stats reappear with no re-stage — proving the node's stats reconciler
// rehydrated the registry from live iSCSI sessions, resolving each session's
// (IQN, LUN) → external id via the read-only SAN. SKIPS cleanly without a qnap
// deployment / reachable controller query API.
func TestIntegration_VolumeStats_QNAP_SurvivesPluginRestart(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.qnapPluginID, 1, true) // optional: skips if not deployed

	port := envOr("STATS_QNAP_QUERY_PORT", "9611")
	hosts := c.reachableMetricsHosts(t, port, stats.QueryPathPrefix) // skips if none answer

	b := qnapBackend(c)
	volID := fmt.Sprintf("ncd-stats-qnap-restart-%d", time.Now().UnixNano())
	c.createCSIVolume(t, b, volID, "", "")
	t.Cleanup(func() { c.deleteVolume(volID) })
	c.runConsumer(t, volID)
	t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })
	node := c.consumerNode(t)

	// Baseline: the controller aggregate reports the volume before the restart.
	// Tolerate transient GET errors (flaky network) — poll, don't fail.
	c.poll(t, "qnap stats hydrate before restart", 2*time.Minute, func() bool {
		code, cs, err := c.statsByIDErr(hosts[0], port, volID)
		return err == nil && code == 200 && !cs.StatfsAt.IsZero() && cs.TotalBytes > 0
	})

	// Bounce the qnap node plugin on the mount node: the alloc (and its iSCSI
	// session + mount) keeps running, only the plugin restarts, so Nomad issues NO
	// NodeStageVolume and the node's stats registry starts empty.
	c.restartNodePlugin(t, node)

	// Without any re-stage, the node's stats reconciler must re-enumerate its iSCSI
	// sessions, resolve each to its external id via the SAN, and re-measure — so the
	// controller aggregate reports the volume again. (A plugin-task restart provably
	// wipes the in-memory registry, so a fresh reading here can only come from the
	// reconciler.) Capture the reading from the successful poll iteration.
	var got stats.PublicVolumeStats
	c.poll(t, "qnap stats rehydrate after restart (no re-stage)", 4*time.Minute, func() bool {
		code, cs, err := c.statsByIDErr(hosts[0], port, volID)
		if err != nil || code != 200 || cs.StatfsAt.IsZero() || cs.TotalBytes == 0 {
			return false
		}
		got = cs
		return true
	})
	require.Equal(t, volID, got.ID)
	require.Equal(t, node, got.Node, "rehydrated stats attributed to the mounting node")
	require.Equal(t, stats.AccessMount, got.AccessType)
	require.Empty(t, got.LastError, "a healthy volume should have no error after rehydration")
}
