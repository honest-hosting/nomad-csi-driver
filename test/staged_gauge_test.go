//go:build integration

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Staged-gauge / stateless-teardown suite. These prove the two properties the
// stateless redesign is about, which unit tests with fakes cannot: that the
// host-derived node_staged_volumes gauge and the qnap iSCSI teardown both survive
// a node-plugin restart that wipes the plugin's in-memory state.
//
// They exec inside the qnap node plugin alloc (which carries iscsiadm and reaches
// the host iSCSI stack) and bounce the plugin task in place with `nomad alloc
// restart` — the allocation (and its host mounts/sessions) keeps running, only
// the plugin process restarts, which is exactly the case Nomad does NOT re-stage.

// nodePluginISCSISessions returns `iscsiadm -m session` output from the qnap node
// plugin on the given node (empty when there are no sessions).
func (c *client) nodePluginISCSISessions(t *testing.T, node string) string {
	t.Helper()
	alloc := c.allocOnNode(t, c.qnapPluginID+"-node", node)
	out, _ := c.nomadOut("alloc", "exec", "-i=false", "-t=false", alloc, "/bin/sh", "-c", "iscsiadm -m session 2>/dev/null || true")
	return out
}

// countISCSISessions counts session lines (each begins "tcp: [N] ...").
func countISCSISessions(out string) int {
	n := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "tcp:") {
			n++
		}
	}
	return n
}

// restartNodePlugin bounces the qnap node-plugin task on node in place (the alloc
// and its host state persist; only the plugin process restarts).
func (c *client) restartNodePlugin(t *testing.T, node string) {
	t.Helper()
	alloc := c.allocOnNode(t, c.qnapPluginID+"-node", node)
	require.NoError(t, c.nomad("alloc", "restart", alloc), "restart node plugin task on %s", node)
	c.requirePluginHealthy(t, c.qnapPluginID, 1, false)
}

// INT-2: the host-derived staged gauge survives a node-plugin restart. The old
// in-memory Inc/Dec gauge reset to 0 here (and could go negative on the later
// unstage); the host-counted gauge stays correct because the iSCSI session
// persists across the plugin bounce.
func TestIntegration_StagedGauge_SurvivesPluginRestart(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.qnapPluginID, 1, true) // optional: skips if qnap not deployed

	path := envOr("METRICS_PATH", "/metrics")
	nodePort := envOr("QNAP_NODE_METRICS_PORT", "9502")
	nodeHosts := c.reachableMetricsHosts(t, nodePort, path)
	staged := func() float64 { return c.scrapeSum(t, nodeHosts, nodePort, path, "nomad_csi_node_staged_volumes") }

	base := staged()

	b := qnapBackend(c)
	volID := fmt.Sprintf("ncd-restart-%d", time.Now().UnixNano())
	c.createCSIVolume(t, b, volID, "", "")
	t.Cleanup(func() { c.deleteVolume(volID) })
	c.runConsumer(t, volID)
	t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })
	require.Equal(t, base+1, staged(), "staged gauge rises by 1 after stage")

	// Bounce the node plugin on the node the volume is staged on.
	c.restartNodePlugin(t, c.consumerNode(t))

	// The gauge must still read base+1 — NOT 0, NOT negative — with no re-stage.
	// The metrics endpoint is briefly down (and may flap) right after the restart,
	// so poll with a non-fatal scrape that retries through connection errors.
	c.poll(t, "staged gauge recovers to base+1 after plugin restart", 90*time.Second, func() bool {
		v, ok := c.stagedSoft(nodeHosts, nodePort, path)
		return ok && v == base+1
	})

	c.stopConsumer(t, volID)
	c.poll(t, "staged gauge returns to baseline after unstage", 90*time.Second, func() bool {
		v, ok := c.stagedSoft(nodeHosts, nodePort, path)
		return ok && v == base
	})
}

// stagedSoft sums node_staged_volumes across hosts WITHOUT failing the test on a
// transient scrape error — the metrics endpoint is briefly unavailable (and may
// flap) right after a plugin restart. Returns ok=false unless every host answered,
// so a poll retries until the restarted endpoint is back and serving.
func (c *client) stagedSoft(hosts []string, port, path string) (float64, bool) {
	var total float64
	for _, h := range hosts {
		raw, err := c.tryScrape(h, port, path)
		if err != nil {
			return 0, false
		}
		total += sumSeries(parsePromText(raw), "nomad_csi_node_staged_volumes")
	}
	return total, true
}

// INT-3: qnap iSCSI teardown works after a plugin restart that lost the in-memory
// identity cache — the session is cleanly logged out (via host reconstruction),
// leaving no leaked session. This is the split-brain-prevention property.
func TestIntegration_QNAPTeardown_NoLeakAfterRestart(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.qnapPluginID, 1, true)

	b := qnapBackend(c)
	volID := fmt.Sprintf("ncd-noleak-%d", time.Now().UnixNano())
	c.createCSIVolume(t, b, volID, "", "")
	t.Cleanup(func() { c.deleteVolume(volID) })

	c.runConsumer(t, volID)
	t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })
	node := c.consumerNode(t)

	baseSessions := countISCSISessions(c.nodePluginISCSISessions(t, node))
	require.Greater(t, baseSessions, 0, "staging established at least one iSCSI session")

	// Wipe the plugin's in-memory teardown cache.
	c.restartNodePlugin(t, node)

	// Unstage on the cold cache: the plugin must reconstruct the identity from the
	// still-present staging mount and log the session out.
	c.stopConsumer(t, volID)

	c.poll(t, "iSCSI sessions fully cleaned up after cold-cache unstage", 90*time.Second, func() bool {
		return countISCSISessions(c.nodePluginISCSISessions(t, node)) == 0
	})
}

// nodePluginExec runs a /bin/sh command inside the qnap node plugin alloc on node.
func (c *client) nodePluginExec(t *testing.T, node, sh string) (string, error) {
	t.Helper()
	alloc := c.allocOnNode(t, c.qnapPluginID+"-node", node)
	return c.nomadOut("alloc", "exec", "-i=false", "-t=false", alloc, "/bin/sh", "-c", sh)
}

// firstSessionTarget parses `iscsiadm -m session` ("tcp: [N] IP:PORT,TPGT IQN
// (flags)") and returns the first session's target IQN and portal (host:port).
func firstSessionTarget(out string) (iqn, portal string, ok bool) {
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) >= 4 && f[0] == "tcp:" {
			return f[3], strings.SplitN(f[2], ",", 2)[0], true
		}
	}
	return "", "", false
}

// hasSessionTo reports whether the node plugin currently has an iSCSI session to iqn.
func (c *client) hasSessionTo(t *testing.T, node, iqn string) bool {
	return strings.Contains(c.nodePluginISCSISessions(t, node), iqn)
}

// INT-8: the session reconciler logs out a LEAKED iSCSI session — one that is
// logged in but backs no mount and is unknown to the plugin's in-memory cache —
// within its grace window. This is the split-brain-prevention path (a stale
// session left on one node while the volume moves to another lets two initiators
// write one LUN). Requires the node plugin to have the reconciler enabled with a
// short grace (localdev sets qnap.reconcile_enabled=true, reconcile_grace=2m).
func TestIntegration_Reconciler_LogsOutLeakedSession(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.qnapPluginID, 1, true) // optional: skips if qnap not deployed
	b := qnapBackend(c)

	volID := fmt.Sprintf("ncd-recon-%d", time.Now().UnixNano())
	c.createCSIVolume(t, b, volID, "", "")
	t.Cleanup(func() { c.deleteVolume(volID) })

	// Stage it to establish a real session; learn its target IQN + portal; then
	// stop the consumer so the driver logs the session out cleanly and forgets it.
	c.runConsumer(t, volID)
	t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })
	node := c.consumerNode(t)
	iqn, portal, ok := firstSessionTarget(c.nodePluginISCSISessions(t, node))
	require.True(t, ok, "expected an active iSCSI session after staging")

	_ = c.nomad("job", "stop", "-purge", consumerJob)
	c.poll(t, "driver logs the session out on unstage", 90*time.Second, func() bool {
		return !c.hasSessionTo(t, node, iqn)
	})

	// Forge the leak: log back in by hand. The LUN is still mapped (delete happens
	// later), so the session comes up — but with no mount and unknown to the cache,
	// exactly the hazard the reconciler exists to clean up.
	_, _ = c.nodePluginExec(t, node, fmt.Sprintf("iscsiadm -m node -T %s -p %s --login 2>/dev/null || true", iqn, portal))
	t.Cleanup(func() {
		_, _ = c.nodePluginExec(t, node, fmt.Sprintf("iscsiadm -m node -T %s -p %s --logout 2>/dev/null || true", iqn, portal))
	})
	require.True(t, c.hasSessionTo(t, node, iqn), "leaked session established")

	// The reconciler (localdev: grace 2m, interval 2m) must log it out. Allow a
	// generous margin for up to ~2 sweeps plus the grace window.
	c.poll(t, "reconciler logs out the leaked session", 6*time.Minute, func() bool {
		return !c.hasSessionTo(t, node, iqn)
	})
}
