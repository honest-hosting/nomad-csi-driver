//go:build integration

package e2e

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Volume-context loss suite. See
// specs/nomad-csi-driver/NOMAD-CSI-DRIVER-MISSING-CTX-FIX.PLAN.md.
//
// Re-running `nomad volume create` against an ALREADY-EXISTING volume does not
// call the plugin: the Nomad server merges the submitted spec into the stored
// volume instead. Before Nomad 1.9.6 (hashicorp/nomad#24922) that merge assigned
// the spec's context unconditionally, and a create spec carries no `context`
// block — so the plugin-supplied context was replaced with nothing. The volume
// keeps running (it is already staged); the next NodeStageVolume is what fails.
//
// These tests assert the DRIVER survives that, and separately record what NOMAD
// did, so they stay meaningful across the 1.6.3 → 1.9.6+ upgrade instead of
// asserting a bug that upstream has since fixed.

// volumeContext returns a volume's stored CSI context. Nomad does not print it
// in `nomad volume status`, so this is the only way to observe the wipe.
func (c *client) volumeContext(t *testing.T, volID string) map[string]string {
	t.Helper()
	var v struct {
		Context map[string]string
	}
	require.NoError(t, c.apiGet("/v1/volume/csi/"+volID, &v))
	return v.Context
}

// TestIntegrationLocal_VolumeContext_SurvivesRecreate is the regression for the reported failure:
// stop the workload, re-run `volume create`, restart, and the volume must still
// mount with its data intact.
//
// On a vulnerable Nomad (< 1.9.6) this exercises the driver's rebuild path; on a
// fixed Nomad the context survives and the rebuild is never entered. Either way
// the volume must come back — which is the property we actually care about.
func TestIntegrationLocal_VolumeContext_SurvivesRecreate(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.localPluginID, 1, false)

	id := fmt.Sprintf("ncd-ctx-%d", time.Now().Unix())
	c.createLocalVolume(t, id, "auto", "")
	t.Cleanup(func() { c.deleteVolume(id) })
	// Purge the consumer no matter where the test exits: a leaked consumer job
	// holds a claim, blocks the volume delete above, and lands on whichever test
	// runs next.
	t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })

	c.runConsumer(t, id)
	token := uniqueToken()
	c.writeMarker(t, token)
	require.Equal(t, token, c.readMarker(t), "sanity: marker readable before the re-create")

	before := c.volumeContext(t, id)
	require.NotEmpty(t, before, "a freshly created volume must have a plugin-supplied context")
	t.Logf("context before re-create: %v", before)

	// Stop the consumer first. Nomad's merge refuses to change mount options
	// while the volume is in use, and that guard is the only thing that would
	// block the merge — releasing the claim is what lets it complete, which is
	// exactly the sequence an operator follows after hitting that error.
	c.stopConsumer(t, id)

	// Re-submit the identical spec. This is the operation under test.
	c.createLocalVolume(t, id, "auto", "")

	after := c.volumeContext(t, id)
	wiped := len(after) == 0
	if wiped {
		t.Logf("nomad ERASED the volume context on re-create (expected on Nomad < 1.9.6); driver must rebuild it")
	} else {
		t.Logf("nomad PRESERVED the volume context on re-create (expected on Nomad >= 1.9.6): %v", after)
		assert.Equal(t, before, after, "a preserved context should be unchanged, not partially rewritten")
	}

	// The property that matters on every version: the volume still mounts and
	// still holds its data.
	c.runConsumer(t, id)
	assert.Equal(t, token, c.readMarker(t),
		"volume must remount with data intact after a re-create, whether or not Nomad kept the context")
	c.stopConsumer(t, id)
}

// TestIntegrationLocal_VolumeContext_SurvivesExpand covers the same merge path reached the way
// operators will reach it routinely once the cluster is on Nomad >= 1.8: volume
// expansion is a re-submitted `volume create` with a larger capacity, so on any
// Nomad in the vulnerable range EVERY expand wipes that volume's context.
//
// Skips cleanly where expansion is unsupported (Nomad 1.6.x), so it can land
// before the upgrade and start covering it the moment the upgrade happens.
func TestIntegrationLocal_VolumeContext_SurvivesExpand(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.localPluginID, 1, false)

	// Same gate the lifecycle suite's 06_expand step uses. Nomad < 1.8 accepts a
	// larger capacity_min without error but never grows the volume — so without
	// this the size assertion below fails on an unchanged filesystem, which says
	// nothing about the context. Note the wipe DOES still happen on those
	// versions (see the log line below); it is only the expansion that no-ops.
	if ver := c.agentVersion(t); !versionAtLeast(ver, 1, 8) {
		t.Skipf("CSI volume expand requires Nomad >= 1.8.0 (cluster reports %q)", ver)
	}

	id := fmt.Sprintf("ncd-ctx-exp-%d", time.Now().Unix())
	c.createLocalVolume(t, id, "auto", "")
	t.Cleanup(func() { c.deleteVolume(id) })
	t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })

	c.runConsumer(t, id)
	token := uniqueToken()
	c.writeMarker(t, token)
	sizeBefore := c.mountSizeKB(t)

	// The consumer MUST be stopped first. An in-use re-create is rejected before
	// the expand is even attempted: the spec carries no mount_options (nil) while
	// the stored volume's were normalized to a non-nil empty struct by
	// CSIVolume.Copy, and CSIMountOptions.Equal treats nil vs non-nil as a
	// difference. That guard is unrelated to expansion and is still present in
	// Nomad 1.10.x, so there is no "online expand" path to test here.
	c.stopConsumer(t, id)

	// Grow it: same spec, larger capacity — the routine operation that puts a
	// production cluster on the reconcile path once expansion is available.
	c.createVolumeSpec(t, id, c.localPluginID, "128MiB", "", "  host   = \"auto\"\n  fsType = \"ext4\"\n")

	if len(c.volumeContext(t, id)) == 0 {
		t.Logf("nomad erased the volume context during expand (Nomad < 1.9.6); driver must rebuild on the next stage")
	}

	// Re-stage: the wipe is latent and only surfaces at the next NodeStageVolume.
	c.runConsumer(t, id)
	assert.Equal(t, token, c.readMarker(t), "expanded volume must remount with data intact")
	c.poll(t, "filesystem grew after expand", 90*time.Second, func() bool {
		return c.mountSizeKB(t) > sizeBefore
	})
	c.stopConsumer(t, id)
}

// TestIntegrationLocal_VolumeContext_RebuildIsObservable asserts the operator-facing signal: when
// the driver has to rebuild a context, it says so. Without this the failure mode
// simply changes from a visible outage to an invisible one.
//
// Skipped when Nomad preserved the context (>= 1.9.6), since there is then
// nothing to rebuild and the counter correctly stays at zero.
func TestIntegrationLocal_VolumeContext_RebuildIsObservable(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.localPluginID, 1, false)

	id := fmt.Sprintf("ncd-ctx-obs-%d", time.Now().Unix())
	c.createLocalVolume(t, id, "auto", "")
	t.Cleanup(func() { c.deleteVolume(id) })
	// Purge the consumer no matter where the test exits: a leaked consumer job
	// holds a claim, blocks the volume delete above, and lands on whichever test
	// runs next.
	t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })

	port := envOr("METRICS_PORT", "9503") // local monolith
	path := envOr("METRICS_PATH", "/metrics")
	hosts := c.reachableMetricsHosts(t, port, path)

	const metric = "nomad_csi_node_volume_context_reconstructed_total"
	// The stage can land on any node, so sum the family across every endpoint —
	// same model the observability suite uses.
	before := c.scrapeSum(t, hosts, port, path, metric, `field="dataset"`)

	c.runConsumer(t, id)
	c.stopConsumer(t, id)

	c.createLocalVolume(t, id, "auto", "") // the wipe, if this Nomad does it
	if len(c.volumeContext(t, id)) != 0 {
		t.Skip("nomad preserved the volume context (>= 1.9.6): nothing to rebuild, counter stays at zero")
	}

	c.runConsumer(t, id) // rebuild happens here, during NodeStageVolume

	after := c.scrapeSum(t, hosts, port, path, metric, `field="dataset"`)
	assert.Greater(t, after, before,
		"a rebuilt volume context must be visible as %s{field=\"dataset\"}, not just a log line", metric)
	c.stopConsumer(t, id)
}
