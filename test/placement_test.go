//go:build integration

package e2e

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Local-only placement behaviors that don't apply to network storage (qnap):
// the --driver=local backend decides the node at create and pins the volume
// there via topology, with controller→controller forwarding routing
// create/delete to the owning node. These verify the L4 coordination layer:
// pinning, purge/rerun re-pinning, host=auto, pool selection, and forwarded
// delete. (qnap is node-mobile, so none of this is meaningful for it.)
func TestIntegration_LocalPlacement(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.localPluginID, 2, false)

	nodes := c.readyNodes(t)
	require.GreaterOrEqual(t, len(nodes), 2, "need >=2 ready nodes for the forwarding test")
	owner := nodes[len(nodes)-1]

	// Pinning + forwarding: a volume pinned to `owner` lands its consumer there,
	// and after purge+rerun returns to the same node. Delete is forwarded to the
	// owning node's controller.
	t.Run("pin_and_repin", func(t *testing.T) {
		id := "ncd-e2e-pinned"
		c.createLocalVolume(t, id, owner, "")
		t.Cleanup(func() { c.deleteVolume(id) })
		t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })

		c.runConsumer(t, id)
		assert.Equal(t, owner, c.consumerNode(t), "consumer pinned to the volume's owning node")

		c.stopConsumer(t, id)
		c.runConsumer(t, id)
		assert.Equal(t, owner, c.consumerNode(t), "re-run returns to the owning node")
		c.stopConsumer(t, id)

		require.NoError(t, c.nomad("volume", "delete", id), "forwarded delete to the owning node succeeds")
	})

	// host=auto: the controller picks a node; the consumer lands on a ready node.
	t.Run("host_auto", func(t *testing.T) {
		id := "ncd-e2e-auto"
		c.createLocalVolume(t, id, "auto", "")
		t.Cleanup(func() { c.deleteVolume(id) })
		t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })

		c.runConsumer(t, id)
		assert.Contains(t, nodes, c.consumerNode(t), "auto placed on a ready node")
		c.stopConsumer(t, id)
		require.NoError(t, c.nomad("volume", "delete", id))
	})

	// Multi-pool selection (§15.8). The integration cluster has tank1 + tank2, so
	// this runs by default against the second pool (LOCAL_INTEGRATION_POOL2, default "tank2" —
	// the value deploy.sh adds to the allowlist). Set LOCAL_INTEGRATION_POOL2="" to skip on a
	// single-pool cluster. The driver must encode the chosen pool into external_id.
	pool2, ok := os.LookupEnv("LOCAL_INTEGRATION_POOL2")
	if !ok {
		pool2 = "tank2"
	}
	if pool2 != "" {
		t.Run("pool_selection", func(t *testing.T) {
			id := "ncd-e2e-pool2"
			c.createLocalVolume(t, id, "auto", pool2)
			t.Cleanup(func() { c.deleteVolume(id) })
			t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })

			var vol struct{ ExternalID string }
			require.NoError(t, c.apiGet("/v1/volume/csi/"+id, &vol))
			assert.Contains(t, vol.ExternalID, "/"+pool2+"/", "external_id encodes the selected pool")

			c.runConsumer(t, id)
			assert.Contains(t, nodes, c.consumerNode(t), "pool2 volume placed on a ready node")
			c.stopConsumer(t, id)
			require.NoError(t, c.nomad("volume", "delete", id))
		})
	}
}
