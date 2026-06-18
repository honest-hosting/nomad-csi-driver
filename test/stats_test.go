//go:build integration

package e2e

// Per-volume usage-stats suite: proves the stats query API and the
// nomad_csi_volume_* metrics report real usage across a create→mount→unmount
// cycle, for both backends.
//
// Endpoints (host network, like /metrics): the controller query API is
// GET /v1/volume-stats[/{externalID}]. Local monoliths each serve :9610 (default
// query_addr) for their own node's volumes — and any monolith forwards a
// by-id lookup to the owning node, so we deliberately query hosts[0] regardless
// of placement. The qnap controller serves an aggregate on :9611 (localdev sets
// query_addr there + forward_secret to enable the fan-out); without that config
// the qnap suite SKIPS cleanly, exactly like the qnap observability suite skips
// without an appliance.
//
// Env knobs: STATS_QUERY_PORT (default 9610, local), STATS_QNAP_QUERY_PORT
// (default 9611, qnap controller), METRICS_HOSTS/METRICS_PORT as in the
// observability suite. If no query endpoint is reachable the suite SKIPS (an
// environment gap, not a bug); once reachable, wrong/missing data FAILS.

import (
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/honest-hosting/nomad-csi-driver/internal/stats"
)

// TestIntegration_VolumeStats_Local drives the local backend: it creates a
// node-pinned volume, mounts it, and asserts the stats API reports its usage
// (statfs bytes + the directory walk), the per-volume metric is exposed, and the
// reading is evicted on unstage. The by-id lookup is issued to an arbitrary
// monolith to exercise the forward-to-owner path.
func TestIntegration_VolumeStats_Local(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.localPluginID, 1, false)

	port := envOr("STATS_QUERY_PORT", "9610")
	hosts := c.reachableMetricsHosts(t, port, stats.QueryPathPrefix) // skips if none answer
	t.Logf("volume-stats(local): querying %d endpoint(s) on :%s%s -> %v", len(hosts), port, stats.QueryPathPrefix, hosts)

	baseCount := c.statsListCount(t, hosts, port)

	node := c.pickNode(t)
	volID := fmt.Sprintf("ncd-stats-%d", time.Now().UnixNano())
	c.createLocalVolume(t, volID, node, "")
	t.Cleanup(func() { c.deleteVolume(volID) })
	c.runConsumer(t, volID)
	t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })
	require.Equal(t, node, c.consumerNode(t), "consumer should pin to the volume's node")

	// The query API is keyed by the Nomad volume id (volID) — the driver resolves
	// it to its internal external id. statfs hydrates (any monolith forwards the
	// by-id lookup to the owner).
	var got stats.PublicVolumeStats
	c.poll(t, "volume stats hydrate (statfs)", 2*time.Minute, func() bool {
		code, cs := c.statsByID(t, hosts[0], port, volID)
		if code == 200 && !cs.StatfsAt.IsZero() && cs.TotalBytes > 0 {
			got = cs
			return true
		}
		return false
	})
	require.Equal(t, volID, got.ID, "stats should be keyed by the Nomad volume id")
	require.Equal(t, node, got.Node, "stats should report the owning node")
	require.Equal(t, stats.AccessMount, got.AccessType)
	require.Greater(t, got.AvailableBytes, int64(0), "a mounted ext4 fs has free space")
	require.Empty(t, got.LastError, "a healthy mount should have no error")

	// The owning monolith's list grew by exactly one volume.
	require.Equal(t, baseCount+1, c.statsListCount(t, hosts, port),
		"summed volume-stats list should rise by 1 after a stage")

	// The directory walk completes; a freshly-formatted ext4 fs has lost+found.
	c.poll(t, "volume stats walk completes", 3*time.Minute, func() bool {
		_, cs := c.statsByID(t, hosts[0], port, volID)
		return cs.WalkComplete
	})
	_, walked := c.statsByID(t, hosts[0], port, volID)
	require.GreaterOrEqual(t, walked.DirCount, int64(1), "walk should count at least lost+found")

	// The per-volume gauge is exposed on /metrics (same hosts, metrics port).
	mport := envOr("METRICS_PORT", "9503")
	require.Greater(t, c.volMetricSum(hosts, mport, "nomad_csi_volume_used_bytes"), float64(0),
		"nomad_csi_volume_used_bytes should be exposed for the mounted volume")

	// Unstage evicts the reading.
	c.stopConsumer(t, volID)
	c.poll(t, "volume stats evicted after unstage", 2*time.Minute, func() bool {
		return c.statsListCount(t, hosts, port) == baseCount
	})

	c.requireVolumeDeleted(t, volID)
}

// TestIntegration_VolumeStats_QNAP drives the qnap backend: it creates a LUN,
// mounts it, and asserts the controller's aggregated stats report its usage.
// It SKIPS when the qnap controller query endpoint is not reachable (no
// appliance, or forward_secret/query_addr not configured).
func TestIntegration_VolumeStats_QNAP(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.qnapPluginID, 1, true) // optional: skips if not deployed

	port := envOr("STATS_QNAP_QUERY_PORT", "9611")
	hosts := c.reachableMetricsHosts(t, port, stats.QueryPathPrefix) // skips if none answer
	t.Logf("volume-stats(qnap): querying controller on :%s%s -> %v", port, stats.QueryPathPrefix, hosts)

	b := qnapBackend(c)
	volID := fmt.Sprintf("ncd-stats-qnap-%d", time.Now().UnixNano())
	c.createCSIVolume(t, b, volID, "", "")
	t.Cleanup(func() { c.deleteVolume(volID) })
	c.runConsumer(t, volID)
	t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })

	mountNode := c.consumerNode(t)

	// The controller's fan-out aggregate picks up the node's reading, keyed by the
	// Nomad volume id.
	var got stats.PublicVolumeStats
	c.poll(t, "qnap controller aggregates volume stats", 2*time.Minute, func() bool {
		code, cs := c.statsByID(t, hosts[0], port, volID)
		if code == 200 && !cs.StatfsAt.IsZero() && cs.TotalBytes > 0 {
			got = cs
			return true
		}
		return false
	})
	require.Equal(t, volID, got.ID, "stats should be keyed by the Nomad volume id")
	require.Equal(t, mountNode, got.Node, "aggregate should attribute the reading to the mounting node")
	require.Equal(t, stats.AccessMount, got.AccessType)
	require.Greater(t, got.AvailableBytes, int64(0))

	// Teardown: after unstage the volume still exists in Nomad but is mounted
	// nowhere, so the aggregate ages it out and a by-id lookup returns 412
	// (precondition failed: not mounted) — distinct from a 404 for an unknown id.
	c.stopConsumer(t, volID)
	c.poll(t, "qnap aggregate drops the volume after unstage", 2*time.Minute, func() bool {
		code, _ := c.statsByID(t, hosts[0], port, volID)
		return code == 412
	})

	c.requireVolumeDeleted(t, volID)
}

// --- stats query helpers ---

// statsByID GETs the by-Nomad-id endpoint, returning the HTTP status and decoded
// body (decoded for 200/503; empty otherwise).
func (c *client) statsByID(t *testing.T, host, port, nomadID string) (int, stats.PublicVolumeStats) {
	t.Helper()
	url := fmt.Sprintf("http://%s:%s%s/%s", host, port, stats.QueryPathPrefix, nomadID)
	resp, err := scrapeHTTP.Get(url)
	require.NoError(t, err, "GET %s", url)
	defer func() { _ = resp.Body.Close() }()
	var cs stats.PublicVolumeStats
	if resp.StatusCode == 200 || resp.StatusCode == 503 {
		_ = json.NewDecoder(resp.Body).Decode(&cs)
	} else {
		_, _ = io.Copy(io.Discard, resp.Body)
	}
	return resp.StatusCode, cs
}

// statsList GETs the list endpoint on one host.
func (c *client) statsList(t *testing.T, host, port string) []stats.PublicVolumeStats {
	t.Helper()
	url := fmt.Sprintf("http://%s:%s%s", host, port, stats.QueryPathPrefix)
	resp, err := scrapeHTTP.Get(url)
	require.NoError(t, err, "GET %s", url)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out []stats.PublicVolumeStats
	require.NoError(t, json.Unmarshal(body, &out), "decode volume-stats list")
	return out
}

// statsListCount sums the list length across every host (local: each monolith
// reports its own node's volumes, so the sum is the cluster total).
func (c *client) statsListCount(t *testing.T, hosts []string, port string) int {
	t.Helper()
	total := 0
	for _, h := range hosts {
		total += len(c.statsList(t, h, port))
	}
	return total
}

// volMetricSum sums a per-volume gauge family across hosts, best-effort (a host
// that doesn't answer /metrics is skipped rather than failing the test).
func (c *client) volMetricSum(hosts []string, port, name string, labels ...string) float64 {
	var total float64
	for _, h := range hosts {
		raw, err := c.tryScrape(h, port, "/metrics")
		if err != nil {
			continue
		}
		total += sumSeries(parsePromText(raw), name, labels...)
	}
	return total
}
