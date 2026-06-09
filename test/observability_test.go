//go:build integration

package e2e

// Observability suite: proves the plugin's own Prometheus endpoint is exposed by
// the deployed jobs (localdev/*.nomad.hcl render a `metrics { enabled = true }`
// block) and that the failure-domain counters/gauges actually move across a real
// lifecycle.
//
// Scrape model: the /metrics endpoint binds 0.0.0.0:<port> on the host network
// (network_mode=host), so we scrape every node's endpoint directly from the test
// host and SUM each family across nodes — this avoids mapping a node name to a
// single IP and is robust to which node a forwarded create / stage lands on.
//
//   - Hosts: METRICS_HOSTS (comma-separated IPs/hostnames) is the source of
//     truth when set. Otherwise the suite derives hosts from each node's Nomad
//     HTTPAddr. Set METRICS_HOSTS when Nomad advertises an IP that isn't routable
//     from the test host — e.g. a VirtualBox host-only cluster where Nomad
//     fingerprints the NAT address (10.0.2.x) but only 192.168.56.x is reachable:
//         export METRICS_HOSTS=192.168.56.51,192.168.56.52,192.168.56.53
//   - Port/path: METRICS_PORT (default 9503, the local monolith),
//     QNAP_METRICS_PORT (default 9501, the qnap controller), QNAP_NODE_METRICS_PORT
//     (default 9502, the qnap node), all under METRICS_PATH (default /metrics).
//   - If NO endpoint is reachable the suite SKIPS (an environment gap, not a
//     plugin bug); once a scrape connects, missing/incorrect metrics FAIL.

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestIntegration_Observability_Local exercises the local backend's metrics over
// a full create→mount→unmount→delete cycle, asserting the summed gauges/counters
// move, plus a deliberate bad-pool create to prove the RPC error counter
// increments (failure domain).
func TestIntegration_Observability_Local(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.localPluginID, 1, false)

	port := envOr("METRICS_PORT", "9503") // local monolith (qnap controller 9501 / node 9502)
	path := envOr("METRICS_PATH", "/metrics")
	hosts := c.reachableMetricsHosts(t, port, path)
	t.Logf("observability: scraping %d local endpoint(s) on :%s%s -> %v", len(hosts), port, path, hosts)

	// sum totals a metric family across every reachable endpoint (a forwarded
	// create / a stage can land on any node, so we never assume which one).
	sum := func(name string, labels ...string) float64 {
		return c.scrapeSum(t, hosts, port, path, name, labels...)
	}

	// Sanity that the endpoint exposes our namespace. Only check ALWAYS-present
	// families: rpc_total (the plugin serves RPCs continuously) and registered
	// gauges (exposed at 0 even before first use). Labeled counters like
	// local_zfs_op_total are CounterVecs that Prometheus does NOT emit until their
	// first .Inc(), so they can be absent on a given node — those are validated by
	// the cross-node delta assertions below, not a single-host presence check.
	probe := c.scrapeParsed(t, hosts[0], port, path)
	for _, fam := range []string{
		"nomad_csi_rpc_total",
		"nomad_csi_node_staged_volumes",
		"nomad_csi_local_peers",
	} {
		require.True(t, hasFamily(probe, fam), "metric family %q missing from %s:%s%s", fam, hosts[0], port, path)
	}

	baseStaged := sum("nomad_csi_node_staged_volumes")
	baseCreate := sum("nomad_csi_local_zfs_op_total", `op="create"`, `outcome="ok"`)
	baseMount := sum("nomad_csi_node_mount_total")
	baseDestroy := sum("nomad_csi_local_zfs_op_total", `op="destroy"`, `outcome="ok"`)

	// --- lifecycle: create pinned to a node, mount via the consumer ---
	node := c.pickNode(t)
	volID := fmt.Sprintf("ncd-obs-%d", time.Now().UnixNano())
	c.createLocalVolume(t, volID, node, "")
	t.Cleanup(func() { c.deleteVolume(volID) })
	c.runConsumer(t, volID)
	t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })
	require.Equal(t, node, c.consumerNode(t), "consumer should be pinned to the volume's node")

	require.Equal(t, baseStaged+1, sum("nomad_csi_node_staged_volumes"),
		"summed node_staged_volumes should rise by 1 after a stage")
	require.Greater(t, sum("nomad_csi_local_zfs_op_total", `op="create"`, `outcome="ok"`), baseCreate,
		"local_zfs_op_total{op=create,outcome=ok} should rise after a create")
	require.Greater(t, sum("nomad_csi_node_mount_total"), baseMount,
		"node_mount_total should rise after a stage's format+mount")

	// --- teardown: stop the consumer (releases the claim, unstages) ---
	c.stopConsumer(t, volID)
	require.Equal(t, baseStaged, sum("nomad_csi_node_staged_volumes"),
		"summed node_staged_volumes should return to baseline after unstage")

	// --- delete: the forwarded zfs destroy lands on the owner node ---
	c.requireVolumeDeleted(t, volID)
	require.Greater(t, sum("nomad_csi_local_zfs_op_total", `op="destroy"`, `outcome="ok"`), baseDestroy,
		"local_zfs_op_total{op=destroy,outcome=ok} should rise after a delete")

	// --- failure domain: a bad-pool create returns InvalidArgument, recorded by
	// whichever controller Nomad routed the RPC to — the cross-node sum catches it. ---
	beforeErr := sum("nomad_csi_rpc_total", `method="CreateVolume"`, `code="InvalidArgument"`)
	c.createVolumeExpectError(t, fmt.Sprintf("ncd-obs-bad-%d", time.Now().UnixNano()), node, "ncd-no-such-pool")
	c.poll(t, "rpc InvalidArgument counter increments after a bad create", 30*time.Second, func() bool {
		return sum("nomad_csi_rpc_total", `method="CreateVolume"`, `code="InvalidArgument"`) >= beforeErr+1
	})
}

// TestIntegration_Observability_QNAP exercises the qnap backend's metrics across
// a create→mount→unmount→delete cycle. qnap is two separate processes on two
// ports — the controller (talks to the appliance) and the per-node plugin (iSCSI
// + mount) — so it scrapes both and asserts each side's families move. Optional:
// skips cleanly where no qnap appliance/plugin is deployed.
func TestIntegration_Observability_QNAP(t *testing.T) {
	c := newClient(t)
	c.requirePluginHealthy(t, c.qnapPluginID, 1, true)

	path := envOr("METRICS_PATH", "/metrics")
	ctrlPort := envOr("QNAP_METRICS_PORT", "9501")
	nodePort := envOr("QNAP_NODE_METRICS_PORT", "9502")

	// The controller is a count-1 service, so only its node answers on ctrlPort;
	// the node plugin is a daemonset, so every node answers on nodePort.
	ctrlHosts := c.reachableMetricsHosts(t, ctrlPort, path)
	nodeHosts := c.reachableMetricsHosts(t, nodePort, path)
	t.Logf("observability(qnap): controller %v:%s, nodes %v:%s%s", ctrlHosts, ctrlPort, nodeHosts, nodePort, path)

	ctrlSum := func(name string, labels ...string) float64 {
		return c.scrapeSum(t, ctrlHosts, ctrlPort, path, name, labels...)
	}
	nodeSum := func(name string, labels ...string) float64 {
		return c.scrapeSum(t, nodeHosts, nodePort, path, name, labels...)
	}

	// Sanity that each endpoint exposes our namespace. Only check ALWAYS-present
	// families: rpc_total (the controller serves RPCs continuously) and the
	// node_staged_volumes gauge (exposed at 0 from startup). The qnap labeled
	// counters (qnap_op_total, qnap_iscsi_login_total, qnap_node_stage_total) are
	// CounterVecs that Prometheus does NOT emit until their first .Inc() — so they
	// are absent on a controller/node until activity occurs THERE (e.g. an iSCSI
	// login only touches the node a LUN is staged on). Those are validated by the
	// cross-node delta assertions below, not a single-host presence check.
	require.True(t, hasFamily(c.scrapeParsed(t, ctrlHosts[0], ctrlPort, path), "nomad_csi_rpc_total"),
		"controller endpoint missing nomad_csi_rpc_total")
	require.True(t, hasFamily(c.scrapeParsed(t, nodeHosts[0], nodePort, path), "nomad_csi_node_staged_volumes"),
		"node endpoint missing nomad_csi_node_staged_volumes")

	baseOpsOK := ctrlSum("nomad_csi_qnap_op_total", `outcome="ok"`)
	baseCreateRPC := ctrlSum("nomad_csi_rpc_total", `method="CreateVolume"`, `code="OK"`)
	baseStaged := nodeSum("nomad_csi_node_staged_volumes")
	baseMount := nodeSum("nomad_csi_node_mount_total")
	baseLogin := nodeSum("nomad_csi_qnap_iscsi_login_total", `outcome="ok"`)
	baseStage := nodeSum("nomad_csi_qnap_node_stage_total")

	// --- create: the controller provisions the LUN/target on the appliance ---
	b := qnapBackend(c)
	volID := fmt.Sprintf("ncd-obs-qnap-%d", time.Now().UnixNano())
	c.createCSIVolume(t, b, volID, "", "")
	t.Cleanup(func() { c.deleteVolume(volID) })
	require.Greater(t, ctrlSum("nomad_csi_qnap_op_total", `outcome="ok"`), baseOpsOK,
		"appliance ops should rise after a create")
	require.Greater(t, ctrlSum("nomad_csi_rpc_total", `method="CreateVolume"`, `code="OK"`), baseCreateRPC,
		"rpc_total{CreateVolume,OK} should rise after a create")

	// --- mount: the node logs in over iSCSI, formats, mounts ---
	c.runConsumer(t, volID)
	t.Cleanup(func() { _ = c.nomad("job", "stop", "-purge", consumerJob) })
	require.Equal(t, baseStaged+1, nodeSum("nomad_csi_node_staged_volumes"),
		"summed node_staged_volumes should rise by 1 after a stage")
	require.Greater(t, nodeSum("nomad_csi_node_mount_total"), baseMount,
		"node_mount_total should rise after a stage's format+mount")
	require.Greater(t, nodeSum("nomad_csi_qnap_iscsi_login_total", `outcome="ok"`), baseLogin,
		"qnap_iscsi_login_total{outcome=ok} should rise after a stage")
	require.Greater(t, nodeSum("nomad_csi_qnap_node_stage_total"), baseStage,
		"qnap_node_stage_total should rise after a stage")

	// --- teardown + delete (controller detaches the LUN on the appliance) ---
	c.stopConsumer(t, volID)
	require.Equal(t, baseStaged, nodeSum("nomad_csi_node_staged_volumes"),
		"summed node_staged_volumes should return to baseline after unstage")

	beforeDeleteOps := ctrlSum("nomad_csi_qnap_op_total", `outcome="ok"`)
	c.requireVolumeDeleted(t, volID)
	require.Greater(t, ctrlSum("nomad_csi_qnap_op_total", `outcome="ok"`), beforeDeleteOps,
		"appliance ops should rise after a delete")

	// --- failure domain: an unsupported fsType is rejected by the controller in
	// resolveParams (no appliance call), bumping the RPC InvalidArgument counter. ---
	beforeErr := ctrlSum("nomad_csi_rpc_total", `method="CreateVolume"`, `code="InvalidArgument"`)
	c.createSpecExpectError(t, fmt.Sprintf("ncd-obs-qnap-bad-%d", time.Now().UnixNano()),
		c.qnapPluginID, b.capMin, "  fsType = \"btrfs\"\n", "create with an unsupported fsType should fail")
	c.poll(t, "rpc InvalidArgument counter increments after a bad create", 30*time.Second, func() bool {
		return ctrlSum("nomad_csi_rpc_total", `method="CreateVolume"`, `code="InvalidArgument"`) >= beforeErr+1
	})
}

// --- host discovery ---

// metricsHosts is the list of endpoints to scrape. METRICS_HOSTS (comma-separated)
// overrides API discovery — required when Nomad advertises an IP that isn't
// routable from the test host (e.g. a VirtualBox NAT address). Otherwise hosts are
// derived from each ready node's HTTPAddr.
func (c *client) metricsHosts(t *testing.T) []string {
	t.Helper()
	if v := os.Getenv("METRICS_HOSTS"); strings.TrimSpace(v) != "" {
		var hosts []string
		for _, h := range strings.Split(v, ",") {
			if h = strings.TrimSpace(h); h != "" {
				hosts = append(hosts, h)
			}
		}
		return hosts
	}
	addrs := c.readyNodeAddrs(t)
	hosts := make([]string, 0, len(addrs))
	for _, h := range addrs {
		hosts = append(hosts, h)
	}
	sort.Strings(hosts)
	return hosts
}

// reachableMetricsHosts filters metricsHosts to endpoints that actually answer,
// skipping the suite if none do (an environment/routing gap, not a plugin bug).
func (c *client) reachableMetricsHosts(t *testing.T, port, path string) []string {
	t.Helper()
	candidates := c.metricsHosts(t)
	var ok []string
	for _, h := range candidates {
		if _, err := c.tryScrape(h, port, path); err == nil {
			ok = append(ok, h)
		}
	}
	if len(ok) == 0 {
		t.Skipf("no /metrics endpoint reachable on :%s from the test host (tried %v); "+
			"set METRICS_HOSTS to the routable node IPs and METRICS_PORT to the deployed port", port, candidates)
	}
	return ok
}

// readyNodeAddrs maps each ready node's name to its host IP (from HTTPAddr). Used
// only as the fallback host source when METRICS_HOSTS is unset.
func (c *client) readyNodeAddrs(t *testing.T) map[string]string {
	t.Helper()
	var nodes []struct {
		ID     string
		Name   string
		Status string
	}
	require.NoError(t, c.apiGet("/v1/nodes", &nodes))
	out := map[string]string{}
	for _, n := range nodes {
		if n.Status != "ready" {
			continue
		}
		var nd struct{ HTTPAddr string }
		if err := c.apiGet("/v1/node/"+n.ID, &nd); err != nil || nd.HTTPAddr == "" {
			continue
		}
		host, _, err := net.SplitHostPort(nd.HTTPAddr)
		if err != nil {
			host = nd.HTTPAddr // already bare?
		}
		out[n.Name] = host
	}
	return out
}

// pickNode returns a deterministic ready node name to pin the test volume to.
func (c *client) pickNode(t *testing.T) string {
	t.Helper()
	ns := c.readyNodes(t)
	require.NotEmpty(t, ns, "no ready nodes")
	sort.Strings(ns)
	return ns[0]
}

// --- scrape + parse ---

var scrapeHTTP = &http.Client{Timeout: 10 * time.Second}

// tryScrape GETs the metrics endpoint, returning the body or a connection error.
func (c *client) tryScrape(host, port, path string) (string, error) {
	url := fmt.Sprintf("http://%s:%s%s", host, port, path)
	resp, err := scrapeHTTP.Get(url)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return string(body), nil
}

// scrapeParsed scrapes one endpoint and parses it; it fails the test on error
// (callers confirm reachability via reachableMetricsHosts first).
func (c *client) scrapeParsed(t *testing.T, host, port, path string) []sample {
	t.Helper()
	raw, err := c.tryScrape(host, port, path)
	require.NoError(t, err, "scrape %s:%s%s", host, port, path)
	return parsePromText(raw)
}

// scrapeSum sums a counter/gauge family across every host (an RPC outcome / a
// forwarded op lands on whichever node served it, so we never assume which one).
func (c *client) scrapeSum(t *testing.T, hosts []string, port, path, name string, labelContains ...string) float64 {
	t.Helper()
	var total float64
	for _, h := range hosts {
		total += sumSeries(c.scrapeParsed(t, h, port, path), name, labelContains...)
	}
	return total
}

// createSpecExpectError writes a volume spec and asserts `volume create` fails.
// Nothing is registered on a controller-side error, so there's no volume to clean
// up — used to exercise the RPC error counters (failure domain).
func (c *client) createSpecExpectError(t *testing.T, id, pluginID, capMin, params, why string) {
	t.Helper()
	spec := fmt.Sprintf(volumeTmpl, id, id, pluginID, capMin, "", params)
	p := filepath.Join(c.dir, id+".volume.hcl")
	require.NoError(t, os.WriteFile(p, []byte(spec), 0o644))
	require.Error(t, c.nomad("volume", "create", p), why)
}

// createVolumeExpectError writes a local volume spec with an unknown pool and
// asserts it fails (→ InvalidArgument at the controller).
func (c *client) createVolumeExpectError(t *testing.T, id, host, pool string) {
	t.Helper()
	params := fmt.Sprintf("  host   = %q\n  fsType = \"ext4\"\n", host)
	if pool != "" {
		params += fmt.Sprintf("  pool   = %q\n", pool)
	}
	c.createSpecExpectError(t, id, c.localPluginID, "64MiB", params, "create with an unknown pool should fail")
}

// --- Prometheus text parsing (minimal; bounded label sets, no exotic syntax) ---

type sample struct {
	name   string // metric family name (without the {labels})
	labels string // the raw "{...}" label block, or "" if none
	val    float64
}

func parsePromText(raw string) []sample {
	var out []sample
	for _, ln := range strings.Split(raw, "\n") {
		ln = strings.TrimSpace(ln)
		if ln == "" || strings.HasPrefix(ln, "#") {
			continue
		}
		sp := strings.LastIndexByte(ln, ' ')
		if sp < 0 {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(ln[sp+1:]), 64)
		if err != nil {
			continue
		}
		metric := ln[:sp]
		name, labels := metric, ""
		if i := strings.IndexByte(metric, '{'); i >= 0 {
			name, labels = metric[:i], metric[i:]
		}
		out = append(out, sample{name: name, labels: labels, val: v})
	}
	return out
}

// sumSeries sums every series of family `name` whose label block contains all of
// labelContains (e.g. `op="create"`). With no label filter it sums the family.
func sumSeries(samples []sample, name string, labelContains ...string) float64 {
	var total float64
	for _, s := range samples {
		if s.name != name {
			continue
		}
		match := true
		for _, lc := range labelContains {
			if !strings.Contains(s.labels, lc) {
				match = false
				break
			}
		}
		if match {
			total += s.val
		}
	}
	return total
}

func hasFamily(samples []sample, name string) bool {
	for _, s := range samples {
		if s.name == name {
			return true
		}
	}
	return false
}
