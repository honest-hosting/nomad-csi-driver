//go:build integration

package e2e

// End-to-end harness against an EXTERNAL Nomad cluster (managed outside this
// repo). The CSI plugin jobs are deployed out of band by
// `make test-integration-deploy` (see ../localdev/); these suites assume the
// plugins are already healthy and only create/clean up the volumes and consumer
// they need (via t.Cleanup). They never deploy or tear down the plugin jobs.
//
// Driven via the `nomad` CLI (which honors NOMAD_ADDR / NOMAD_TOKEN /
// NOMAD_SKIP_VERIFY) and the Nomad HTTP API. Everything skips unless NOMAD_ADDR
// is set; cluster prerequisites are in ../localdev/README.md.
//
// This file is the shared harness only. The tests live in:
//   - lifecycle_test.go — the full create→mount→write→read→persist→delete
//     lifecycle, run identically against BOTH backends (local + qnap).
//   - placement_test.go — local-only behaviors (topology pinning, host=auto,
//     pool selection, forwarded delete) that don't apply to network storage.

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	consumerJob  = "ncd-e2e-consumer"
	consumerTask = "t" // task name inside consumer.nomad.hcl (group "g", task "t")
	markerPath   = "/mnt/vol/ncd-e2e.marker"
)

// --- client against the external cluster ---

type client struct {
	t             *testing.T
	addr          string
	localPluginID string
	qnapPluginID  string
	http          *http.Client
	dir           string
	repo          string

	// createdSnaps tracks every snapshot the run makes so teardown can delete
	// them BEFORE their volumes — snapshots are dependent, so a leftover one
	// blocks (FailedPrecondition) its volume's delete and orphans both.
	createdSnaps []snapRef
}

type snapRef struct{ plugin, id string }

func newClient(t *testing.T) *client {
	t.Helper()
	addr := os.Getenv("NOMAD_ADDR")
	if addr == "" {
		t.Skip("NOMAD_ADDR not set; point it at an external Nomad cluster (see localdev/README.md)")
	}
	tr := &http.Transport{}
	if v := os.Getenv("NOMAD_SKIP_VERIFY"); v == "1" || v == "true" {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // opt-in for self-signed dev clusters
	}
	return &client{
		t:             t,
		addr:          strings.TrimRight(addr, "/"),
		localPluginID: envOr("LOCAL_INTEGRATION_PLUGIN_ID", "nomad-csi-driver-local"),
		qnapPluginID:  envOr("QNAP_INTEGRATION_PLUGIN_ID", "nomad-csi-driver-qnap"),
		http:          &http.Client{Timeout: 30 * time.Second, Transport: tr},
		dir:           t.TempDir(),
		repo:          repoRoot(t),
	}
}

// requirePluginHealthy waits for a CSI plugin to report healthy. When optional,
// a plugin that never becomes healthy skips the test (e.g. qnap with no
// appliance) rather than failing it.
func (c *client) requirePluginHealthy(t *testing.T, pluginID string, minNodes int, optional bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		var p struct {
			ControllersHealthy int
			NodesHealthy       int
		}
		if err := c.apiGet("/v1/plugin/csi/"+pluginID, &p); err == nil &&
			p.ControllersHealthy >= 1 && p.NodesHealthy >= minNodes {
			return
		}
		if time.Now().After(deadline) {
			msg := fmt.Sprintf("CSI plugin %q not healthy (need >=1 controller, >=%d node(s))", pluginID, minNodes)
			if optional {
				t.Skipf("%s — skipping; deploy it (make test-integration-deploy) to run this test", msg)
			}
			t.Fatalf("%s — run `make test-integration-deploy` first", msg)
		}
		time.Sleep(2 * time.Second)
	}
}

func (c *client) readyNodes(t *testing.T) []string {
	t.Helper()
	var nodes []struct {
		Name   string
		Status string
	}
	require.NoError(t, c.apiGet("/v1/nodes", &nodes))
	var out []string
	for _, n := range nodes {
		if n.Status == "ready" {
			out = append(out, n.Name)
		}
	}
	return out
}

// --- volume helpers ---

// createLocalVolume registers a --driver=local volume. host selects the node
// ("<name>" or "auto"); pool optionally selects a zpool (empty = default_pool).
func (c *client) createLocalVolume(t *testing.T, id, host, pool string) string {
	t.Helper()
	params := fmt.Sprintf("  host   = %q\n  fsType = \"ext4\"\n", host)
	if pool != "" {
		params += fmt.Sprintf("  pool   = %q\n", pool)
	}
	c.createVolumeSpec(t, id, c.localPluginID, "64MiB", "", params)
	return id
}

// createCSIVolume registers a volume for backend b. node is the placement hint
// (local honors host=<node>/auto; qnap ignores it); sourceLine is an optional
// top-level spec attribute such as `snapshot_id = "…"` or `clone_id = "…"`.
func (c *client) createCSIVolume(t *testing.T, b backend, id, node, sourceLine string) {
	t.Helper()
	c.createVolumeSpec(t, id, b.pluginID, b.capMin, sourceLine, b.params(node))
}

func (c *client) createVolumeSpec(t *testing.T, id, pluginID, capacityMin, sourceLine, params string) {
	t.Helper()
	spec := fmt.Sprintf(volumeTmpl, id, id, pluginID, capacityMin, sourceLine, params)
	path := filepath.Join(c.dir, id+".volume.hcl")
	require.NoError(t, os.WriteFile(path, []byte(spec), 0o644))
	require.NoError(t, c.nomad("volume", "create", path))
}

func (c *client) deleteVolume(id string) {
	_ = c.nomad("volume", "delete", id)
}

// requireVolumeDeleted deletes a volume and retries until it succeeds. A clean
// delete is the signal that no dependent snapshot remains; async snapshot
// cleanup (qnap) can briefly block it, while a persistent failure (e.g. a leaked
// inherited snapshot on a clone/restore) correctly fails the test.
func (c *client) requireVolumeDeleted(t *testing.T, id string) {
	t.Helper()
	c.poll(t, "volume "+id+" deletes cleanly (no dependent snapshots)", 90*time.Second, func() bool {
		return c.nomad("volume", "delete", id) == nil
	})
}

// externalID returns a volume's CSI external id (what the driver parses). Nomad
// forwards clone_id verbatim to the driver, so a clone must reference the source
// by this id, not by its Nomad registration id.
func (c *client) externalID(t *testing.T, volID string) string {
	t.Helper()
	var v struct{ ExternalID string }
	require.NoError(t, c.apiGet("/v1/volume/csi/"+volID, &v))
	require.NotEmpty(t, v.ExternalID, "volume %s has no external id", volID)
	return v.ExternalID
}

// --- consumer helpers ---

func (c *client) runConsumer(t *testing.T, volID string) {
	t.Helper()
	job := filepath.Join(c.repo, "localdev", "consumer.nomad.hcl")
	require.NoError(t, c.nomad("job", "run", "-var", "volume_id="+volID, job))
	c.poll(t, "consumer running", 120*time.Second, func() bool {
		return c.consumerStatus() == "running"
	})
}

func (c *client) stopConsumer(t *testing.T, volID string) {
	t.Helper()
	require.NoError(t, c.nomad("job", "stop", "-purge", consumerJob))
	// Wait for the CSI claim to release, NOT for the alloc to be GC'd. Alloc/job GC
	// for a CSI consumer is gated behind the CSI claim GC (CSIVolumeClaimGCThreshold,
	// ~5m), so polling for the job's allocation list to empty would time out even on
	// a clean stop. The claim releases on NodeUnstage/ControllerUnpublish within
	// seconds — and it's the released claim (not GC) that lets the volume be
	// re-mounted or deleted. A volume with no active Allocations holds no claim.
	c.poll(t, "consumer claim released", 90*time.Second, func() bool {
		var v struct {
			Allocations []struct{ ID string }
		}
		if err := c.apiGet("/v1/volume/csi/"+volID, &v); err != nil {
			return false
		}
		return len(v.Allocations) == 0
	})
}

func (c *client) consumerNode(t *testing.T) string {
	t.Helper()
	a, ok := c.runningAlloc()
	require.True(t, ok, "consumer has no live allocation")
	return a.NodeName
}

func (c *client) consumerStatus() string {
	a, ok := c.runningAlloc()
	if !ok {
		return ""
	}
	return a.ClientStatus
}

// runningAlloc returns the consumer's current non-terminal allocation. After a
// purge+rerun the previous run's terminal alloc can still be listed (alloc GC
// lags the claim release), so we skip terminal allocs to read the live one.
func (c *client) runningAlloc() (allocInfo, bool) {
	for _, a := range c.allocs(consumerJob) {
		if !isTerminal(a.ClientStatus) {
			return a, true
		}
	}
	return allocInfo{}, false
}

// isTerminal reports whether a Nomad client status means the alloc has stopped.
func isTerminal(status string) bool {
	switch status {
	case "complete", "failed", "lost":
		return true
	}
	return false
}

type allocInfo struct {
	ID           string
	NodeName     string
	ClientStatus string
}

func (c *client) allocs(jobID string) []allocInfo {
	var out []allocInfo
	_ = c.apiGet("/v1/job/"+jobID+"/allocations", &out)
	return out
}

// allocOnNode returns a running alloc of jobID on the given node.
func (c *client) allocOnNode(t *testing.T, jobID, node string) string {
	t.Helper()
	for _, a := range c.allocs(jobID) {
		if a.NodeName == node && !isTerminal(a.ClientStatus) {
			return a.ID
		}
	}
	require.FailNowf(t, "alloc not found", "no running alloc of %q on node %q", jobID, node)
	return ""
}

// nodePluginMultipath runs `multipath -ll` inside the qnap node plugin on node
// (it carries the multipath tools and reaches the host multipathd), returning
// the raw listing. nodeJob is the node-plugin job id (e.g. "<plugin>-node").
func (c *client) nodePluginMultipath(t *testing.T, nodeJob, node string) string {
	t.Helper()
	alloc := c.allocOnNode(t, nodeJob, node)
	out, err := c.nomadOut("alloc", "exec", "-i=false", "-t=false", alloc, "/bin/sh", "-c", "multipath -ll")
	require.NoError(t, err, "exec multipath -ll on node plugin")
	return out
}

// countActivePaths counts active iSCSI paths in `multipath -ll` output — each
// path line ends with a running status (e.g. "5:0:0:0 sdb 8:16 active ready running").
func countActivePaths(out string) int {
	n := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "ready running") {
			n++
		}
	}
	return n
}

// --- data I/O (proves the mount is a usable, persistent filesystem) ---

// writeMarker writes token into the mounted volume via `nomad alloc exec` and
// fsyncs it, so it survives an unmount.
func (c *client) writeMarker(t *testing.T, token string) {
	t.Helper()
	a, ok := c.runningAlloc()
	require.True(t, ok, "no running consumer alloc to write into")
	_, err := c.nomadOut("alloc", "exec", "-i=false", "-t=false", "-task", consumerTask, a.ID,
		"/bin/sh", "-c", "printf %s "+token+" > "+markerPath+" && sync")
	require.NoError(t, err, "write marker into the mounted volume")
}

// readMarker reads the marker file back out of the mounted volume.
func (c *client) readMarker(t *testing.T) string {
	t.Helper()
	a, ok := c.runningAlloc()
	require.True(t, ok, "no running consumer alloc to read from")
	out, err := c.nomadOut("alloc", "exec", "-i=false", "-t=false", "-task", consumerTask, a.ID,
		"/bin/sh", "-c", "cat "+markerPath)
	require.NoError(t, err, "read marker from the mounted volume")
	return strings.TrimSpace(out)
}

func uniqueToken() string {
	return fmt.Sprintf("ncd-%d", time.Now().UnixNano())
}

// mountSizeKB returns the total size (1K-blocks) of the filesystem mounted at
// /mnt/vol in the running consumer — used to prove an expand actually grew it.
func (c *client) mountSizeKB(t *testing.T) int64 {
	t.Helper()
	a, ok := c.runningAlloc()
	require.True(t, ok, "no running consumer alloc to measure")
	out, err := c.nomadOut("alloc", "exec", "-i=false", "-t=false", "-task", consumerTask, a.ID,
		"/bin/sh", "-c", "df -k /mnt/vol | tail -1")
	require.NoError(t, err, "df the mounted volume")
	fields := strings.Fields(out)
	require.GreaterOrEqual(t, len(fields), 2, "unexpected df output: %q", out)
	kb, err := strconv.ParseInt(fields[1], 10, 64)
	require.NoError(t, err, "parse 1K-blocks from df output %q", out)
	return kb
}

// --- snapshot / clone lifecycle ops ---

// snapshotCreate snapshots volID and returns the CSI snapshot id (used as
// snapshot_id in a restore volume spec).
func (c *client) snapshotCreate(t *testing.T, volID, name string) string {
	t.Helper()
	out, err := c.nomadOut("volume", "snapshot", "create", volID, name)
	require.NoError(t, err, "create snapshot of %s", volID)
	id := snapshotIDFromOutput(out)
	require.NotEmpty(t, id, "could not parse snapshot id from `volume snapshot create` output:\n%s", out)
	return id
}

func (c *client) snapshotDelete(pluginID, snapID string) {
	_ = c.nomad("volume", "snapshot", "delete", pluginID, snapID)
}

// snapshotIDFromOutput pulls the Snapshot ID out of `nomad volume snapshot
// create` table output: a header row containing "Snapshot ID", then a data row
// whose first whitespace-delimited field is the (space-free) snapshot id. (The
// second column is labeled "Volume ID" on newer Nomad, "External ID" on older —
// so we key only off "Snapshot ID".)
func snapshotIDFromOutput(out string) string {
	lines := strings.Split(out, "\n")
	for i, ln := range lines {
		if strings.Contains(ln, "Snapshot ID") {
			for _, d := range lines[i+1:] {
				if d = strings.TrimSpace(d); d != "" {
					return strings.Fields(d)[0]
				}
			}
		}
	}
	return ""
}

// agentVersion reports the Nomad build version (e.g. "1.6.3") via /v1/agent/self,
// so the expand stage can gate on >= 1.8.0 (CSI expansion wasn't wired before then).
func (c *client) agentVersion(t *testing.T) string {
	t.Helper()
	var self struct {
		Member struct{ Tags map[string]string }
	}
	require.NoError(t, c.apiGet("/v1/agent/self", &self))
	if v := self.Member.Tags["build"]; v != "" {
		return v
	}
	return "unknown"
}

// versionAtLeast reports whether a "MAJOR.MINOR.PATCH"-ish version string is at
// least major.minor. Unparseable input is treated as older (returns false).
func versionAtLeast(have string, major, minor int) bool {
	have = strings.SplitN(have, "-", 2)[0]
	have = strings.SplitN(have, "+", 2)[0]
	parts := strings.Split(have, ".")
	if len(parts) < 2 {
		return false
	}
	hMaj, err1 := strconv.Atoi(parts[0])
	hMin, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return false
	}
	if hMaj != major {
		return hMaj > major
	}
	return hMin >= minor
}

// --- transport ---

// nomad runs the CLI and discards stdout. The CLI inherits NOMAD_ADDR /
// NOMAD_TOKEN / NOMAD_SKIP_VERIFY from the environment.
func (c *client) nomad(args ...string) error {
	_, err := c.nomadOut(args...)
	return err
}

// nomadOut runs the CLI and returns its combined output.
func (c *client) nomadOut(args ...string) (string, error) {
	cmd := exec.Command("nomad", args...)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("nomad %s: %v: %s", strings.Join(args, " "), err, out)
	}
	return string(out), nil
}

func (c *client) apiGet(path string, out any) error {
	req, err := http.NewRequest(http.MethodGet, c.addr+path, nil)
	if err != nil {
		return err
	}
	if tok := os.Getenv("NOMAD_TOKEN"); tok != "" {
		req.Header.Set("X-Nomad-Token", tok)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", path, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *client) poll(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(2 * time.Second)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "go.mod not found walking up from test dir")
		dir = parent
	}
}

// volumeTmpl args: id, name, plugin_id, capacity_min, sourceLine, parametersBody.
// One attribute per line (HCL2). sourceLine is "" or a top-level attribute like
// `snapshot_id = "…"` / `clone_id = "…"`; the parameters body is rendered by the
// caller.
const volumeTmpl = `
id        = "%s"
name      = "%s"
type      = "csi"
plugin_id = "%s"
capacity_min = "%s"
%s
capability {
  access_mode     = "single-node-writer"
  attachment_mode = "file-system"
}
parameters {
%s}
`
