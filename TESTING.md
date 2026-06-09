# Testing

## Unit tests — `make test`

Hermetic: no network, appliance, ZFS, or root required. Everything
side-effecting is faked behind seams:

- `internal/exec.FakeRunner` fakes `zfs`/`iscsiadm`/`multipath`/`mkfs`/`mount`/…
- a fake `qnap.Client` (in-memory model) replaces go-qnap for the controller.
- the local backend's forwarding is exercised with an in-process HTTP server.
- the CSI servers are driven over an in-memory `bufconn` gRPC dialer.

```bash
make test          # go test -race ./... (integration excluded by build tag)
```

## Integration / e2e tests — `make test-integration`

The integration suite (build-tag-gated `//go:build integration`) runs against an
**external, already-running Nomad cluster** (managed outside this repo — e.g. a
small 3-node dev cluster). It does **not** manage Nomad nodes, pools, or ZFS on
localhost. Docker-only (privileged container for ZFS); `raw_exec` is not
supported (the cgroup device controller blocks `/dev/zfs` outside a privileged
container).

Three steps — deploy the plugins, run the suite (re-runnable), tear down:

```bash
export NOMAD_ADDR=https://nomad.example:4646
export NOMAD_CSI_INTEGRATION_IMAGE=quay.io/honesthosting/nomad-csi-driver:latest   # pushed to a registry the cluster can pull

make test-integration-deploy      # deploy CSI plugin jobs (local + qnap), wait healthy
make test-integration             # volume/placement suite against the running plugins
make test-integration-teardown    # remove the plugin jobs + e2e volumes
```

`test-integration` runs **preflight** (checks `NOMAD_ADDR` is set + cluster
reachable, **bails** otherwise — no localhost ZFS checks) then `go test
-tags=integration -run ^TestIntegration ./...`. The Go suite **skips** if
`NOMAD_ADDR` is unset (so a bare `go test -tags=integration` is safe) and **fails
fast** if the local plugin isn't deployed.

Cluster prerequisites, env vars (`NOMAD_TOKEN`, `NOMAD_SKIP_VERIFY`, `NOMAD_CSI_INTEGRATION_IMAGE`,
`LOCAL_INTEGRATION_POOL1`, `LOCAL_INTEGRATION_PLUGIN_ID`), and how to build/push the plugin image (`make
package`) are in [`../localdev/README.md`](../localdev/README.md).

### Deployment (`make test-integration-deploy`)

`localdev/deploy.sh` deploys **both** engines and waits them healthy:
the **local** monolith (`localdev/csi-local.nomad.hcl`) and, when
`QNAP_INTEGRATION_URL` is set, **qnap** as the standard controller + node split —
a count-1 service controller (`csi-qnap-controller.nomad.hcl`) and a system
daemonset node (`csi-qnap-node.nomad.hcl`) sharing one plugin_id.

### The Nomad e2e (`TestIntegration_NomadCluster`)

Assumes the local plugin is deployed + healthy, then validates the behaviors only
Nomad can show for the **local** backend: topology pinning to the owning node,
controller→controller forwarding (a volume pinned to a chosen node is created
there and its consumer
lands there), purge/rerun re-pinning, and forwarded delete. It creates/cleans
only its own volumes + consumer (via `t.Cleanup`) and does not touch the plugin
jobs. (qnap's deploy + healthy is covered by `test-integration-deploy`; a full
qnap volume e2e is a follow-up.) The plugin image is built/pushed with `make
package` (Packer).

## qnap controller-level test (library against a real appliance)

Separate from the Nomad e2e: a controller-level test that drives our qnap
backend directly against a real sandbox appliance over the network (no Nomad
cluster, no localhost ZFS/root). It reuses the **same env vars as go-qnap's
integration suite**, and self-skips when they're absent.

| var | meaning |
| --- | --- |
| `QNAP_INTEGRATION_URL` | appliance base URL |
| `QNAP_INTEGRATION_USER` / `QNAP_INTEGRATION_PASSWORD` | sandbox admin creds |
| `QNAP_INTEGRATION_POOL_ID` | a Ready pool id (`qnapctl pools list`) |
| `QNAP_INTEGRATION_IFACES` | comma-separated iSCSI portal interfaces (e.g. `eth0`) |
| `QNAP_INTEGRATION_PORTAL` | (optional) iSCSI portal host for volume context |
| `QNAP_INTEGRATION_INSECURE` | (optional) `false` to require valid TLS; default insecure |

Run it directly (it needs no Nomad cluster, so don't go through
`make test-integration`, which requires `NOMAD_ADDR`):

```bash
QNAP_INTEGRATION_URL=https://qnap.example.com QNAP_INTEGRATION_USER=admin \
QNAP_INTEGRATION_PASSWORD=... QNAP_INTEGRATION_POOL_ID=1 QNAP_INTEGRATION_IFACES=eth0 \
go test -tags=integration -run '^TestIntegration_QNAP' ./internal/driver/qnap/...
```

### Early integration spikes (see NOMAD-CSI-DRIVER-ARCHITECTURE.md §10)

1. L4 forwarding + Nomad `/v1/nodes` peer-discovery end-to-end (incl. node-down mid-op).
2. Nomad 1.6.x claim-GC timing on purge → re-attach.
3. block-device passthrough via the docker task driver on 1.6.3.
4. QuTScloud-sandbox multipath spot-check.
