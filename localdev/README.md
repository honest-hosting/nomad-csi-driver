# localdev — integration/e2e against an external Nomad cluster

The integration suite does **not** manage a Nomad cluster. It targets an
**external, already-running** cluster (managed outside this repo — e.g. a small
3-node dev cluster) via `NOMAD_ADDR`, deploys the CSI job(s) here, verifies
volumes/allocs, and cleans up.

## Cluster prerequisites (you provide these)

- **Nomad ≥ 1.6.3**, reachable at `NOMAD_ADDR`.
- The **docker** task driver, with **privileged** containers and **host
  volumes** allowed:
  ```hcl
  plugin "docker" {
    config {
      allow_privileged = true
      volumes { enabled = true }
    }
  }
  ```
- **ZFS** on each node, with usable **zpool(s)** — the integration cluster has
  `tank1` (default pool, `LOCAL_INTEGRATION_POOL1`) and `tank2` (second pool, `LOCAL_INTEGRATION_POOL2`, used by
  the multi-pool selection subtest). A single-pool cluster works too: set
  `LOCAL_INTEGRATION_POOL2=""`. The plugin image's ZFS userland must match each node's kernel
  module version (see `../Dockerfile`; `debian:12` → zfs 2.1.x).
- **Workload identity** usable by the plugin task (the jobspec's `identity`
  block surfaces the token it uses to read Nomad's `/v1/nodes` for peer discovery
  and `/v1/volumes` for the stats query API's id resolution, over `api.sock`).
  With Nomad **ACLs enabled**, bind a policy granting `node:read` **and**
  `csi-list-volume` + `csi-read-volume` (in the volumes' namespace) to the plugin
  task; with ACLs off, no policy is needed. No Consul required.
- The **plugin image** must be pullable by the cluster — build and push it via
  Packer:
  ```sh
  make package               # packer build+push nomad-csi-driver image
  ```
  Point the suite at it with `NOMAD_CSI_INTEGRATION_IMAGE`.
- For the **qnap** engine: a reachable QNAP appliance + the `QNAP_INTEGRATION_*`
  creds (same vars as go-qnap), and cluster nodes that can iSCSI-login to it.

## Environment

| var | required | meaning |
| --- | --- | --- |
| `NOMAD_ADDR` | yes | external cluster API (e.g. `https://nomad.example:4646`) |
| `NOMAD_TOKEN` | no | ACL token (passed through to `nomad` + the API) |
| `NOMAD_SKIP_VERIFY` | no | `1`/`true` to skip TLS verification |
| `NOMAD_CSI_INTEGRATION_IMAGE` | recommended | plugin image (default `quay.io/honesthosting/nomad-csi-driver:latest`) |
| `LOCAL_INTEGRATION_POOL1` | no | default zpool name (default `tank1`) |
| `LOCAL_INTEGRATION_POOL2` | no | second zpool (default `tank2`): added to the deployed allowlist AND used by the multi-pool selection subtest. Set to `""` for a single-pool cluster (deploys one pool, skips the subtest). Must pre-exist on the node(s) that serve it |
| `LOCAL_INTEGRATION_PLUGIN_ID` | no | local CSI plugin id (default `nomad-csi-driver-local`) |
| `QNAP_INTEGRATION_*` | for qnap | appliance creds — deploys the qnap engine when `QNAP_INTEGRATION_URL` is set (URL, USER, PASSWORD, POOL_ID, IFACES; optional PORTAL, INSECURE) |
| `METRICS_HOSTS` | no | comma-separated node IPs/hostnames the observability suite scrapes (e.g. `192.168.56.51,192.168.56.52,192.168.56.53`). **Set this** when Nomad advertises an IP that isn't routable from the test host — on a VirtualBox host-only cluster Nomad often fingerprints the NAT address (`10.0.2.x`) while only the `192.168.56.x` host-only network is reachable. Unset → derived from each node's Nomad `HTTPAddr` |
| `METRICS_PORT` / `METRICS_PATH` | no | where the observability suite scrapes the **local monolith** (default `9503` / `/metrics`); must match the deployed `metrics_address` port |
| `QNAP_METRICS_PORT` / `QNAP_NODE_METRICS_PORT` | no | where the observability suite scrapes the **qnap controller** / **qnap node** (defaults `9501` / `9502`); must match the deployed ports |
| `LOCAL_INTEGRATION_METRICS_PORT` / `QNAP_INTEGRATION_METRICS_PORT` / `QNAP_INTEGRATION_NODE_METRICS_PORT` | no | ports `deploy.sh` binds the `/metrics` endpoints to: local monolith `9503`, qnap controller `9501`, qnap node `9502` (chosen so all three can co-exist on one host-networked node); keep `METRICS_PORT` in sync with the local one |

The suite deploys both storage engines:

- **local** — one privileged docker **monolith** system job
  (`nomad-csi-driver-local`).
- **qnap** (when `QNAP_INTEGRATION_URL` is set) — controller-XOR-node, the
  standard CSI shape: a **count-1 service** controller
  (`nomad-csi-driver-qnap-controller`) + a **system daemonset** node
  (`nomad-csi-driver-qnap-node`), sharing plugin_id `nomad-csi-driver-qnap`.

The local engine gets the full volume/placement assertions; qnap is deployed and
checked healthy as a smoke test.

## Run

```sh
export NOMAD_ADDR=https://nomad.example:4646
export NOMAD_CSI_INTEGRATION_IMAGE=quay.io/honesthosting/nomad-csi-driver:latest

make test-integration-deploy     # deploy the CSI plugin jobs (local + qnap), wait healthy
make test-integration            # run the volume/placement suite against them (re-runnable)
make test-integration-teardown   # remove the plugin jobs + e2e volumes/consumers
```

`test-integration-deploy` registers the plugin jobs and leaves them running;
`test-integration` only creates/cleans the volumes + consumer it needs and will
fail fast if the local plugin isn't deployed.

## Files

- `csi-local.nomad.hcl` — the local-backend CSI plugin (privileged docker
  monolith; peer discovery via Nomad `/v1/nodes` over `api.sock`). Registered by
  the suite via `nomad job run -var`.
- `csi-qnap-controller.nomad.hcl` / `csi-qnap-node.nomad.hcl` — the qnap plugin
  split into a count-1 service controller + a system daemonset node (shared
  plugin_id). Deployed when `QNAP_INTEGRATION_URL` is set.
- `consumer.nomad.hcl` — a docker consumer that mounts a CSI volume (used to
  prove topology pinning).
- `preflight.sh` / `teardown.sh` — invoked by the Makefile.
