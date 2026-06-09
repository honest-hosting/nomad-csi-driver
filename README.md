# nomad-csi-driver

A Container Storage Interface (CSI) driver for **HashiCorp Nomad** with pluggable
storage backends selected by `--driver`. There is **no Kubernetes support** — Nomad
only (floor: v1.6.3). It is the Go, Nomad-native replacement for `democratic-csi`.

Ships as a single binary (`nomad-csi-driver`) and a single image
(`quay.io/honesthosting/nomad-csi-driver`). The backend identity lives at the
`--driver` value and in the metrics `driver=` label; the Nomad `plugin_id` is
operator-chosen per deploy.

## Backends

| | `--driver=qnap` (SAN) | `--driver=local` (ZFS) |
| --- | --- | --- |
| Storage | iSCSI block LUNs on a QNAP appliance (via `github.com/honest-hosting/go-qnap`) | node-local thick ZFS zvols |
| Modes | `controller` XOR `node` | `monolith` on every node (single `plugin_id`) |
| Topology | reachable from any node | node-pinned (CSI topology) |
| Placement | scheduler-driven (any node) | node chosen at create (`parameters.host=auto\|<node>`); controller↔controller forwarding |
| Snapshots (dependent point-in-time) | yes — QNAP LUN snapshot | yes — ZFS `@snap` child |
| Clones (independent copy, new id) | yes — full LUN copy | yes — `send\|recv` |
| Snapshot list | yes (`nomad volume snapshot list`) | yes (aggregated across nodes) |
| Sizing | whole-GiB exact | rounded up to `volblocksize` |
| Filesystems | ext4 / xfs (+ block-device) | ext4 / xfs (+ block-device) |

Both backends produce a block device and then share the same idempotent
format/mount layer.

### Snapshot & clone semantics

- **Snapshot** = a **dependent** point-in-time image of a volume with its own
  id/lifecycle (`nomad volume snapshot {create,delete,list}`). Because it depends
  on its source, **`nomad volume delete` is refused (`FailedPrecondition`) while a
  volume still has snapshots** — delete the snapshots first.
- **Clone** = an **independent**, byte-for-byte copy into a **new** volume
  (`clone_id = "<source>"` in the volume spec); the source is unaffected. (local
  clones land on the source's node; qnap clones are node-mobile.)
- **Restore / "rollback"** = provision a **new** volume from a snapshot
  (`snapshot_id = "<snap>"`) and repoint the job. There is **no in-place rollback**
  via Nomad/CSI — CSI has no revert RPC. True in-place `zfs rollback` / QNAP revert
  exists at the storage layer but must be done out-of-band (Nomad-bypassing).

### Access modes & data safety

- **Access modes**: the driver accepts every **single-node** CSI access mode
  (`SINGLE_NODE_WRITER`, `SINGLE_NODE_SINGLE_WRITER`, `SINGLE_NODE_MULTI_WRITER`,
  `SINGLE_NODE_READER_ONLY`) and **rejects all multi-node modes** with
  `InvalidArgument` — ext4/xfs are not cluster filesystems. (This is the CSI gRPC
  access-mode surface, which is broader than the set of strings Nomad's HCL exposes.)
- **Deletes are explicit-only**: a volume/snapshot is destroyed solely in response
  to an operator's `nomad volume delete` (mapped to CSI `DeleteVolume`/
  `DeleteSnapshot`). The driver runs **no reconcile/orphan-sweep that destroys
  storage** — there is no background process that can delete a volume you didn't ask
  it to.
- **Idempotent, non-clobbering**: an existing filesystem is never reformatted
  (blkid probe), a ZFS destroy is leaf-only (never `-r`/`-f`), and expansion is
  grow-only. A clone/restore records its source so an idempotent retry can't
  silently alias a same-named volume from a different source.

## Usage

```bash
nomad-csi-driver run \
  --mode            {controller|node|monolith} \
  --driver          {qnap|local} \
  --endpoint        unix:///csi/csi.sock     # or $CSI_ENDPOINT
  --node-id         ${node.unique.id}        # node/monolith modes; or $CSI_NODE_ID
  --parent-dataset  nomad-csi                # --driver=local only; ZFS parent dataset (see below)
  --config          /local/config.hcl
```

- `qnap`: a process is `controller` (one per cluster/DC, sole talker to the
  appliance) **xor** `node` (every client; iSCSI + multipath + format/mount).
  `--parent-dataset` is **not used** by qnap (LUNs are named per-volume and
  namespaced on the appliance).
- `local`: `monolith` on every node under one `plugin_id`; controllers forward
  create/delete/expand/snapshot to the owning node (peer discovery via Nomad's
  `/v1/nodes` API over the task API socket — the plugin task needs an `identity`
  block, plus a `node:read` policy when ACLs are enabled).

### `--parent-dataset` (`--driver=local`)

`--parent-dataset` is the ZFS dataset under each pool that holds provisioned
zvols: they are created at `<pool>/<parent-dataset>/<volume-id>`. It **defaults to
`nomad-csi`** and is optional. A pool may override it for itself via
`parent_dataset` in its `pool { … }` block (the per-pool value wins).

**Recommended for multi-deployment clusters: set it to your Nomad
`csi_plugin.id`** (e.g. `--parent-dataset=${var.plugin_id}` alongside
`csi_plugin { id = var.plugin_id }`). The plugin id is the volume namespace at the
Nomad level, so reusing it on disk keeps the two consistent and lets multiple
local deployments share a pool without colliding (`<pool>/<plugin-A>/…` vs
`<pool>/<plugin-B>/…`). For a single local deployment the `nomad-csi` default is
fine — pick whatever dataset name you like.

Configuration is HCL (JSON fallback) — the controller/node deployment config, not
the CSI volume spec. See [examples/](examples/) for example jobspecs, config,
volume definitions, and consumer jobs (walked through under [Examples](#examples)).

## Security model

> **The `local` forwarding transport is unencrypted and trusts the network.**

For `--driver=local`, controllers forward create/delete/expand/snapshot operations
to the owning node over an HTTP server (default `:9602`, set by
`local.forward_addr`). Requests are authenticated by a **static shared secret**
(`local.forward_secret`) sent in the `X-NCD-Secret` header — **in cleartext, over
plain HTTP**. There is **no TLS/mTLS** in this release (deliberately deferred).

Consequences and required deployment posture:

- Anyone who can reach or sniff the forwarding port can replay the secret and then
  issue `zfs create`/`destroy` against any node — a storage-destruction surface.
- Run the forwarding port **only on a trusted, isolated L2 segment** (e.g. a
  dedicated storage/back-end network), and **firewall `:9602`** to the peer
  controllers. Do not expose it on an untrusted or shared network.
- The secret is loaded from config (file), never passed on argv.

The `qnap` backend has no peer-forwarding transport. mTLS for `local` forwarding is
tracked as future hardening.

## Examples

[`examples/`](examples/) holds a complete, copy-pasteable set per backend —
plugin jobspec(s), the deployment config they template in, a CSI volume
definition, and a consumer job that mounts the volume and stays up
(`tail -f /dev/null`) so you can exec in and look around:

| File | Purpose |
| --- | --- |
| `local-monolith.nomad.hcl` / `local-config.hcl` | local plugin (monolith system job) + its config |
| `local-volume.hcl` | `nomad volume create` spec for a local ZFS volume |
| `local-consumer.nomad.hcl` | job that mounts the local volume at `/data` |
| `qnap-controller.nomad.hcl` / `qnap-node.nomad.hcl` / `qnap-config.hcl` | qnap controller + node plugins + config |
| `qnap-volume.hcl` | `nomad volume create` spec for a qnap iSCSI volume |
| `qnap-consumer.nomad.hcl` | job that mounts the qnap volume at `/data` |

The jobspecs are starting points — edit the `image`, secrets (sourced via
`nomadVar`), pool names, and portal/credentials for your cluster first.

**Local (ZFS) end-to-end:**

```sh
nomad job run    examples/local-monolith.nomad.hcl   # plugin on every node
nomad volume create examples/local-volume.hcl        # provision the zvol
nomad job run    examples/local-consumer.nomad.hcl   # mount it, stay up
nomad alloc exec -job local-consumer df -h /data     # verify the mount
```

**QNAP (iSCSI SAN) end-to-end:**

```sh
nomad job run    examples/qnap-controller.nomad.hcl  # one controller (creds)
nomad job run    examples/qnap-node.nomad.hcl         # node plugin, every node
nomad volume create examples/qnap-volume.hcl          # provision the LUN
nomad job run    examples/qnap-consumer.nomad.hcl     # mount it, stay up
nomad alloc exec -job qnap-consumer df -h /data       # verify the mount
```

Tear down in reverse — stop the consumer first so the CSI claim releases, then
delete the volume:

```sh
nomad job stop -purge local-consumer && nomad volume delete local-data
nomad job stop -purge qnap-consumer  && nomad volume delete qnap-data
```

(For an automated multi-node end-to-end suite against a real cluster, see
[`localdev/`](localdev/) and `make test-integration`.)

## Startup readiness gate

Before it serves the CSI socket, the plugin **probes its backend** and refuses to
come up if the backing store is unusable — so Nomad never marks healthy a plugin
that can't actually provision/mount. The probe is the same `Probe` Nomad calls for
liveness:

- **local** — at least one allowlisted `zpool` is imported and **ONLINE** on this
  node (and `zpool`/`zfs` are runnable). A node legitimately holding only a subset
  of pools is fine; it fails only if ZFS is unreachable or none of its pools are
  usable.
- **qnap controller** — a live appliance session. (A `node`-only process has
  nothing remote to probe and is ready immediately.)

If the probe fails, the process **exits non-zero and Nomad reschedules it** rather
than serving a socket it can't back. Tune the retry window:

```hcl
readiness {
  timeout  = "20m"   # total wait before exiting non-zero; "0"/omitted = single attempt (fail fast)
  interval = "5s"    # delay between probes (default 5s)
}
```

`timeout = 0` (the default, or omitting the block) means **one attempt** — fail
fast and let Nomad's reschedule/backoff retry the whole alloc. A non-zero timeout
retries **in-process** (gentler than alloc churn) and is the right choice where the
store can take a while to appear — e.g. an integration node whose `zpool` is still
being created/formatted (the `localdev/` jobspecs default local to `20m`, qnap to
`5m`, overridable via the `readiness_timeout` job var / `*_READINESS_TIMEOUT` env).

## Observability (Prometheus metrics)

The plugin exposes Prometheus metrics from its **own** HTTP endpoint — it is **not**
wired into Nomad's `telemetry`. Prometheus scrapes the plugin process directly.

**Enable** with an explicit `enabled = true` in the plugin config:

```hcl
metrics {
  enabled = true      # required — an address alone leaves metrics OFF
  address = ":9501"   # optional, defaults to 0.0.0.0:9090; serves GET /metrics
  path    = "/metrics" # optional, defaults to /metrics
}
```

**Disable** (the default) by omitting the `metrics` block or leaving
`enabled = false`: no HTTP endpoint is started, so nothing is scrapeable. (Metrics
are still tallied in memory by the RPC interceptor — "off" means *not exposed*,
not *not collected*.) The `localdev/` jobspecs render this block from
`metrics_enabled` / `metrics_address` job variables; the `examples/` jobspecs set
it inline. Default ports are
chosen so all three plugin processes can co-exist on one host-networked node
without collision: **qnap controller `:9501`, qnap node `:9502`, local monolith
`:9503`** (all path `/metrics`).

**Scrape:** `GET http://<plugin-host>:<port>/metrics`. With `network_mode = host`
the endpoint binds the alloc's node. Each plugin **process** has its own endpoint,
so scrape **every** controller and node alloc. Co-located processes must use
**different ports** (host networking shares the port space) — hence qnap controller
`:9501`, qnap node `:9502`, local monolith `:9503`.

**Scrape labels (deployment identity):** `plugin_id`, `role` (controller/node),
and `nomad_node` are **not** encoded in metric names — each process exposes only
its own series, so attach identity at **scrape time** via relabeling (Nomad
service-discovery / `__meta_nomad_*` target labels, or Consul SD tags if the
cluster runs Consul). This keeps metrics driver-generic and lets multiple
deployments of the same driver (e.g. `qnap-sanA` / `qnap-sanB`) be distinguished
by label with no code change.

**Metrics exposed:**

| Metric | Labels | Where | Meaning |
| --- | --- | --- | --- |
| `nomad_csi_rpc_total` (counter) | `driver,method,code` | controller + node | CSI RPCs by method and gRPC code |
| `nomad_csi_rpc_duration_seconds` (histogram) | `driver,method,code` | controller + node | CSI RPC latency |
| `nomad_csi_node_mount_total` (counter) | `op,outcome` | node | Mount-layer ops (format/mount/unmount/bind/resize) by outcome |
| `nomad_csi_node_mount_duration_seconds` (histogram) | `op` | node | Mount-layer op latency |
| `nomad_csi_node_format_skipped_total` (counter) | — | node | Existing-filesystem reuse (idempotent format skip) |
| `nomad_csi_node_staged_volumes` (gauge) | — | node | Volumes currently staged on this node |
| `nomad_csi_qnap_op_total` (counter) | `op,outcome` | qnap controller | go-qnap ops by categorized outcome (auth/busy/conflict/…) |
| `nomad_csi_qnap_op_duration_seconds` (histogram) | `op,success` | qnap controller | go-qnap operation latency/outcome |
| `nomad_csi_qnap_request_duration_seconds` (histogram) | `op,method` | qnap controller | QNAP HTTP request latency |
| `nomad_csi_qnap_iscsi_login_total` (counter) | `outcome` | qnap node | iSCSI portal logins (ok/fail) |
| `nomad_csi_qnap_node_stage_total` (counter) | `outcome` | qnap node | Node stage attach outcome (ok/degraded/failed) |
| `nomad_csi_qnap_iscsi_rescan_total` / `nomad_csi_qnap_device_wait_total` (counters) | `outcome` | qnap node | Expand rescan / device-appearance waits |
| `nomad_csi_local_zfs_op_total` (counter) | `op,outcome` | local | ZFS logical ops (create/clone/destroy/expand/snapshot/list) by outcome |
| `nomad_csi_local_zfs_op_duration_seconds` (histogram) | `op` | local | ZFS op latency |
| `nomad_csi_local_forward_total` (counter) | `method,outcome` | local | Controller→controller forwards (ok/error/unreachable) |
| `nomad_csi_local_peer_resolve_total` (counter) | `outcome` | local | Peer-roster resolutions |
| `nomad_csi_local_placement_total` (counter) | `mode,outcome` | local | Volume placements by mode (content/host/auto) |
| `nomad_csi_local_capacity_reject_total` (counter) | — | local | Creates refused for dropping below pool reserve |
| `nomad_csi_local_peers` (gauge) | — | local | Peer controllers discovered at last resolve |
| `nomad_csi_local_pool_*` (gauges) | `pool` | local | Pool online/free/avail bytes + zvol count (computed at scrape) |
| `go_*`, `process_*` | — | all | Go runtime + process collectors |

Every CSI RPC is instrumented via a shared gRPC interceptor, so both `--driver=qnap`
and `--driver=local` emit the `nomad_csi_rpc_*` families (label `driver=qnap|local`);
the `nomad_csi_qnap_*` families appear only on the qnap processes and the
`nomad_csi_local_*` families only on the local monolith. The `nomad_csi_node_*`
families are shared by both backends' node side. Pool gauges are computed lazily by
a scrape-time collector (no background poller, no extra load on the SAN).

The `make test-integration` suite scrapes the live endpoints and asserts the
gauges/counters move across a real create→mount→unmount→delete cycle (plus a
deliberately-rejected create for the RPC error path):
`TestIntegration_Observability_Local` covers the monolith (`:9503`), and
`TestIntegration_Observability_QNAP` covers both the qnap controller (`:9501`,
appliance ops) and node (`:9502`, iSCSI login / stage / staged-count) — the latter
skips cleanly when no qnap appliance is deployed.

> Not yet emitted (planned, lower priority): qnap controller cache LUN count / age
> and live iSCSI session count (qnap node).

## Development

```bash
make build              # -> bin/nomad-csi-driver (version-stamped)
make test               # unit tests (race), hermetic; integration excluded by build tag
make lint               # golangci-lint
make package            # build + push the container image via Packer (DOCKER_* creds)
make test-integration   # e2e against an EXTERNAL Nomad cluster (NOMAD_ADDR) — see localdev/
```

`make test-integration` does not manage a cluster: it targets an existing Nomad
cluster via `NOMAD_ADDR` (+ optional `NOMAD_TOKEN`, `NOMAD_SKIP_VERIFY`), deploys
the CSI job from [`localdev/`](localdev/), verifies, and cleans up. Docker-only
(privileged container for ZFS). Preflight bails immediately if `NOMAD_ADDR` is
unset. See [`localdev/README.md`](localdev/README.md) for cluster prerequisites
and env vars.

The `go-qnap` dependency uses a local `replace => ../go-qnap` during development;
release builds pin a real tag.

## Release

To cut a release, perform the following steps:
- Verify unit + integration tests pass, commit & push changes
- Export the new TAG version: `export TAG=0.0.1`                               # Set new TAG version
- Run a build: `make build`                                                    # Build the binary with version stamping
- Run `DOCKER_HOST=quay.io DOCKER_REPO=quay.io/honesthosting ... make package` # Push tagged image to Quay.io
- Run `DOCKER_HOST=harbor... DOCKER_REPO=... make package`                     # Push tagged image to Harbor
- Run `make release`                                                           # Create a new git-tag + release on GitHub

## License

MIT. See [LICENSE](LICENSE).
