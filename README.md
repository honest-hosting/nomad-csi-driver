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

## Architecture: control plane only (zero-disruption upgrades)

The plugin is a **control-plane shim**, never a data-plane component. It *issues*
storage operations — provision, attach, `mount`, `unmount` — and then steps out of
the I/O path entirely. The bytes a running workload reads and writes never traverse
the plugin process. This is a deliberate choice, and it's what lets you **upgrade,
restart, or reschedule the plugin with running workloads untouched** — no restarts,
no remounts, no I/O interruption.

The reason is *where the data plane lives*: the **host kernel** (and host daemons),
not the plugin container.

- **local (ZFS)** — `NodeStage`/`Publish` is a kernel mount of
  `/dev/zvol/<pool>/<vol>` plus a `mount --bind` into the alloc dir. Live I/O is
  workload → kernel VFS → ZFS → disk.
- **qnap (iSCSI + multipath)** — the plugin drives the **host's** `iscsid` (login →
  `/dev/sdX`) and the **host's** `multipathd` (assembles `/dev/mapper/<wwid>`), then
  kernel-mounts the result. Live I/O is workload → kernel FS → dm-multipath →
  in-kernel iSCSI initiator → SAN. (This is why the node task bind-mounts
  `/etc/iscsi`, `/etc/multipath`, and `/run/lock` from the host — it configures the
  host's daemons; it does **not** run its own.)

Kill the plugin container in either mode and the iSCSI sessions, multipath maps, and
mounts all persist on the host. Two mechanisms make this hold:

1. **Shared mount propagation** — Nomad's `csi_plugin` stanza bind-mounts the
   plugin's `mount_dir` with bidirectional propagation, so mounts the plugin makes
   propagate **out** to the host namespace and **into** the workload's. They aren't
   owned by the plugin container's namespace and don't die with it. (`privileged =
   true` is what permits this.)
2. **Decoupled allocations** — Nomad never restarts a *consuming* workload because
   the *plugin* job changed; they're separate allocs and the consumer's volume claim
   survives a plugin restart. (True of any CSI plugin on Nomad — so a workload
   restart on plugin update is always a *data-plane-in-the-plugin* symptom, not
   something the orchestrator imposes.)

**What this avoids:** the disruption inherent to drivers whose data plane lives *in*
the plugin/daemon process — e.g. hyperconverged stores like Portworx (the `px`
daemon *is* the backend), or NFS/FUSE drivers that mount inside the container's
private namespace or proxy I/O through an in-pod server. For those, a plugin restart
yields stale handles ("transport endpoint not connected") and **forces every
consuming workload to restart**. Here, a rolling image bump is a non-event for
running workloads — which is exactly why the storage jobspecs can roll one node at a
time and soak on health without ever draining workloads.

**Boundaries** (all seconds-scale; none restart a steady workload):

- While the plugin is down you cannot perform **new** stage/publish/unpublish on that
  node — existing mounts are unaffected, but fresh attach/detach waits for the plugin.
- A workload that **independently** restarts during the plugin-down window blocks on
  re-publish until the plugin returns, then proceeds.
- A **host reboot** is different in kind: it tears down the in-kernel sessions and
  mounts, and everything re-stages on boot. The survival property is about *plugin*
  restarts, not *host* restarts.

## Usage

```bash
nomad-csi-driver run \
  --mode            {controller|node|monolith} \
  --driver          {qnap|local} \
  --endpoint        unix:///csi/csi.sock     # or $CSI_ENDPOINT
  --node-id         ${node.unique.name}      # required (all modes); or $CSI_NODE_ID
  --plugin-id       ${var.plugin_id}         # required; or $CSI_PLUGIN_ID (matches csi_plugin.id)
  --parent-dataset  nomad-csi                # --driver=local only; ZFS parent dataset (see below)
  --config          /local/config.hcl
```

`--node-id` and `--plugin-id` are **required in every mode** (controller included)
and become metric labels (see Observability). `--plugin-id` should match the Nomad
`csi_plugin { id = … }` for this deployment; `--node-id` is conventionally
`${node.unique.name}`.

- `qnap`: a process is `controller` (one per cluster/DC, sole talker to the
  appliance) **xor** `node` (every client; iSCSI + multipath + format/mount).
  `--parent-dataset` is **not used** by qnap (LUNs are named per-volume and
  namespaced on the appliance).
- `local`: `monolith` on every node under one `plugin_id`; controllers forward
  create/delete/expand/snapshot to the owning node (peer discovery via Nomad's
  `/v1/nodes` API over the task API socket — the plugin task needs an `identity`
  block, plus, when ACLs are enabled, `node:read` and — for the stats query API's
  Nomad-id resolution, which **lists** `/v1/volumes` — `csi-list-volume`
  (and `csi-read-volume`) in the queried namespace).

**Workload-identity discovery is mandatory** — there is no static peer table.
A deployment without an `identity` block / reachable `api.sock` fails fast at
startup so Nomad reschedules it.

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
curl -s localhost:9610/v1/volume-stats/local-data | jq  # per-volume usage by Nomad id
```

**QNAP (iSCSI SAN) end-to-end:**

```sh
nomad job run    examples/qnap-controller.nomad.hcl  # one controller (creds)
nomad job run    examples/qnap-node.nomad.hcl         # node plugin, every node
nomad volume create examples/qnap-volume.hcl          # provision the LUN
nomad job run    examples/qnap-consumer.nomad.hcl     # mount it, stay up
nomad alloc exec -job qnap-consumer df -h /data       # verify the mount
curl -s <controller-host>:9611/v1/volume-stats/qnap-data | jq  # per-volume usage by Nomad id
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

**Deployment-identity labels (baked in).** Every series — including `go_*` /
`process_*` — carries four constant labels identifying the emitting deployment:
`driver` (the `--driver` value), `mode` (`--mode`: controller/node/monolith),
`node_id` (`--node-id`), and `plugin_id` (`--plugin-id`). A
`nomad_csi_build_info{version,commit,build_date} 1` gauge carries the build stamp
alongside those constants, so a bare scrape (or `curl`) is self-describing and
identifies which process/listener served it — no relabeling required. (This
intentionally reverses an earlier "identity as scrape-relabel only" posture:
identity is intrinsic to the process, so it is self-reported. Multiple deployments
of the same driver — e.g. `qnap-sanA` / `qnap-sanB` — are still distinguished, now
by their `plugin_id`.) `nomad_node` and any extra SD metadata can still be attached
at scrape time via relabeling.

**Metrics exposed:**

> All metrics below also carry the constant `driver`, `mode`, `node_id`, and
> `plugin_id` labels described above; only each metric's **own** labels are listed.

| Metric | Labels | Where | Meaning |
| --- | --- | --- | --- |
| `nomad_csi_build_info` (gauge) | `version,commit,build_date` | all | Build/deployment identity; always 1 (carries the constant identity labels) |
| `nomad_csi_rpc_total` (counter) | `method,code` | controller + node | CSI RPCs by method and gRPC code |
| `nomad_csi_rpc_duration_seconds` (histogram) | `method,code` | controller + node | CSI RPC latency |
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
| `nomad_csi_local_placement_total` (counter) | `strategy,outcome` | local | Volume placements by strategy (content/host/auto) |
| `nomad_csi_local_capacity_reject_total` (counter) | — | local | Creates refused for dropping below pool reserve |
| `nomad_csi_local_pool_*` (gauges) | `pool` | local | Pool online/free/avail bytes + zvol count (computed at scrape) |
| `nomad_csi_cluster_forward_total` (counter) | `method,outcome` | controller (local + qnap) | Forwards by method (ok/error/unreachable): local controller→controller, qnap controller→node fan-out |
| `nomad_csi_cluster_resolve_total` (counter) | `outcome` | controller (local + qnap) | Peer/node roster resolutions |
| `nomad_csi_cluster_peers` (gauge) | — | controller (local + qnap) | Cluster members discovered at last resolve (local: peer controllers; qnap: node daemons) |
| `go_*`, `process_*` | — | all | Go runtime + process collectors |

Every CSI RPC is instrumented via a shared gRPC interceptor, so both `--driver=qnap`
and `--driver=local` emit the `nomad_csi_rpc_*` families (the backend is the constant
`driver` label, not part of each series' own labels). The `nomad_csi_qnap_*` families
appear only on the qnap processes and the `nomad_csi_local_*` families only on the
local monolith. The `nomad_csi_node_*` families are shared by both backends' node
side, and the `nomad_csi_cluster_*` forwarding families are shared by both backends'
controller role. Pool gauges are computed lazily by a scrape-time collector (no
background poller, no extra load on the SAN).

The `make test-integration` suite scrapes the live endpoints and asserts the
gauges/counters move across a real create→mount→unmount→delete cycle (plus a
deliberately-rejected create for the RPC error path):
`TestIntegration_Observability_Local` covers the monolith (`:9503`), and
`TestIntegration_Observability_QNAP` covers both the qnap controller (`:9501`,
appliance ops) and node (`:9502`, iSCSI login / stage / staged-count) — the latter
skips cleanly when no qnap appliance is deployed.

> Not yet emitted (planned, lower priority): qnap controller cache LUN count / age
> and live iSCSI session count (qnap node).

## Per-volume usage stats

A backend-agnostic subsystem reports **per-volume** filesystem usage — total /
used / free **bytes**, **inode** counts, and **file / directory / other** object
counts — for every volume a node currently has staged. It works the same for
both `--driver=local` and `--driver=qnap`, is **on by default**, and is exposed
two ways: a synchronous **HTTP+JSON query API** and **Prometheus gauges**.

**How it works.** Each node runs one lightweight background worker per staged
volume: a cheap `statfs` on a fast cadence (bytes + inodes) and a concurrent
directory walk on a slower cadence (file/dir/other counts). Readings are cached
in memory. The **controller** answers queries and emits `/metrics` from that
data:

- **local** — the owning node is embedded in the volume ID, so a query forwards
  to that node over the existing `:9602` forwarding transport. Each monolith
  exposes its own node's volumes on `/metrics`.
- **qnap** — the controller periodically **fans out** to all node daemons (same
  forwarding transport), aggregates, and serves queries + a single central
  `/metrics`. This requires `qnap.forward_secret` on the controller and node
  (and a cluster-uniform node `forward_addr` — driver default `:9602`, which the
  examples set to `:9612` so it doesn't clash with a co-located local monolith's
  `:9602`); without `forward_secret`, qnap volumes still hydrate node-locally but
  are not centrally queryable.

**Resilience.** The subsystem degrades to *stale data* and never blocks the CSI
RPC path: queries serve cached values, a hung `statfs`/`readdir` is abandoned by
a watchdog (single-flight bounds leaked goroutines), repeated failures back off
and self-heal, and staleness is observable via `*_age_seconds` / `stale` gauges.

**Config** (`stats {}`; all fields optional, shown with defaults):

```hcl
stats {
  enabled               = true               # master toggle (default ON)
  interval              = "60s"              # statfs cadence
  statfs_timeout        = "30s"              # hung-mount watchdog
  walk_enabled          = true               # file/dir counting (default ON)
  walk_interval         = "5m"               # tree-walk cadence
  walk_workers          = 4                  # shared walk pool size (the IO ceiling)
  walk_buffer           = 4096               # shared pending-directory backlog
  walk_timeout          = "10m"              # per-volume walk deadline
  stale_after           = "5m"               # reading considered stale beyond this
  max_failure_backoff   = "30m"              # backoff cap after repeated errors

  metrics_per_volume    = true               # per-volume gauges (vs aggregate-only)
  query_addr            = ":9610"            # query API listener ("" disables)
  query_token           = ""                 # bearer token; "" / unset = OPEN (no auth)
  query_token_header    = "X-NCD-Query-Token"

  aggregate_interval    = "60s"              # qnap only: controller fan-out cadence
}
```

**Query API.** Served by the **controller role** on `query_addr` (local monolith
default `:9610`; the qnap controller uses `:9611` in the examples). All endpoints
are **`GET`** and return `application/json`.

| Method & path | Success | Other statuses |
| --- | --- | --- |
| `GET /v1/volume-stats[?namespace=default]` | `200` — JSON array | `401` (bad/missing token), `502` (Nomad/upstream error) |
| `GET /v1/volume-stats/{id}[?namespace=default]` | `200` — one record | `404` (id unknown to Nomad), `412` (known but **not mounted** on any node — created-but-unmounted, or workload stopped; body explains), `503` (mounted, not measured yet — partial record with a zero `statfs_at`), `401`, `502` |

> **`{id}` is the Nomad volume id you manage** (e.g. `local-data` — what you set
> in the volume spec and see in `nomad volume status`). The driver's internal
> external id is never exposed: the controller resolves the Nomad id → external id
> via Nomad's API (cached). `namespace` defaults to `default` (no wildcard /
> multi-namespace in v1).
>
> ```sh
> curl -s localhost:9610/v1/volume-stats/local-data | jq
> ```
>
> For **local**, a by-id lookup may hit *any* monolith — it resolves the id then
> forwards to the owning node. The **list** endpoint returns only the queried
> node's volumes (scrape every node, or rely on the aggregated metrics). For
> **qnap**, both endpoints serve the controller's cluster-wide aggregate.

**Response body:** byte/inode fields come from `statfs`; `*_count` from the tree
walk. Timestamps are RFC3339; `walk_duration` is **nanoseconds** (Go
`time.Duration`). On a `503` (not yet measured) the numeric fields are zero and
`statfs_at`/`walk_at` are the zero time.

```jsonc
{
  "id":              "local-data",   // the Nomad volume id (no external id exposed)
  "namespace":       "default",
  "node":            "node1",        // owning/mounting node
  "access_type":     "mount",        // "mount" | "block" (block omits fs/inode/walk data)
  "total_bytes":     1063256064,
  "used_bytes":      2125824,
  "available_bytes": 1007550464,
  "total_inodes":    65536,
  "used_inodes":     11,
  "free_inodes":     65525,
  "statfs_at":       "2026-06-18T17:04:12Z",  // last successful statfs
  "file_count":      0,
  "dir_count":       1,              // a fresh ext4 fs has lost+found
  "other_count":     0,             // symlinks/sockets/devices/pipes
  "walk_at":         "2026-06-18T17:04:15Z",  // last completed walk
  "walk_duration":   3500000,       // ns
  "walk_complete":   true,          // false until the first full walk
  "last_error":      ""             // most recent statfs/walk error ("" = healthy); omitted when empty
}
```

**Auth** is an opt-in bearer token: set `query_token` and clients must send it in
`query_token_header` (default `X-NCD-Query-Token`), e.g.
`curl -H "X-NCD-Query-Token: $TOKEN" …`. **An empty/unset token leaves the
endpoint OPEN** to anyone who can reach `query_addr` — an explicit, supported
choice. The token is distinct from the forwarding `forward_secret` (different
trust domain). Like the forwarding transport, traffic is cleartext on a trusted
L2 (mTLS deferred).

**Metrics** (controller role; `metrics_per_volume = false` collapses to the
aggregate gauges):

| Metric | Labels | Meaning |
| --- | --- | --- |
| `nomad_csi_volume_total_bytes` / `_used_bytes` / `_available_bytes` (gauges) | `id,namespace,node` | Filesystem size / used / available |
| `nomad_csi_volume_inodes_total` / `_inodes_used` / `_inodes_free` (gauges) | `id,namespace,node` | Inode usage |
| `nomad_csi_volume_files` / `_dirs` / `_other` (gauges) | `id,namespace,node` | Object counts from the tree walk |
| `nomad_csi_volume_statfs_age_seconds` / `_walk_age_seconds` / `_walk_duration_seconds` (gauges) | `id,namespace,node` | Freshness + last walk duration |
| `nomad_csi_volume_stale` (gauge) | `id,namespace,node` | 1 if older than `stale_after` |
| `nomad_csi_volume_count` / `_used_bytes_sum` / `_total_bytes_sum` (gauges) | — | Aggregate (emitted when `metrics_per_volume = false`) |

> Per-volume labels are a deliberate, documented exception to the driver's
> otherwise no-per-volume-cardinality rule (bounded by volume count). Set
> `metrics_per_volume = false` if cardinality is a concern.

## Development

```bash
make build              # build + lint, creates 'bin/nomad-csi-driver' (version-stamped)
make test               # unit tests (race), hermetic; integration excluded by build tag
make package            # build + push the container image via Packer (DOCKER_* creds) as 'latest' tag
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
