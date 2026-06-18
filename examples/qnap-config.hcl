# Example deployment config for --driver=qnap (controller + node).
# Mount this file into the plugin task (e.g. via a Nomad template/secret) and
# pass it with --config. Secrets live here, never on argv.

qnap {
  base_url = "https://qnap.example.com"
  username = "csi"
  password = "REDACTED"
  insecure = false

  # iSCSI portals nodes log into — ONE PATH PER PORTAL (multipath). List two
  # (e.g. two NICs/subnets) for a redundant, multipathed LUN; the node assembles
  # them via dm-multipath. The controller advertises these to nodes in the volume
  # context. Port defaults to 3260. `portals` takes precedence over `portal`; a
  # single `portal = "10.0.10.5"` is the one-path equivalent.
  portals = ["10.0.10.5", "10.0.20.5"]

  # Pool used when a volume omits parameters.pool.
  default_pool_id = 1

  # Network interfaces new 1:1 targets are bound to (required for controller).
  # For multipath, bind one interface per portal subnet (e.g. ["eth0", "eth1"]).
  interfaces = ["eth0"]

  # default_sector_size = 512   # 512 default; 4096 is Windows-only, unsupported
  # disable_multipath   = false # true = raw single device, skip dm-multipath
  # node_state_dir      = "/var/lib/nomad-csi-driver/qnap"
  # debug_http          = false # log raw QNAP requests/responses (verbose; needs --log-level=debug)

  # Per-volume stats fan-out (optional). qnap volumes carry no owning-node in
  # their ID, so the controller pulls each node's readings over a small
  # forwarding transport and aggregates them. Set the SAME forward_secret on the
  # node config; nodes listen on forward_addr, the controller dials it.
  # Requires an `identity` block on the CONTROLLER task (Nomad /v1/nodes
  # discovery over api.sock) — see qnap-controller.nomad.hcl. Omit forward_secret
  # to leave central stats off (nodes still hydrate locally).
  forward_secret = "REDACTED-shared-secret" # MUST match the node config
  forward_addr   = ":9612"                  # cluster-uniform; :9612 ≠ local's :9602
  # nomad {}  # discovery tuning; defaults to api.sock + $NOMAD_DC (see local-config.hcl)
}

metrics {
  enabled = true
  address = ":9501"
}

# Per-volume usage stats. On the CONTROLLER this serves the aggregated query API
# + nomad_csi_volume_* gauges; :9611 keeps it off the local monolith's :9610 when
# co-located. (On a NODE config, this block only tunes local hydration cadences.)
stats {
  query_addr = ":9611"
  # query_token        = ""     # empty/unset = OPEN; set to require X-NCD-Query-Token
  # aggregate_interval = "60s"  # how often the controller fans out to nodes
  # interval           = "60s"  # statfs cadence
  # walk_interval      = "5m"   # file/dir walk cadence
  # metrics_per_volume = true   # per-volume gauges; false = aggregate-only
}
