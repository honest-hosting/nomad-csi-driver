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
}

metrics {
  enabled = true
  address = ":9501"
}
