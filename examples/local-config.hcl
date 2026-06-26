# Example deployment config for --driver=local (monolith on every node).

local {
  # Allowlist of usable zpools (by NAME — the driver never creates them; Management
  # is up to the operator). A volume picks one via parameters.pool; default_pool
  # is used when omitted and MUST be one of the pools below.
  default_pool = "tank"

  pool "tank" {
    parent_dataset = "csi"
  }
  # A second (e.g. NVMe) tier; per-pool reserve override is optional:
  pool "nvme" {
    parent_dataset  = "csi"
    reserve_percent = 15
  }

  default_volblocksize = "16K"
  reserve_percent      = 10 # global default free-space floor (per-pool can override)

  # Address this node's forwarding server listens on; peers reach it here.
  forward_addr   = ":9602"
  forward_secret = "REDACTED-shared-secret"

  # Peer discovery is via Nomad's own /v1/nodes API over the task API socket
  # (api.sock) — no external service, and the ONLY mechanism (no static peer
  # table). It requires an `identity` block on the plugin task (see the jobspec)
  # and, with ACLs enabled, a `node:read` policy (plus `csi-list-volume` and
  # `csi-read-volume` for the stats query API's volume lookups). It covers single- AND multi-node clusters; for a lone node
  # every op resolves to self. The block is OPTIONAL — omit it for all defaults
  # (api.sock + nomad_token from NOMAD_SECRETS_DIR, scoped to $NOMAD_DC, 5m
  # cache). Shown here with the defaults made explicit:
  nomad {
    # socket_path = "${NOMAD_SECRETS_DIR}/api.sock"  # default
    # token_path  = "${NOMAD_SECRETS_DIR}/nomad_token" # default (re-read for rotation)
    # datacenter  = "kitchen"   # default $NOMAD_DC
    # node_filter = "NodeClass == \"storage\""  # only if the plugin job is constrained
    cache_ttl = "5m"
  }
}

metrics {
  enabled = true
  address = ":9503"
}

# Per-volume usage stats. The driver measures each staged volume's bytes/inodes
# (statfs) and file/dir/other counts (a background tree walk), serving them on an
# HTTP+JSON query API (GET /v1/volume-stats[/{nomad-volume-id}]) and as
# nomad_csi_volume_* Prometheus gauges. ON by default — this whole block is
# optional; the production defaults shown as comments apply when omitted.
stats {
  # enabled            = true    # master toggle (default on)
  # query_addr         = ":9610" # query API listener; "" disables it
  # query_token        = ""      # require this token (header X-NCD-Query-Token);
  #                              # EMPTY/unset leaves the endpoint OPEN (no auth)
  # query_token_header = "X-NCD-Query-Token"
  # interval           = "60s"   # statfs cadence (cheap)
  # walk_interval      = "5m"    # file/dir walk cadence (expensive)
  # walk_workers       = 4       # shared walk pool size (the IO ceiling)
  # walk_timeout       = "10m"   # per-volume walk deadline
  # metrics_per_volume = true    # per-volume gauges; false = aggregate-only
}
