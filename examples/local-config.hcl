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

  # Peer discovery. By default the driver discovers peers via Nomad's own
  # /v1/nodes API over the task API socket (api.sock) — no external service.
  # This requires an `identity` block on the plugin task (see the jobspec) and,
  # with ACLs enabled, a node:read policy bound to it. It covers single- AND
  # multi-node clusters; for a lone node every op resolves to self.
  #
  # (a) Nomad discovery (default). The block is OPTIONAL — omit it to use all
  #     defaults (api.sock + nomad_token from NOMAD_SECRETS_DIR, scoped to
  #     $NOMAD_DC, 5m cache). Shown here with the defaults made explicit:
  nomad {
    # socket_path = "${NOMAD_SECRETS_DIR}/api.sock"  # default
    # token_path  = "${NOMAD_SECRETS_DIR}/nomad_token" # default (re-read for rotation)
    # datacenter  = "kitchen"   # default $NOMAD_DC
    # node_filter = "NodeClass == \"storage\""  # only if the plugin job is constrained
    cache_ttl = "5m"
  }
  #
  # (b) A static peer table — the opt-in override (hard-coded addresses, or
  #     running outside Nomad). When present it takes precedence; ship the same
  #     table to every node:
  #
  #   peer "node1" { addr = "10.0.0.1:9602" }
  #   peer "node2" { addr = "10.0.0.2:9602" }
}

metrics {
  enabled = true
  address = ":9503"
}
