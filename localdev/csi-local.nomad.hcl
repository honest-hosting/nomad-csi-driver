# Local-backend CSI plugin (monolith) for the --driver=local backend, as a
# PRIVILEGED docker system job. Privileged is required so the cgroup device
# controller permits /dev/zfs (a non-privileged/raw_exec task is denied with
# EPERM). Peer discovery is via Nomad's /v1/nodes API over the task API socket
# (api.sock); the identity block below surfaces the workload-identity token.
#
# Register against an external cluster with `nomad job run -var ...` (the e2e
# suite does this for you). The image must carry a ZFS userland matching each
# node's kernel module (see ../Dockerfile; debian:12 -> zfs 2.1.x).
#
# Variables let the e2e harness isolate itself from any production plugin
# (distinct plugin_id, dataset, etc.).

variable "image" {
  type        = string
  description = "Container image for the plugin (must be pullable by the cluster)."
}

variable "plugin_id" {
  type    = string
  default = "nomad-csi-driver-local"
}

variable "pool" {
  type    = string
  default = "tank1"
}

variable "pool2" {
  type        = string
  default     = "tank2"
  description = "Second zpool added to the allowlist. The integration cluster has tank1 + tank2; set to \"\" for a single-pool deploy. Must pre-exist on the node(s) that serve it."
}

variable "forward_secret" {
  type    = string
  default = "e2e-secret"
}

variable "discovery_cache_ttl" {
  type        = string
  default     = "30s"
  description = "How long the peer roster from Nomad's /v1/nodes is cached before refresh. Short here for e2e responsiveness; production defaults to 5m."
}

variable "metrics_enabled" {
  type        = bool
  default     = true
  description = "Expose the plugin's own Prometheus /metrics endpoint. The e2e observability suite scrapes it."
}

variable "metrics_address" {
  type        = string
  default     = "0.0.0.0:9503"
  description = "host:port the /metrics endpoint binds (host networking). The monolith carries both controller + node series on this one port. 9503 avoids the qnap controller (9501) / node (9502) ports when those plugins are co-located on the same node."
}

variable "readiness_timeout" {
  type        = string
  default     = "20m"
  description = "How long the plugin retries the startup readiness probe (a usable zpool ONLINE) before exiting non-zero so Nomad reschedules. Generous here because ZFS install/zpool create+format on a fresh integration node can take a while. \"0\" = single attempt (fail fast)."
}

job "nomad-csi-driver-local" {
  type = "system"

  group "plugin" {
    task "plugin" {
      driver = "docker"

      # Peer discovery reads Nomad's /v1/nodes over the task API socket; this
      # surfaces the workload-identity token (file is re-read for rotation, env
      # is the fallback). It reads /v1/nodes (discovery) + /v1/volumes (stats id
      # resolution). With Nomad ACLs ENABLED, bind node:read + csi-read-volume
      # (ACLs off => no policy needed):
      #   cat > ncd.policy.hcl <<'EOF'
      #   node { policy = "read" }
      #   namespace "default" { capabilities = ["csi-read-volume"] }
      #   EOF
      #   nomad acl policy apply -namespace default -job nomad-csi-driver-local \
      #     -group plugin -task plugin csi-local-ncd ncd.policy.hcl
      identity {
        env  = true
        file = true
      }

      config {
        image        = var.image
        force_pull   = true
        privileged   = true
        network_mode = "host"
        args = [
          "run",
          "--driver=local",
          "--mode=monolith",
          "--node-id=${node.unique.name}",
          "--parent-dataset=${var.plugin_id}", # ZFS parent dataset: <pool>/${var.plugin_id}/<vol-id> (namespace per deployment)
          "--config=/local/config.hcl",
          "--log-level=debug",
        ]
        # No explicit /dev mount: privileged already bind-mounts the host /dev
        # (incl. /dev/zfs + dynamically-created /dev/zvol/*); adding it would be a
        # "Duplicate mount point: /dev" error.
      }

      # The config is uniform across nodes (each node has its own pool); the
      # only per-node value, the node id, is passed via args. Job `var.*` is
      # substituted at `nomad job run` parse time, so it is safe inside
      # template.data (unlike Nomad's runtime ${node.*}/${meta.*}).
      template {
        destination = "local/config.hcl"
        data        = <<EOH
local {
  # parent_dataset is omitted -> the driver defaults it to "nomad-csi", so the
  # driver provisions zvols at <pool>/nomad-csi/<id> (and auto-creates that
  # parent via `zfs create -p`). Set parent_dataset inside a pool block ONLY to
  # override that namespace.
  default_pool = "${var.pool}"
  pool "${var.pool}" {}
%{ if var.pool2 != "" ~}
  pool "${var.pool2}" {}
%{ endif ~}
  default_volblocksize = "16K"
  forward_addr         = ":9602"
  forward_secret       = "${var.forward_secret}"
  # Peer discovery via Nomad's /v1/nodes over api.sock (defaults: api.sock +
  # nomad_token from NOMAD_SECRETS_DIR, scoped to $NOMAD_DC). Only cache_ttl is
  # overridden here, for test responsiveness.
  nomad {
    cache_ttl = "${var.discovery_cache_ttl}"
  }
}
%{ if var.metrics_enabled ~}
metrics {
  enabled = true
  address = "${var.metrics_address}"
}
%{ endif ~}
readiness {
  timeout = "${var.readiness_timeout}"
}
# Per-volume usage stats. Defaults are production cadences (60s statfs / 5m walk);
# they are shortened here so the e2e suite observes a walk within the test window.
# query_addr defaults to :9610 (the klm API / test scrapes it).
stats {
  interval      = "5s"
  walk_interval = "10s"
  walk_timeout  = "2m"
  stale_after   = "30s"
}
EOH
      }

      csi_plugin {
        id        = var.plugin_id
        type      = "monolith"
        mount_dir = "/csi"
      }

      resources {
        cpu    = 200
        memory = 256
      }
    }
  }
}
