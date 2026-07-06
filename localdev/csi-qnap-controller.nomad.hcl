# QNAP CSI controller — a count-1 SERVICE job (provisions LUNs on the appliance
# read-write, holds creds; the node also reads the appliance, read-only). Pairs
# with csi-qnap-node.nomad.hcl under the same plugin_id; Nomad ties the controller
# + node daemonset into one CSI plugin.
#
# No privileged/devices: the controller only speaks HTTP to the QNAP API.
# host networking so it can reach the appliance. Secrets via -var for the e2e
# harness (they land in the job spec) — production should use Vault / nomad vars.

variable "image" {
  type = string
}

variable "plugin_id" {
  type    = string
  default = "nomad-csi-driver-qnap"
}

variable "base_url" {
  type = string
}

variable "username" {
  type = string
}

variable "password" {
  type = string
}

variable "insecure" {
  type    = bool
  default = true
}

variable "portal" {
  type    = string
  default = ""
}

variable "portals" {
  type        = string
  default     = ""
  description = "Comma-separated iSCSI portals for multipath (one path each), e.g. \"10.0.10.5,10.0.20.5\". Takes precedence over `portal`."
}

variable "pool_id" {
  type = number
}

variable "interfaces" {
  type        = string
  description = "Comma-separated iSCSI portal interfaces, e.g. \"eth0\" or \"bond0,bond1\"."
}

variable "debug_http" {
  type        = bool
  default     = false
  description = "Log every raw QNAP request path + response body at debug level (in-driver `qnapctl --debug-http`). Verbose; troubleshooting only."
}

variable "metrics_enabled" {
  type        = bool
  default     = true
  description = "Expose the controller's own Prometheus /metrics endpoint. The e2e observability suite scrapes it."
}

variable "metrics_address" {
  type        = string
  default     = "0.0.0.0:9501"
  description = "host:port the controller's /metrics endpoint binds (host networking). Distinct from the node's port so a co-located controller+node don't collide."
}

variable "readiness_timeout" {
  type        = string
  default     = "5m"
  description = "How long the controller retries the startup readiness probe (a live QNAP appliance session) before exiting non-zero so Nomad reschedules. \"0\" = single attempt (fail fast)."
}

variable "forward_secret" {
  type        = string
  default     = "e2e-secret-qnap"
  description = "Shared secret for the per-volume stats forwarding transport. Must match the qnap node job. Enables the controller's stats fan-out + query API."
}

variable "discovery_cache_ttl" {
  type        = string
  default     = "30s"
  description = "How long the node roster from Nomad's /v1/nodes is cached before refresh (controller stats fan-out). Short here for e2e responsiveness."
}

job "nomad-csi-driver-qnap-controller" {
  type = "service"

  group "controller" {
    count = 1

    task "plugin" {
      driver = "docker"

      # Peer discovery reads Nomad's /v1/nodes over the task API socket; this
      # surfaces the workload-identity token (file is re-read for rotation, env
      # is the fallback). It reads /v1/nodes (node fan-out) + /v1/volumes (stats id
      # resolution). With Nomad ACLs ENABLED, bind node:read + csi-list-volume + csi-read-volume
      # (ACLs off => no policy needed):
      #   cat > ncd.policy.hcl <<'EOF'
      #   node { policy = "read" }
      #   namespace "default" { capabilities = ["csi-list-volume", "csi-read-volume"] }
      #   EOF
      #   nomad acl policy apply -namespace default -job nomad-csi-driver-qnap-controller \
      #     -group controller -task plugin csi-qnap-ncd ncd.policy.hcl
      identity {
        env  = true
        file = true
      }

      config {
        image        = var.image
        force_pull   = true
        network_mode = "host"
        args = [
          "run",
          "--driver=qnap",
          "--mode=controller",
          "--node-id=${node.unique.name}",
          "--plugin-id=${var.plugin_id}",
          "--config=/local/config.hcl",
          "--log-level=debug",
        ]
      }

      template {
        destination = "local/config.hcl"
        data        = <<EOH
qnap {
  base_url        = "${var.base_url}"
  username        = "${var.username}"
  password        = "${var.password}"
  insecure        = ${var.insecure}
  portal          = "${var.portal}"
  portals         = ${jsonencode([for p in split(",", var.portals) : trimspace(p) if trimspace(p) != ""])}
  default_pool_id = ${var.pool_id}
  interfaces      = ${jsonencode(split(",", var.interfaces))}
  debug_http      = ${var.debug_http}
  # Per-volume stats fan-out: the controller pulls each node's readings over the
  # forwarding transport. forward_addr is the cluster-uniform port nodes listen on
  # (:9612); the controller dials it, it does not bind it. Node discovery uses
  # Nomad's /v1/nodes over api.sock (same as the local backend).
  forward_secret = "${var.forward_secret}"
  forward_addr   = ":9612"
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
# Per-volume usage stats. query_addr :9611 avoids the local monolith's :9610 on a
# co-located node; cadences shortened for the e2e suite.
stats {
  query_addr         = ":9611"
  aggregate_interval = "5s"
  interval           = "5s"
  walk_interval      = "10s"
  walk_timeout       = "2m"
  stale_after        = "30s"
}
EOH
      }

      csi_plugin {
        id        = var.plugin_id
        type      = "controller"
        mount_dir = "/csi"
      }

      resources {
        cpu    = 200
        memory = 256
      }
    }
  }
}
