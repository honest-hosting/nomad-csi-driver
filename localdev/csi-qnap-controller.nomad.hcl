# QNAP CSI controller — a count-1 SERVICE job (the sole talker to the appliance,
# holds creds). Pairs with csi-qnap-node.nomad.hcl under the same plugin_id;
# Nomad ties the controller + node daemonset into one CSI plugin.
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

job "nomad-csi-driver-qnap-controller" {
  type = "service"

  group "controller" {
    count = 1

    task "plugin" {
      driver = "docker"

      config {
        image        = var.image
        force_pull   = true
        network_mode = "host"
        args = [
          "run",
          "--driver=qnap",
          "--mode=controller",
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
