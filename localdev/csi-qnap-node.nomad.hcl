# QNAP CSI node — a SYSTEM job (daemonset; runs on every client). Performs iSCSI
# login + multipath assembly + format/mount. Pairs with
# csi-qnap-controller.nomad.hcl under the same plugin_id.
#
# Privileged + /dev so the cgroup device controller permits the iSCSI/multipath
# (dm) devices; host networking to reach the iSCSI portal. The node reads the
# portal/IQN from the CSI volume context, BUT it now also needs READ-ONLY SAN
# credentials: it resolves iSCSI sessions to volume identities (for per-volume
# stats rehydration across restarts + cold-cache teardown). The plugin refuses to
# start in node mode without base_url/username/password. NOTE: QNAP has no
# read-only API account, so these are the same credentials the controller uses —
# scope their exposure accordingly.

variable "image" {
  type = string
}

variable "plugin_id" {
  type    = string
  default = "nomad-csi-driver-qnap"
}

variable "base_url" {
  type        = string
  description = "QNAP appliance base URL (read-only SAN access for session→volume resolution)."
}

variable "username" {
  type = string
}

variable "password" {
  type = string
}

variable "insecure" {
  type    = bool
  default = false
}

variable "metrics_enabled" {
  type        = bool
  default     = true
  description = "Expose the node's own Prometheus /metrics endpoint. The e2e observability suite scrapes it."
}

variable "metrics_address" {
  type        = string
  default     = "0.0.0.0:9502"
  description = "host:port the node's /metrics endpoint binds (host networking). Distinct from the controller's :9501 so a co-located controller+node don't collide."
}

variable "forward_secret" {
  type        = string
  default     = "e2e-secret-qnap"
  description = "Shared secret for the per-volume stats forwarding transport (controller fans out to nodes). Must match the controller. Distinct from the local backend's :9602 secret."
}

job "nomad-csi-driver-qnap-node" {
  type = "system"

  group "node" {
    task "plugin" {
      driver = "docker"

      config {
        image        = var.image
        force_pull   = true
        privileged   = true
        network_mode = "host"
        args = [
          "run",
          "--driver=qnap",
          "--mode=node",
          "--node-id=${node.unique.name}",
          "--plugin-id=${var.plugin_id}",
          "--config=/local/config.hcl",
          "--log-level=debug",
        ]
        # No explicit /dev mount: privileged already bind-mounts the host /dev
        # (iSCSI/multipath device nodes); adding it would be a "Duplicate mount
        # point: /dev" error.
        #
        # iSCSI/multipath are HOST-side (kernel modules + iscsid + multipathd run
        # on the host). With network_mode=host the container's
        # iscsiadm/multipathd reach the host daemons over the shared
        # netns; these bind-mounts share the host's iSCSI config/initiator + the
        # multipath drop-in dir the driver writes its QNAP profile into.
        volumes = [
          "/etc/iscsi:/etc/iscsi",       # initiatorname + node db (host iscsid config)
          "/etc/multipath:/etc/multipath", # driver writes conf.d/<profile>; multipathd reads it
          "/run/lock:/run/lock",         # iscsiadm/multipathd lock files
        ]
      }

      template {
        destination = "local/config.hcl"
        data        = <<EOH
qnap {
  # Read-only SAN access: required in node mode to resolve iSCSI sessions to volume
  # identities (per-volume stats rehydration + cold-cache teardown). Same creds the
  # controller uses (QNAP has no read-only API account).
  base_url = "${var.base_url}"
  username = "${var.username}"
  password = "${var.password}"
  insecure = ${var.insecure}

  # The node reads volume/iSCSI attach details from the CSI volume context; it only
  # needs the stats forwarding server config here so the controller can pull its
  # per-volume readings. Port :9612 is distinct from the local backend's :9602.
  forward_secret = "${var.forward_secret}"
  forward_addr   = ":9612"

  # Session reconciler (leaked-iSCSI-session / split-brain cleanup). OFF by
  # default in production; enabled here for e2e validation with a short grace
  # (production default is 5m) so the sweep→logout path is exercised in-suite.
  reconcile_enabled = true
  reconcile_grace   = "2m"
}
%{ if var.metrics_enabled ~}
metrics {
  enabled = true
  address = "${var.metrics_address}"
}
%{ endif ~}
# Shortened stats cadences for the e2e suite (production defaults are 60s/5m).
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
        type      = "node"
        mount_dir = "/csi"
      }

      resources {
        cpu    = 200
        memory = 256
      }
    }
  }
}
