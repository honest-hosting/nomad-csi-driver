# Example: qnap node plugin (system job — runs on every client). Performs
# iSCSI login, multipath assembly, format/mount. Never talks to the appliance.

job "csi-qnap-node" {
  datacenters = ["dc1"]
  type        = "system"

  group "node" {
    task "plugin" {
      driver = "docker"

      config {
        image      = "quay.io/honesthosting/nomad-csi-driver:latest"
        args       = ["run", "--driver=qnap", "--mode=node", "--node-id=${node.unique.id}", "--config=/local/config.hcl"]
        privileged = true
      }

      template {
        destination = "local/config.hcl"
        data        = <<-EOF
          # The node reads the portal(s)/IQN/LUN from the CSI volume context the
          # controller populates (multipath included), so it needs no appliance
          # creds or portals — an empty block is enough. Optional node-side knobs:
          qnap {
            # disable_multipath    = false  # true = raw single device, no dm-multipath
            # multipath_config_dir = "/etc/multipath/conf.d"
            # node_state_dir       = "/var/lib/nomad-csi-driver/qnap"
          }
          # Distinct port from the controller's :9501 so a co-located
          # controller+node (both host-networked) don't collide.
          metrics {
            enabled = true
            address = ":9502"
          }
        EOF
      }

      csi_plugin {
        id        = "qnap"
        type      = "node"
        mount_dir = "/csi"
      }

      resources {
        cpu    = 200
        memory = 128
      }
    }
  }
}
