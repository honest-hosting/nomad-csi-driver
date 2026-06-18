# Example: qnap node plugin (system job — runs on every client). Performs
# iSCSI login, multipath assembly, format/mount. Never talks to the appliance.

job "csi-qnap-node" {
  datacenters = ["dc1"]
  type        = "system"

  group "node" {
    task "plugin" {
      driver = "docker"

      config {
        image = "quay.io/honesthosting/nomad-csi-driver:latest"
        # node-id uses the stable node NAME (matches /v1/nodes[].Name and the
        # stats `node` label), not the UUID.
        args = ["run", "--driver=qnap", "--mode=node", "--node-id=${node.unique.name}", "--config=/local/config.hcl"]
        # privileged + host networking: iSCSI/multipath are HOST-side (kernel
        # modules + iscsid + multipathd on the host). privileged already
        # bind-mounts host /dev (the iSCSI/dm device nodes — do NOT add /dev:/dev,
        # it errors "Duplicate mount point"). host networking shares the host
        # netns so the container's iscsiadm/multipathd reach the host daemons and
        # the metrics (:9502) + stats forward (:9612) ports bind reachably.
        privileged   = true
        network_mode = "host"
        volumes = [
          "/etc/iscsi:/etc/iscsi",         # initiatorname + node db (host iscsid config)
          "/etc/multipath:/etc/multipath", # driver writes conf.d/<profile>; multipathd reads it
          "/run/lock:/run/lock",           # iscsiadm/multipathd lock files
        ]
      }

      template {
        destination = "local/config.hcl"
        data        = <<-EOF
          # The node reads the portal(s)/IQN/LUN from the CSI volume context the
          # controller populates (multipath included), so it needs no appliance
          # creds or portals. The only config it needs is the stats forwarding
          # server, so the controller can pull its per-volume readings.
          qnap {
            # Same secret as the controller; :9612 is the cluster-uniform port the
            # controller dials. Omit to leave the node out of central stats.
            forward_secret = "{{ with nomadVar "nomad/jobs/csi-qnap-node" }}{{ .forward_secret }}{{ end }}"
            forward_addr   = ":9612"
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
          # Node-local stats hydration cadences (defaults shown). The node serves
          # readings to the controller; it has no query API of its own.
          stats {
            # interval      = "60s"
            # walk_interval = "5m"
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
