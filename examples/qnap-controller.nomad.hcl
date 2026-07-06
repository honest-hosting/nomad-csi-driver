# Example: qnap controller plugin (one per cluster/DC). Provisions LUNs on the
# appliance (read-write); holds credentials. The node also talks to the appliance
# but READ-ONLY (session→volume resolution). Pair with qnap-node.nomad.hcl.

job "csi-qnap-controller" {
  datacenters = ["dc1"]
  type        = "service"

  group "controller" {
    count = 1

    task "plugin" {
      driver = "docker"

      config {
        image = "quay.io/honesthosting/nomad-csi-driver:latest"
        args  = ["run", "--driver=qnap", "--mode=controller", "--node-id=${node.unique.name}", "--plugin-id=qnap", "--config=/local/config.hcl"]
        # host networking so the controller reaches the appliance and binds its
        # reachable ports: metrics (:9501) and the stats query API (:9611).
        privileged   = true
        network_mode = "host"
      }

      # The stats fan-out discovers node daemons via Nomad's /v1/nodes and resolves
      # volume ids via /v1/volumes, both over the task API socket; this identity
      # block surfaces the workload-identity token (env + file). REQUIRED when
      # forward_secret is set below — without it the controller can't enumerate
      # nodes/volumes and fails to start the fan-out. With Nomad ACLs enabled, bind
      # a policy granting node:read + csi-list-volume + csi-read-volume:
      #   cat > ncd.policy.hcl <<'EOF'
      #   node { policy = "read" }
      #   namespace "default" { capabilities = ["csi-list-volume", "csi-read-volume"] }
      #   EOF
      #   nomad acl policy apply -job csi-qnap-controller -group controller \
      #     -task plugin csi-qnap-ncd ncd.policy.hcl
      identity {
        env  = true
        file = true
      }

      # Render the deployment config (use Vault/Nomad variables for secrets).
      template {
        destination = "local/config.hcl"
        data        = <<-EOF
          qnap {
            base_url        = "https://qnap.example.com"
            username        = "csi"
            password        = "{{ with nomadVar "nomad/jobs/csi-qnap-controller" }}{{ .qnap_password }}{{ end }}"
            # One iSCSI path per portal (multipath); list two for redundancy. The
            # node logs into all of them. A single `portal = "..."` is one path.
            portals         = ["10.0.10.5", "10.0.20.5"]
            default_pool_id = 1
            interfaces      = ["eth0"]
            # Per-volume stats fan-out: pull each node's readings over the
            # forwarding transport. Same secret as the node job; :9612 is the
            # uniform port nodes listen on (the controller dials, never binds it).
            # Omit forward_secret to leave central stats off.
            forward_secret  = "{{ with nomadVar "nomad/jobs/csi-qnap-controller" }}{{ .forward_secret }}{{ end }}"
            forward_addr    = ":9612"
          }
          # The plugin's own Prometheus endpoint (scraped directly, not via Nomad
          # telemetry). enabled is required — an address alone leaves metrics OFF.
          metrics {
            enabled = true
            address = ":9501"
          }
          # Startup gate: refuse to serve the CSI socket until the QNAP appliance
          # session is live, retrying for up to `timeout`, else exit non-zero so
          # Nomad reschedules. "0" (or omitting the block) = single attempt.
          readiness {
            timeout = "2m"
          }
          # Per-volume usage stats: the controller aggregates node readings and
          # serves the query API + nomad_csi_volume_* gauges. :9611 keeps it off
          # the local monolith's :9610 when both backends share a node.
          stats {
            query_addr = ":9611"
            # query_token = "{{ with nomadVar "nomad/jobs/csi-qnap-controller" }}{{ .stats_query_token }}{{ end }}"
          }
        EOF
      }

      csi_plugin {
        id        = "qnap"
        type      = "controller"
        mount_dir = "/csi"
      }

      resources {
        cpu    = 200
        memory = 128
      }
    }
  }
}
