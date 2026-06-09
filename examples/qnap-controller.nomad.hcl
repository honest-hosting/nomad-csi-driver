# Example: qnap controller plugin (one per cluster/DC). Sole talker to the
# appliance; holds credentials. Pair with qnap-node.nomad.hcl.

job "csi-qnap-controller" {
  datacenters = ["dc1"]
  type        = "service"

  group "controller" {
    count = 1

    task "plugin" {
      driver = "docker"

      config {
        image      = "quay.io/honesthosting/nomad-csi-driver:latest"
        args       = ["run", "--driver=qnap", "--mode=controller", "--config=/local/config.hcl"]
        privileged = true
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
