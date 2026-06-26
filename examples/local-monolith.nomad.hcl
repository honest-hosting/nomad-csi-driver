# Example: local backend monolith (system job — runs on every node). Single
# plugin_id; controllers forward owner-node operations over the forwarding port.
# Requires ZFS on the host; peers are discovered via the Nomad task API
# (api.sock) — see the identity block below. No Consul.

job "csi-local" {
  datacenters = ["dc1"]
  type        = "system"

  group "monolith" {
    task "plugin" {
      driver = "docker"

      config {
        image = "quay.io/honesthosting/nomad-csi-driver:latest"
        # node-id uses the stable node NAME (not the UUID) so it lines up with
        # the Nomad node roster (/v1/nodes[].Name) / topology segments.
        # zvols land under <pool>/nomad-csi/<volume-id> by default. Add
        # --parent-dataset=<name> (e.g. your csi_plugin id) to namespace per
        # deployment, or set parent_dataset per pool in the config below.
        args = ["run", "--driver=local", "--mode=monolith", "--node-id=${node.unique.name}", "--config=/local/config.hcl"]
        # privileged: the cgroup device controller must permit /dev/zfs (a
        # non-privileged container is denied EPERM). host networking so the
        # forwarding (:9602) + stats query (:9610) + metrics (:9503) ports bind
        # the node and peers/scrapers reach them.
        privileged   = true
        network_mode = "host"
        # No explicit /dev mount: privileged already bind-mounts the host /dev
        # (incl. /dev/zfs + dynamically-created /dev/zvol/*); adding "/dev:/dev"
        # would fail with "Duplicate mount point: /dev".
      }

      template {
        destination = "local/config.hcl"
        data        = <<-EOF
          local {
            # Allowlist of usable zpools by NAME (the driver never creates them).
            # default_pool is used when a volume omits parameters.pool.
            default_pool = "tank"
            pool "tank" {}
            default_volblocksize = "16K"
            forward_addr         = ":9602"
            forward_secret       = "{{ with nomadVar "nomad/jobs/csi-local" }}{{ .forward_secret }}{{ end }}"
            # Peer discovery is Nomad's /v1/nodes over api.sock (enabled by the
            # identity block below) — the only mechanism. Omit a `nomad {}` block
            # for defaults, or add one to tune cache_ttl / node_filter.
          }
          # The monolith's own Prometheus endpoint (one process carries both the
          # controller and node series). Scraped directly, not via Nomad telemetry.
          metrics {
            enabled = true
            address = ":9503"
          }
          # Startup gate: refuse to serve the CSI socket until a configured zpool
          # is imported + ONLINE, retrying for up to `timeout`. If it never comes
          # up the process exits non-zero and Nomad reschedules. 2m absorbs a slow
          # boot-time pool import; "0" (or omitting the block) = single attempt.
          readiness {
            timeout = "2m"
          }
          # Per-volume usage stats (on by default): a query API on :9610 and
          # nomad_csi_volume_* gauges on the metrics endpoint above. Defaults
          # (60s statfs / 5m walk) are production-sane; uncomment to tune or to
          # require a query token. Set query_addr = "" to disable the API.
          stats {
            # query_addr  = ":9610"
            # query_token = "{{ with nomadVar "nomad/jobs/csi-local" }}{{ .stats_query_token }}{{ end }}"
          }
        EOF
      }

      # Peer discovery (/v1/nodes) and the stats query API's Nomad-id resolution
      # (/v1/volumes) both go over the task API socket; the identity block
      # surfaces the workload-identity token (env + file). With ACLs enabled, bind
      # a policy granting node:read + csi-list-volume + csi-read-volume to this task, e.g.:
      #   cat > ncd.policy.hcl <<'EOF'
      #   node { policy = "read" }
      #   namespace "default" { capabilities = ["csi-list-volume", "csi-read-volume"] }
      #   EOF
      #   nomad acl policy apply -job csi-local -group monolith -task plugin \
      #     csi-local-ncd ncd.policy.hcl
      identity {
        env  = true
        file = true
      }

      csi_plugin {
        id        = "local"
        type      = "monolith"
        mount_dir = "/csi"
      }

      resources {
        cpu    = 200
        memory = 128
      }
    }
  }
}
