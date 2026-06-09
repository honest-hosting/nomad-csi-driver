# A throwaway consumer the e2e suite uses to verify a local CSI volume: it
# mounts the volume and sleeps. Where this alloc lands proves topology pinning
# (the scheduler places it on the volume's owning node). docker-only, matching
# the cluster assumption.
#
# Run with: nomad job run -var volume_id=<id> consumer.nomad.hcl
# (Nomad job labels can't be variables, so the suite reuses this fixed id and
# purges between cases.)

variable "volume_id" {
  type        = string
  description = "CSI volume source (id registered via `nomad volume create`)."
}

job "ncd-e2e-consumer" {
  type = "batch"

  group "g" {
    volume "v" {
      type            = "csi"
      source          = var.volume_id
      attachment_mode = "file-system"
      access_mode     = "single-node-writer"
    }

    task "t" {
      driver = "docker"

      config {
        image   = "busybox:stable"
        command = "sleep"
        args    = ["3600"]
      }

      volume_mount {
        volume      = "v"
        destination = "/mnt/vol"
      }

      resources {
        cpu    = 100
        memory = 64
      }
    }
  }
}
