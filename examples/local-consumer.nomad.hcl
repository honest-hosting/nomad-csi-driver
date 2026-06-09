# Example consumer for a --driver=local CSI volume. Mounts the volume and runs
# forever (tail -f /dev/null) so you can inspect the mount, e.g.:
#
#   nomad volume create examples/local-volume.hcl
#   nomad job run       examples/local-consumer.nomad.hcl
#   nomad alloc exec -job local-consumer df -h /data
#
# The scheduler places this alloc on the volume's owning node (local volumes are
# node-pinned via CSI topology).

job "local-consumer" {
  type = "service"

  group "app" {
    volume "data" {
      type            = "csi"
      source          = "local-data" # = id from examples/local-volume.hcl
      attachment_mode = "file-system"
      access_mode     = "single-node-writer"
    }

    task "app" {
      driver = "docker"

      config {
        image   = "busybox:stable"
        command = "tail"
        args    = ["-f", "/dev/null"]
      }

      volume_mount {
        volume      = "data"
        destination = "/data"
      }

      resources {
        cpu    = 100
        memory = 64
      }
    }
  }
}
