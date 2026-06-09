# Example consumer for a --driver=qnap CSI volume. Mounts the volume and runs
# forever (tail -f /dev/null) so you can inspect the mount, e.g.:
#
#   nomad volume create examples/qnap-volume.hcl
#   nomad job run       examples/qnap-consumer.nomad.hcl
#   nomad alloc exec -job qnap-consumer df -h /data
#
# The LUN is reachable from any node, so this alloc can land anywhere the qnap
# node plugin runs.

job "qnap-consumer" {
  type = "service"

  group "app" {
    volume "data" {
      type            = "csi"
      source          = "qnap-data" # = id from examples/qnap-volume.hcl
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
