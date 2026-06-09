//
// Packer build for the nomad-csi-driver container image.
//
// The binary is built first (`make build`); Packer only copies it in — this
// sidesteps the dev-only `replace => ../go-qnap` directive, which can't be
// resolved inside an isolated build.
//
// The image carries the node-side tooling and a ZFS userland matching the
// cluster nodes' kernel module (debian 12 -> zfs 2.1.x).
//

packer {
  required_plugins {
    docker = {
      version = ">= 1.0"
      source  = "github.com/hashicorp/docker"
    }
  }
}

//
// Variables
//
variable "app_name" {
  type    = string
  default = "nomad-csi-driver"
}

variable "app_file_path" {
  type    = string
  default = "bin/nomad-csi-driver"
}

variable "app_build_tags" {
  type    = list(string)
  default = ["latest"]
}

variable "base_image" {
  type    = string
  default = "debian"
}

variable "base_image_version" {
  type    = string
  default = "bookworm-slim"
}

variable "docker_repo" {
  type = string
}

variable "docker_host" {
  type = string
}

variable "docker_username" {
  type = string
}

variable "docker_password" {
  type = string
}

//
// The base image
//
source "docker" "container" {
  image  = "${var.base_image}:${var.base_image_version}"
  commit = true
  changes = [
    "ENTRYPOINT [\"/opt/${var.app_name}/${var.app_file_path}\"]",
  ]
}

//
// Build
//
build {
  name    = "container"
  sources = ["source.docker.container"]

  provisioner "shell" {
    inline_shebang = "/bin/bash -e"
    inline = [
      "pushd /tmp >/dev/null",

      # zfsutils-linux lives in the 'contrib' component, not 'main' — enable it
      # first (handles both the deb822 and classic sources formats).
      "echo '    -> Enabling contrib (for zfsutils-linux) ...'",
      "if [ -f /etc/apt/sources.list.d/debian.sources ]; then sed -i '/^Components:/ s/$/ contrib/' /etc/apt/sources.list.d/debian.sources; fi",
      "if [ -f /etc/apt/sources.list ]; then sed -i '/^deb / s/$/ contrib/' /etc/apt/sources.list; fi",

      # Node-side tooling + ZFS userland (matches the cluster's kernel module).
      "apt-get update >/dev/null",
      "apt-get install -y --no-install-recommends zfsutils-linux e2fsprogs xfsprogs util-linux mount open-iscsi multipath-tools ca-certificates >/dev/null",

      "mkdir -p /opt/${var.app_name}/bin",

      "echo '    -> Performing cleanup ...'",
      "rm -rf /tmp/* /var/lib/apt/lists/* >/dev/null",
      "popd >/dev/null",
    ]
  }

  provisioner "file" {
    source      = var.app_file_path
    destination = "/opt/${var.app_name}/${var.app_file_path}"
  }

  post-processors {
    post-processor "docker-tag" {
      repository = "${var.docker_repo}/${var.app_name}"
      tags       = var.app_build_tags
    }

    post-processor "docker-push" {
      login          = true
      login_server   = var.docker_host
      login_username = var.docker_username
      login_password = var.docker_password
    }
  }
}
