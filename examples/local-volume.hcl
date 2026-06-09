# Example CSI volume for the --driver=local backend (node-local ZFS zvol).
#
#   nomad volume create examples/local-volume.hcl
#
# The volume is pinned (via CSI topology) to the node that ends up owning the
# zvol; a consumer that mounts it is scheduled onto that node automatically.
# See examples/local-consumer.nomad.hcl.

id        = "local-data"
name      = "local-data"
type      = "csi"
plugin_id = "local" # must match csi_plugin.id in local-monolith.nomad.hcl

# ZFS sizing is fine-grained (rounded up to volblocksize), unlike QNAP's whole-GiB.
capacity_min = "1GiB"
capacity_max = "1GiB"

capability {
  access_mode     = "single-node-writer"
  attachment_mode = "file-system"
}

# All parameters are optional; the defaults the driver would apply are shown.
parameters {
  pool         = "tank"  # a zpool from the config allowlist; omit to use local.default_pool
  host         = "auto"  # "auto" = fewest-volumes node, or an explicit ${node.unique.name}
  fsType       = "ext4"  # ext4 | xfs
  volblocksize = "16K"   # ZFS zvol block size (set at create, immutable thereafter)
}
