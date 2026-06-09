# Example CSI volume for the --driver=qnap backend (iSCSI block LUN on a QNAP).
#
#   nomad volume create examples/qnap-volume.hcl
#
# The LUN is reachable from any node; a consumer that mounts it can be scheduled
# anywhere the qnap node plugin runs. See examples/qnap-consumer.nomad.hcl.

id        = "qnap-data"
name      = "qnap-data"
type      = "csi"
plugin_id = "qnap" # must match csi_plugin.id in qnap-controller.nomad.hcl

# QNAP LUNs are whole-GiB sized — capacity MUST be GiB-aligned (the driver
# rejects non-GiB-aligned requests rather than silently rounding).
capacity_min = "1GiB"
capacity_max = "1GiB"

capability {
  access_mode     = "single-node-writer"
  attachment_mode = "file-system"
}

# All parameters are optional; the defaults the driver would apply are shown.
parameters {
  pool       = "1"     # QNAP storage pool ID; omit to use qnap.default_pool_id
  fsType     = "ext4"  # ext4 | xfs
  sectorSize = "512"   # 512 (default) or 4096 (Windows-only, unsupported here)
  thin       = "true"  # thin-provisioned LUN (default); "false" = thick
}
