#!/usr/bin/env bash
# Best-effort cleanup of e2e artifacts left on the external cluster by an
# interrupted run (the Go suite cleans up via t.Cleanup on normal exit). Honors
# NOMAD_ADDR / NOMAD_TOKEN / NOMAD_SKIP_VERIFY natively.
#
# Order matters: stop the consuming workloads first (releases volume claims),
# then delete the volumes WHILE the controller plugins are still running (volume
# delete is routed to the controller), then purge the plugin jobs last.
set -uo pipefail

[ -n "${NOMAD_ADDR:-}" ] || { echo "[teardown] NOMAD_ADDR not set; nothing to do" >&2; exit 0; }

LOCAL_ID="${LOCAL_INTEGRATION_PLUGIN_ID:-nomad-csi-driver-local}"

echo "[teardown] stopping consuming workloads"
nomad job stop -purge "ncd-e2e-consumer" >/dev/null 2>&1 || true

# Every suite in test/ names its volumes ncd-<something>: ncd-e2e-, ncd-ctx-,
# ncd-stats-, ncd-obs-, ncd-noleak-, ncd-recon-, ncd-restart-, ... Matching only
# one suite's prefix orphaned the rest — and once the plugin jobs are purged below
# there is no controller left to delete them, so they survive until the plugins
# are redeployed. Match the shared prefix instead.
echo "[teardown] deleting ncd-* volumes (controllers still up)"
remaining=""
for _attempt in 1 2 3; do
  remaining=""
  while read -r v; do
    [ -n "$v" ] || continue
    nomad volume delete "$v" >/dev/null 2>&1 || remaining="${remaining} ${v}"
  done < <(nomad volume status 2>/dev/null | awk '/^ncd-/{print $1}')
  [ -n "${remaining# }" ] || break
  # A claim released by the job stop above can take a few seconds to clear, and a
  # volume is undeletable until it does. Retry before reporting it as stuck.
  sleep 5
done
if [ -n "${remaining# }" ]; then
  echo "[teardown] WARNING: could not delete:${remaining}"
  echo "[teardown]   still claimed, or its controller is unreachable. Check with"
  echo "[teardown]   'nomad volume status <id>'. The plugin purge below removes the"
  echo "[teardown]   controllers, so redeploy them before retrying the delete."
fi

echo "[teardown] purging plugin jobs"
nomad job stop -purge "$LOCAL_ID"                        >/dev/null 2>&1 || true
nomad job stop -purge "nomad-csi-driver-qnap-controller" >/dev/null 2>&1 || true
nomad job stop -purge "nomad-csi-driver-qnap-node"       >/dev/null 2>&1 || true

echo "[teardown] done"
