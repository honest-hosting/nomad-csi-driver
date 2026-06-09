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

echo "[teardown] deleting ncd-e2e-* volumes (controllers still up)"
nomad volume status 2>/dev/null | awk '/^ncd-e2e-/{print $1}' | while read -r v; do
  [ -n "$v" ] || continue
  nomad volume delete "$v" >/dev/null 2>&1 || true
done

echo "[teardown] purging plugin jobs"
nomad job stop -purge "$LOCAL_ID"                        >/dev/null 2>&1 || true
nomad job stop -purge "nomad-csi-driver-qnap-controller" >/dev/null 2>&1 || true
nomad job stop -purge "nomad-csi-driver-qnap-node"       >/dev/null 2>&1 || true

echo "[teardown] done"
