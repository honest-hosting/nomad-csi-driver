#!/usr/bin/env bash
# Deploys the CSI plugin jobs to the external cluster and waits for them healthy,
# leaving them running. Run this once before `make test-integration` (which only
# exercises volumes against the already-deployed plugins). Honors NOMAD_ADDR /
# NOMAD_TOKEN / NOMAD_SKIP_VERIFY natively.
#
#   local : one privileged docker monolith system job (nomad-csi-driver-local)
#   qnap  : controller (count-1 service) + node (system daemonset), deployed only
#           when QNAP_INTEGRATION_URL is set; shared plugin_id nomad-csi-driver-qnap
set -uo pipefail
cd "$(dirname "$0")/.." || exit 1

[ -n "${NOMAD_ADDR:-}" ] || { echo "[deploy] NOMAD_ADDR not set" >&2; exit 1; }

IMAGE="${NOMAD_CSI_INTEGRATION_IMAGE:-quay.io/honesthosting/nomad-csi-driver:latest}"
POOL1="${LOCAL_INTEGRATION_POOL1:-tank1}"            # default pool (the integration cluster has tank1 + tank2)
POOL2="${LOCAL_INTEGRATION_POOL2:-tank2}"          # second pool; set LOCAL_INTEGRATION_POOL2="" for a single-pool deploy
LOCAL_ID="${LOCAL_INTEGRATION_PLUGIN_ID:-nomad-csi-driver-local}"
QNAP_ID="nomad-csi-driver-qnap"

# wait_healthy <plugin_id> <min_nodes>: poll until the CSI plugin reports at
# least one healthy controller and <min_nodes> healthy nodes.
#
# Short by design (DEPLOY_HEALTHY_TIMEOUT, seconds, default 60). The plugin's own
# startup readiness gate keeps retrying until its backend is usable — a fresh node
# still creating/formatting its zpool can take many minutes — and Nomad reschedules
# it as needed, so a not-yet-ready node WILL become healthy without this script's
# help. We don't block on that here: if it isn't healthy within the window we bail,
# and the user just re-runs the make target once the cluster has settled.
wait_healthy() {
  local id="$1" min="$2" end=$((SECONDS + ${DEPLOY_HEALTHY_TIMEOUT:-60}))
  until nomad plugin status "$id" 2>/dev/null | awk -v m="$min" '
        /Controllers Healthy/ { c = $NF }
        /Nodes Healthy/       { n = $NF }
        END { exit !(c + 0 >= 1 && n + 0 >= m) }'; do
    if [ "$SECONDS" -ge "$end" ]; then
      echo "[deploy] '$id' not healthy within ${DEPLOY_HEALTHY_TIMEOUT:-60}s; it keeps retrying its readiness gate in the background (e.g. waiting on zpool create/format) — re-run the make target once it settles (nomad plugin status $id)"
      return 1
    fi
    sleep 3
  done
}

echo "[deploy] local engine -> $LOCAL_ID (image $IMAGE, pool $POOL1, pool2 ${POOL2:-<none>})"
nomad job run \
  -detach \
  -var "image=$IMAGE" \
  -var "plugin_id=$LOCAL_ID" \
  -var "pool=$POOL1" \
  -var "pool2=$POOL2" \
  -var "metrics_address=0.0.0.0:${LOCAL_INTEGRATION_METRICS_PORT:-9503}" \
  -var "readiness_timeout=${LOCAL_INTEGRATION_READINESS_TIMEOUT:-20m}" \
  localdev/csi-local.nomad.hcl || exit 1
wait_healthy "$LOCAL_ID" 2 || exit 1
echo "[deploy] local healthy"

if [ -n "${QNAP_INTEGRATION_URL:-}" ]; then
  if [ -z "${QNAP_INTEGRATION_PORTAL:-}${QNAP_INTEGRATION_PORTAL1:-}${QNAP_INTEGRATION_PORTAL2:-}" ]; then
    echo "[deploy] a QNAP iSCSI portal is required (the portal host nodes connect to): set" >&2
    echo "[deploy] QNAP_INTEGRATION_PORTAL (single path) or QNAP_INTEGRATION_PORTAL1[/PORTAL2]" >&2
    echo "[deploy] (multipath). Without one the controller won't start and node staging fails" >&2
    echo "[deploy] 'missing portal/iqn'." >&2
    exit 1
  fi
  insecure=true
  [ "${QNAP_INTEGRATION_INSECURE:-}" = "false" ] && insecure=false
  echo "[deploy] qnap engine -> $QNAP_ID (controller + node)"
  nomad job run \
    -detach \
    -var "image=$IMAGE" \
    -var "plugin_id=$QNAP_ID" \
    -var "base_url=$QNAP_INTEGRATION_URL" \
    -var "username=${QNAP_INTEGRATION_USER:-}" \
    -var "password=${QNAP_INTEGRATION_PASSWORD:-}" \
    -var "pool_id=${QNAP_INTEGRATION_POOL_ID:-1}" \
    -var "interfaces=${QNAP_INTEGRATION_IFACES:-}" \
    -var "portal=${QNAP_INTEGRATION_PORTAL:-}" \
    -var "portals=${QNAP_INTEGRATION_PORTAL1:-},${QNAP_INTEGRATION_PORTAL2:-}" \
    -var "insecure=$insecure" \
    -var "debug_http=${QNAP_INTEGRATION_DEBUG_HTTP:-false}" \
    -var "metrics_address=0.0.0.0:${QNAP_INTEGRATION_METRICS_PORT:-9501}" \
    -var "readiness_timeout=${QNAP_INTEGRATION_READINESS_TIMEOUT:-5m}" \
    localdev/csi-qnap-controller.nomad.hcl || exit 1
  nomad job run \
    -detach \
    -var "image=$IMAGE" \
    -var "plugin_id=$QNAP_ID" \
    -var "base_url=$QNAP_INTEGRATION_URL" \
    -var "username=${QNAP_INTEGRATION_USER:-}" \
    -var "password=${QNAP_INTEGRATION_PASSWORD:-}" \
    -var "insecure=$insecure" \
    -var "metrics_address=0.0.0.0:${QNAP_INTEGRATION_NODE_METRICS_PORT:-9502}" \
    localdev/csi-qnap-node.nomad.hcl || exit 1
  wait_healthy "$QNAP_ID" 1 || exit 1
  echo "[deploy] qnap healthy"
else
  echo "[deploy] QNAP_INTEGRATION_URL not set; skipping qnap engine"
fi

echo "[deploy] done — plugins are running. Next: make test-integration"
