#!/usr/bin/env bash
# Preflight for the integration/e2e suite, which runs against an EXTERNAL,
# already-running Nomad cluster (managed outside this repo). It only checks that
# we can reach that cluster — there is no localhost ZFS/root requirement, since
# the storage lives on the cluster nodes, not here.
#
# Bails immediately if NOMAD_ADDR isn't set or the cluster isn't reachable.
set -uo pipefail

fail() { echo "[preflight] FAIL: $*" >&2; exit 1; }

command -v nomad >/dev/null 2>&1 || fail "nomad CLI not found on PATH"

[ -n "${NOMAD_ADDR:-}" ] || fail "NOMAD_ADDR is not set — point it at your external Nomad cluster (e.g. https://nomad.example:4646)"

# Safety guard: the e2e suite is destructive (creates/deletes volumes, bounces
# plugins), so it must ONLY ever run against the integration cluster. Refuse to
# proceed unless NOMAD_ADDR points at the known integration node 192.168.56.51.
case "${NOMAD_ADDR}" in
  *192.168.56.51*) ;;
  *) fail "NOMAD_ADDR=${NOMAD_ADDR} does not contain 192.168.56.51 — refusing to run the destructive e2e suite against a non-integration (possibly production) Nomad cluster" ;;
esac

echo "[preflight] NOMAD_ADDR=$NOMAD_ADDR"
[ -n "${NOMAD_TOKEN:-}" ] && echo "[preflight] NOMAD_TOKEN is set"
[ "${NOMAD_SKIP_VERIFY:-}" = "1" ] || [ "${NOMAD_SKIP_VERIFY:-}" = "true" ] && echo "[preflight] NOMAD_SKIP_VERIFY enabled (TLS verification off)"

# Connectivity / auth check (honors NOMAD_TOKEN / NOMAD_SKIP_VERIFY natively).
if ! nomad node status >/dev/null 2>&1; then
  fail "cannot reach the Nomad cluster at $NOMAD_ADDR (check NOMAD_ADDR / NOMAD_TOKEN / NOMAD_SKIP_VERIFY and that the cluster is up)"
fi

ready=$(nomad node status 2>/dev/null | grep -cw ready)
echo "[preflight] reachable; $ready ready node(s)"
[ "$ready" -ge 1 ] || fail "no ready nodes in the cluster"

echo "[preflight] ok"
