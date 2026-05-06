#!/usr/bin/env bash
# Brings up the SpiceDB service used by Skyramp Testbot.
#
# Idempotent: Skyramp invokes this from BOTH the workflow pre-step and
# the action's targetSetupCommand (matching the ebikes-lwc / mealie
# lifecycle). If the second call tore the container down, the
# preshared key minted by the pre-step would still match (it's static
# config, not a JWT) — but rebuilding the image twice would waste 3-5
# minutes on Go compile per run, so we still fast-path on existing health.
#
# Behavior:
#   1. If /openapi.json is already responding 2xx, exit 0 with no changes.
#   2. Otherwise compose-up --build (PR source) and block until healthy.
#      Loud failure with `docker compose ps` + last logs on timeout.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${SCRIPT_DIR}/docker-compose.testbot.yaml"
HEALTH_URL="http://localhost:8443/openapi.json"
MAX_WAIT_SECONDS="${SPICEDB_READY_TIMEOUT:-600}"

# Fast path: another caller already brought SpiceDB up — leave it alone.
if curl -fsS "${HEALTH_URL}" >/dev/null 2>&1; then
  echo "[skyramp] spicedb already healthy at ${HEALTH_URL}; nothing to do"
  exit 0
fi

echo "[skyramp] starting spicedb via ${COMPOSE_FILE} (build from PR source, --no-cache)"
# --no-cache forces docker buildx to ignore any layer cache it might
# have inherited from a registry mirror or previous run. Without this,
# the COPY . . layer can be served from cache when the build context
# digest happens to match a prior PR's, and the binary inside the
# container ends up compiled from older source — the exact symptom
# Skyramp's testbot flagged when 3 of 4 integration tests on PR #3
# kept failing with HTTP 400 parse errors despite docker compose down.
# The cost is ~3-5 min of extra Go compile per CI run, which is
# acceptable in exchange for guaranteed fresh code.
docker compose -f "${COMPOSE_FILE}" build --no-cache spicedb
docker compose -f "${COMPOSE_FILE}" up -d

echo "[skyramp] waiting up to ${MAX_WAIT_SECONDS}s for ${HEALTH_URL}"
deadline=$(( $(date +%s) + MAX_WAIT_SECONDS ))
while (( $(date +%s) < deadline )); do
  if curl -fsS "${HEALTH_URL}" >/dev/null 2>&1; then
    echo "[skyramp] spicedb is ready"
    exit 0
  fi
  sleep 2
done

echo "[skyramp] spicedb did NOT become ready within ${MAX_WAIT_SECONDS}s — last logs:"
docker compose -f "${COMPOSE_FILE}" ps || true
docker compose -f "${COMPOSE_FILE}" logs --tail=80 spicedb || true
exit 1
