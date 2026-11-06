#!/usr/bin/env bash
# Compose smoke test: build and start the whole stack, then wait until every app answers /healthz.
# Used by CI (.github/workflows/ci.yml) and runnable locally:  bash deploy/scripts/smoke.sh
#
# It runs under its own compose project name, so it never touches the volumes of your normal dev
# stack. It does need the host ports free (8081-8085, 3000, 9092, ...), so stop the dev stack first.
#
#   SMOKE_TIMEOUT  seconds to wait for the apps to become healthy once started (default 180)
#   SMOKE_KEEP=1   leave the stack running afterwards (default: tear it down and delete its volumes)
set -euo pipefail

cd "$(dirname "$0")/../.."
export COMPOSE_PROJECT_NAME=tracewell-smoke
TIMEOUT="${SMOKE_TIMEOUT:-180}"

# port:name, for every app that has a /healthz endpoint (web has none yet)
APPS="8081:ingest 8082:query 8083:alerter 8084:writer 8085:evals"

teardown() {
  if [ "${SMOKE_KEEP:-0}" = "1" ]; then
    echo "SMOKE_KEEP=1: leaving the stack up (project $COMPOSE_PROJECT_NAME)"
  else
    docker compose --profile apps down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap teardown EXIT

fail() {
  echo "::error::smoke test failed: $1"
  docker compose --profile apps ps -a || true
  docker compose --profile apps logs --tail 80 || true
  exit 1
}

echo "Building and starting the stack..."
docker compose --profile apps up -d --build || fail "docker compose up did not succeed"

echo "Waiting up to ${TIMEOUT}s for every /healthz..."
deadline=$((SECONDS + TIMEOUT))
pending="$APPS"
while [ -n "$pending" ]; do
  still=""
  for app in $pending; do
    port="${app%%:*}"
    if curl -fsS -m 3 "http://localhost:${port}/healthz" >/dev/null 2>&1; then
      echo "  ok   ${app#*:} (:${port})"
    else
      still="$still $app"
    fi
  done
  pending="${still# }"
  [ -z "$pending" ] && break
  [ "$SECONDS" -ge "$deadline" ] && fail "not healthy after ${TIMEOUT}s: $pending"
  sleep 3
done

echo "All apps healthy."
