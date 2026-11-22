#!/usr/bin/env bash
# Builds the six app images for the Helm chart and optionally loads them into a kind cluster.
# Each image is built from the repo root (Go services and evals need proto/gen) using the owning
# track's Dockerfile. Tags match deploy/helm/tracewell/values.yaml: tracewell/<service>:dev.
#
#   bash deploy/scripts/build-images.sh                 build only
#   bash deploy/scripts/build-images.sh --kind          build, then `kind load docker-image` each one
#   KIND_CLUSTER=tracewell TAG=dev ...                  override the cluster name or image tag
set -euo pipefail

cd "$(dirname "$0")/../.."
TAG="${TAG:-dev}"
CLUSTER="${KIND_CLUSTER:-tracewell}"

# service:Dockerfile
IMAGES="ingest:services/ingest/Dockerfile
writer:services/writer/Dockerfile
query:services/query/Dockerfile
alerter:services/alerter/Dockerfile
evals:workers/evals/Dockerfile
web:web/Dockerfile"

for entry in $IMAGES; do
  svc="${entry%%:*}"
  dockerfile="${entry#*:}"
  echo "==> building tracewell/${svc}:${TAG}"
  docker build -f "$dockerfile" -t "tracewell/${svc}:${TAG}" .
  if [ "${1:-}" = "--kind" ]; then
    kind load docker-image "tracewell/${svc}:${TAG}" --name "$CLUSTER"
  fi
done
echo "Done."
