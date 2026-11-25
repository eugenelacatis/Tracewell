#!/usr/bin/env bash
# Creates the local kind cluster and deploys all of Tracewell onto it:
#   kind (1 control plane + 3 workers) -> Strimzi + Altinity operators -> Kafka (3 brokers, KRaft)
#   -> ClickHouse 2 shards x 2 replicas + 3 Keeper -> Postgres/Redis + apps (Helm) -> Prometheus + Grafana
# Safe to re-run: every step is idempotent. Tear down with deploy/kind/down.sh.
#
# Needs: docker (running), kind, kubectl, helm. Needs several GB of RAM for Docker (see docs/services/infra.md).
# Then: gateway on http://localhost:8080, Grafana on http://localhost:3300 (admin / tracewell).
#
#   KIND_CLUSTER=tracewell   cluster name       NS=tracewell   namespace for everything
#   SKIP_IMAGES=1            don't rebuild/load the app images (they must already be loaded)
set -euo pipefail

cd "$(dirname "$0")/../.."
CLUSTER="${KIND_CLUSTER:-tracewell}"
NS="${NS:-tracewell}"
STRIMZI_VERSION=1.3.1        # chart version; serves kafka.strimzi.io/v1, KRaft only
ALTINITY_VERSION=0.27.4      # clickhouse-operator chart version; also installs the Keeper CRD
K="kubectl --context kind-${CLUSTER} -n ${NS}"
H="helm --kube-context kind-${CLUSTER} -n ${NS}"

step() { printf '\n==> %s\n' "$*"; }
need() { command -v "$1" >/dev/null 2>&1 || { echo "missing tool: $1" >&2; exit 1; }; }

for t in docker kind kubectl helm; do need "$t"; done
docker info >/dev/null 2>&1 || { echo "Docker isn't running. Start Docker Desktop first." >&2; exit 1; }

# Waits until a resource exists, then until a jsonpath equals a value (kubectl wait fails on a missing resource).
wait_for() { # <resource> <jsonpath> <value> <timeout seconds>
  local deadline=$((SECONDS + $4))
  until $K get "$1" >/dev/null 2>&1; do
    [ "$SECONDS" -ge "$deadline" ] && { echo "timed out waiting for $1 to exist" >&2; exit 1; }
    sleep 3
  done
  $K wait "$1" --for="jsonpath={$2}=$3" --timeout="$(( deadline - SECONDS ))s"
}

step "kind cluster '${CLUSTER}'"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  echo "already exists"
else
  kind create cluster --config deploy/kind/kind-config.yaml
fi
kubectl --context "kind-${CLUSTER}" create namespace "$NS" --dry-run=client -o yaml | kubectl --context "kind-${CLUSTER}" apply -f -

step "Operators (Strimzi ${STRIMZI_VERSION}, Altinity ${ALTINITY_VERSION})"
helm repo add strimzi https://strimzi.io/charts/ >/dev/null 2>&1 || true
helm repo add altinity https://helm.altinity.com >/dev/null 2>&1 || true
helm repo update >/dev/null
$H upgrade --install strimzi strimzi/strimzi-kafka-operator --version "$STRIMZI_VERSION" \
  --set 'resources.requests.memory=192Mi' --set 'resources.limits.memory=384Mi'
$H upgrade --install clickhouse-operator altinity/altinity-clickhouse-operator --version "$ALTINITY_VERSION"
$K rollout status deployment/strimzi-cluster-operator --timeout=300s
$K rollout status deployment/clickhouse-operator-altinity-clickhouse-operator --timeout=300s

step "Kafka (3 brokers) and topics"
$K apply -f deploy/k8s/kafka/
wait_for kafka/tracewell '.status.conditions[?(@.type=="Ready")].status' True 900

step "ClickHouse Keeper (3) and the 2x2 cluster"
$K apply -f deploy/k8s/clickhouse/keeper.yaml
wait_for clickhousekeeperinstallation/tracewell-keeper '.status.status' Completed 600
$K apply -f deploy/k8s/clickhouse/clickhouse.yaml
wait_for clickhouseinstallation/tracewell '.status.status' Completed 900

step "ClickHouse schema (deploy/migrations/clickhouse-cluster)"
# ON CLUSTER runs it on every node from one pod. Every statement is IF NOT EXISTS, so re-running is harmless.
$K exec -i chi-tracewell-tracewell-0-0-0 -- clickhouse-client -u tracewell --password tracewell --multiquery \
  < deploy/migrations/clickhouse-cluster/001_cluster.sql

step "App images"
if [ "${SKIP_IMAGES:-0}" = "1" ]; then
  echo "SKIP_IMAGES=1: using images already loaded into the cluster"
else
  KIND_CLUSTER="$CLUSTER" bash deploy/scripts/build-images.sh --kind
fi

step "ConfigMaps for files that live outside the Helm chart"
$K create configmap postgres-migrations --from-file=deploy/migrations/postgres --dry-run=client -o yaml | $K apply -f -
HELM_EXTRA=()
if [ -f gateway/envoy.yaml ]; then
  $K create configmap gateway-config --from-file=envoy.yaml=gateway/envoy.yaml --dry-run=client -o yaml | $K apply -f -
else
  echo "WARNING: gateway/envoy.yaml doesn't exist yet (Track D). Deploying without the gateway: nothing"
  echo "         will answer on localhost:8080 until it lands and you re-run this script."
  HELM_EXTRA+=(--set services.gateway=null)
fi

step "Tracewell apps, Postgres and Redis (Helm)"
$H upgrade --install tracewell deploy/helm/tracewell ${HELM_EXTRA[@]+"${HELM_EXTRA[@]}"}
for d in postgres redis ingest writer query alerter evals web gateway; do
  kind_obj=deployment; [ "$d" = postgres ] && kind_obj=statefulset
  if $K get "$kind_obj/$d" >/dev/null 2>&1; then
    $K rollout status "$kind_obj/$d" --timeout=300s
  fi
done

step "Prometheus and Grafana"
$K create configmap prometheus-config --from-file=prometheus.yml=deploy/monitoring/prometheus.yml --dry-run=client -o yaml | $K apply -f -
$K create configmap grafana-datasources --from-file=deploy/monitoring/grafana/provisioning/datasources --dry-run=client -o yaml | $K apply -f -
$K create configmap grafana-dashboard-provider --from-file=deploy/monitoring/grafana/provisioning/dashboards --dry-run=client -o yaml | $K apply -f -
$K create configmap grafana-dashboards --from-file=deploy/monitoring/dashboards --dry-run=client -o yaml | $K apply -f -
$K apply -f deploy/k8s/monitoring/
$K rollout restart deployment/prometheus deployment/grafana >/dev/null  # pick up changed ConfigMaps on a re-run
$K rollout status deployment/prometheus --timeout=180s
$K rollout status deployment/grafana --timeout=180s

step "Ready"
cat <<MSG
  Gateway   http://localhost:8080   (needs gateway/envoy.yaml from Track D)
  Grafana   http://localhost:3300   admin / tracewell
  Pods      kubectl --context kind-${CLUSTER} -n ${NS} get pods
  Prove the ClickHouse cluster:  bash deploy/scripts/cluster-proof.sh
  Tear down:                     bash deploy/kind/down.sh
MSG
