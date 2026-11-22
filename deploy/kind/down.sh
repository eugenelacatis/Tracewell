#!/usr/bin/env bash
# Deletes the kind cluster and everything in it (all Kafka, ClickHouse and Postgres data included).
set -euo pipefail
kind delete cluster --name "${KIND_CLUSTER:-tracewell}"
