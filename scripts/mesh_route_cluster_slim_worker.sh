#!/usr/bin/env bash
set -euo pipefail

iteration="$1"
worker_count="$2"
worker_index="$3"

PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0 -c mesh.cluster_slim_worker_count=${worker_count} -c mesh.cluster_slim_worker_index=${worker_index}" \
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "call mesh_route_cluster_slim(${iteration}, null);"
