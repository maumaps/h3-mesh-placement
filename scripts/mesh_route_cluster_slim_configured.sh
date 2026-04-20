#!/usr/bin/env bash
set -euo pipefail

enabled="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select value::boolean from mesh_pipeline_settings where setting = 'enable_cluster_slim'")"

if [ "${enabled}" != t ]; then
    echo ">> Cluster slim disabled by mesh_pipeline_settings.enable_cluster_slim"
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "delete from mesh_towers where source = 'cluster_slim';"
    exit 0
fi

echo ">> Clearing previous cluster-slim towers and failures"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "delete from mesh_towers where source = 'cluster_slim'; truncate mesh_route_cluster_slim_failures;"

max_iters="${SLIM_ITERATIONS:-$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select value::integer from mesh_pipeline_settings where setting = 'cluster_slim_iterations'")}"
iter=0

while :; do
    iter=$((iter + 1))
    echo ">> Cluster slim iteration ${iter}"
    promoted="$(PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "call mesh_route_cluster_slim(${iter}, null);")"
    promoted="${promoted:-0}"

    if [ "${promoted}" -eq 0 ]; then
        echo ">> Cluster slim converged after $((iter - 1)) iteration(s)"
        break
    fi

    if [ "${max_iters}" -gt 0 ] && [ "${iter}" -ge "${max_iters}" ]; then
        echo ">> Cluster slim hit iteration cap ${max_iters}"
        break
    fi
done
