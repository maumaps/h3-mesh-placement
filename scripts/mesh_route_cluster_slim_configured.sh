#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=pg_settings.sh
source "$(dirname "$0")/pg_settings.sh"

enabled="$(pg_setting_bool enable_cluster_slim)"

if [ "${enabled}" != t ]; then
    echo ">> Cluster slim disabled by mesh_pipeline_settings.enable_cluster_slim"
    deleted="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "with d as (delete from mesh_towers where source = 'cluster_slim' returning h3) select count(*) from d;")"
    echo ">> Cleared ${deleted:-0} previous cluster-slim towers (stage disabled)"
    exit 0
fi

echo ">> Clearing previous cluster-slim towers and failures"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "delete from mesh_towers where source = 'cluster_slim'; truncate mesh_route_cluster_slim_failures;"

max_iters="${SLIM_ITERATIONS:-$(pg_setting_int cluster_slim_iterations)}"
worker_count="${SLIM_PARALLEL_WORKERS:-1}"
if [ "${worker_count}" -lt 1 ]; then
    echo ">> SLIM_PARALLEL_WORKERS must be at least 1, got ${worker_count}" >&2
    exit 1
fi
iter=0

while :; do
    iter=$((iter + 1))
    echo ">> Cluster slim iteration ${iter}"
    before_progress="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select count(*) from mesh_route_cluster_slim_failures;")"
    if [ "${worker_count}" -gt 1 ]; then
        echo ">> Cluster slim iteration ${iter} running ${worker_count} candidate shard worker(s)"
        promoted_output="$(
            seq 0 "$((worker_count - 1))" \
                | parallel --line-buffer --halt soon,fail=1 \
                    bash scripts/mesh_route_cluster_slim_worker.sh "${iter}" "${worker_count}" {}
        )"
        promoted="$(printf '%s\n' "${promoted_output}" | awk 'NF { total += $1 } END { print total + 0 }')"
    else
        promoted="$(bash scripts/mesh_route_cluster_slim_worker.sh "${iter}" 1 0)"
    fi
    promoted="${promoted:-0}"
    after_progress="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select count(*) from mesh_route_cluster_slim_failures;")"

    if [ "${promoted}" -eq 0 ] && [ "${after_progress}" -le "${before_progress}" ]; then
        echo ">> Cluster slim converged after $((iter - 1)) iteration(s)"
        break
    fi

    if [ "${promoted}" -eq 0 ]; then
        echo ">> Cluster slim advanced candidate log without new towers; continuing"
    fi

    if [ "${max_iters}" -gt 0 ] && [ "${iter}" -ge "${max_iters}" ]; then
        echo ">> Cluster slim hit iteration cap ${max_iters}"
        break
    fi
done
