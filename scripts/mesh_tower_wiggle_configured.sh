#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=pg_settings.sh
source "$(dirname "$0")/pg_settings.sh"

enabled="$(pg_setting_bool enable_wiggle)"

if [ "${enabled}" != t ]; then
    echo ">> Tower wiggle disabled by mesh_pipeline_settings.enable_wiggle"
    exit 0
fi

max_iters="${WIGGLE_ITERATIONS:-$(pg_setting_int wiggle_iterations)}"
parallel_workers="${WIGGLE_PARALLEL_WORKERS:-$(pg_setting_int_default wiggle_parallel_workers 8)}"
iter=0
reset=true

if [ "${parallel_workers}" -gt 0 ]; then
    echo ">> Tower wiggle enabling up to ${parallel_workers} PostgreSQL parallel worker(s) per heavy query"
    export PGOPTIONS="${PGOPTIONS:-} -c max_parallel_workers_per_gather=${parallel_workers} -c parallel_setup_cost=0 -c parallel_tuple_cost=0.001 -c min_parallel_table_scan_size=0 -c min_parallel_index_scan_size=0"
fi

while :; do
    iter=$((iter + 1))
    echo ">> Wiggle iteration ${iter}"
    moved="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select mesh_tower_wiggle(${reset});")"
    reset=false
    moved="${moved:-0}"

    if [ "${moved}" -eq 0 ]; then
        echo ">> Wiggle converged after $((iter - 1)) iteration(s)"
        break
    fi

    if [ "${max_iters}" -gt 0 ] && [ "${iter}" -ge "${max_iters}" ]; then
        echo ">> Wiggle hit iteration cap ${max_iters}"
        break
    fi
done
