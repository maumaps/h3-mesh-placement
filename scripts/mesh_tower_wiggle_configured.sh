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

if [ "${parallel_workers}" -gt 1 ]; then
    echo ">> Wiggle resetting queue before ${parallel_workers} worker(s)"
    dirty_count="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At <<'SQL'
update mesh_towers
set recalculation_count = 0
where recalculation_count is null
  and source in ('population', 'route', 'cluster_slim', 'bridge', 'coarse');

-- Queue of towers that still need wiggle evaluation.
create table if not exists mesh_tower_wiggle_queue (
    tower_id integer primary key,
    is_dirty boolean not null default true
);

delete from mesh_tower_wiggle_queue;

insert into mesh_tower_wiggle_queue (tower_id, is_dirty)
select t.tower_id, true
from mesh_towers t
where t.source in ('population', 'route', 'cluster_slim', 'bridge', 'coarse');

select count(*)
from mesh_tower_wiggle_queue
where is_dirty;
SQL
)"
    dirty_count="${dirty_count##*$'\n'}"

    if [ "${dirty_count}" -eq 0 ]; then
        echo ">> Wiggle has no eligible dirty towers"
        exit 0
    fi

    seq "${parallel_workers}" \
        | parallel --line-buffer --halt soon,fail=1 \
            bash scripts/mesh_tower_wiggle_worker.sh "${max_iters}" {}

    exit 0
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
