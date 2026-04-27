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

resume="${SLIM_RESUME:-0}"

if [ "${resume}" = 1 ]; then
    echo ">> Resuming cluster slim without clearing existing cluster-slim towers"
else
    echo ">> Clearing previous cluster-slim towers, failures, queue, and claims"
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "delete from mesh_towers where source = 'cluster_slim'; truncate mesh_route_cluster_slim_failures, mesh_route_cluster_slim_candidate_queue, mesh_route_cluster_slim_claims;"
fi

max_iters="${SLIM_ITERATIONS:-$(pg_setting_int cluster_slim_iterations)}"
default_worker_count="$(nproc 2>/dev/null || printf '1')"
worker_count="${SLIM_PARALLEL_WORKERS:-${default_worker_count}}"
if [ "${worker_count}" -lt 1 ]; then
    echo ">> SLIM_PARALLEL_WORKERS must be at least 1, got ${worker_count}" >&2
    exit 1
fi
if [ "${resume}" = 1 ]; then
    iter="$(
        psql --no-psqlrc --set=ON_ERROR_STOP=1 -At <<'SQL'
with latest_iteration as (
    -- Replaying the latest prepared-but-unfinished iteration is safe because
    -- mesh_route_cluster_slim_prepare_iteration deletes only that iteration's
    -- queue and claims, while preserving already promoted towers and failures.
    select max(iteration_label) as iteration_label
    from mesh_route_cluster_slim_candidate_queue
),
latest_done as (
    select count(*) as completed_rows
    from mesh_route_cluster_slim_candidate_queue queue
    join latest_iteration latest using (iteration_label)
    where queue.status = 'completed'
)
select greatest(
    0,
    case
        when latest_iteration.iteration_label is null then 0
        when latest_done.completed_rows > 0 then latest_iteration.iteration_label
        else latest_iteration.iteration_label - 1
    end
)
from latest_iteration
cross join latest_done;
SQL
    )"
    iter="${iter:-0}"
    echo ">> Cluster slim resume will continue from iteration $((iter + 1))"
else
    iter=0
fi

refresh_parallel_spacing() {
    if [ "${worker_count}" -le 1 ] || [ "${promoted}" -le 0 ]; then
        return
    fi

    psql --no-psqlrc --set=ON_ERROR_STOP=1 <<'SQL'
with slim_towers as (
    -- Parallel workers update exact tower cells only; this serial pass updates
    -- nearby spacing once so workers do not deadlock on overlapping H3 radii.
    select
        surface.h3,
        surface.centroid_geog
    from mesh_towers tower
    join mesh_surface_h3_r8 surface on surface.h3 = tower.h3
    where tower.source = 'cluster_slim'
),
nearest_slim_tower as (
    -- Compute the closest cluster-slim tower per affected cell in one ordered update.
    select
        surface.h3,
        min(ST_Distance(surface.centroid_geog, slim_towers.centroid_geog)) as distance_m
    from mesh_surface_h3_r8 surface
    join slim_towers
      on ST_DWithin(surface.centroid_geog, slim_towers.centroid_geog, 100000)
    group by surface.h3
)
update mesh_surface_h3_r8 surface
set clearance = null,
    path_loss = null,
    visible_uncovered_population = null,
    visible_tower_count = null,
    distance_to_closest_tower = coalesce(
        least(surface.distance_to_closest_tower, nearest_slim_tower.distance_m),
        nearest_slim_tower.distance_m
    )
from nearest_slim_tower
where surface.h3 = nearest_slim_tower.h3
  and not exists (
        select 1
        from slim_towers
        where slim_towers.h3 = surface.h3
    );
SQL
}

while :; do
    iter=$((iter + 1))
    echo ">> Cluster slim iteration ${iter}"
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "call mesh_route_cluster_slim_prepare_iteration(${iter});"
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
    refresh_parallel_spacing
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
