#!/usr/bin/env bash
set -euo pipefail

missing_message="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At <<'SQL'
select case
    when to_regclass('mesh_pipeline_settings') is null then
        'missing table mesh_pipeline_settings; run make db/table/mesh_pipeline_settings first'
    when to_regclass('mesh_towers') is null then
        'missing table mesh_towers; run make db/table/mesh_towers first'
    when to_regclass('mesh_surface_h3_r8') is null then
        'missing table mesh_surface_h3_r8; run make db/table/mesh_surface_h3_r8 first'
    when to_regclass('mesh_los_cache') is null then
        'missing table mesh_los_cache; run make db/table/mesh_los_cache first'
    when to_regclass('mesh_greedy_iterations') is null then
        'missing table mesh_greedy_iterations; run make db/table/mesh_greedy_iterations first'
    else ''
end;
SQL
)"

if [ -n "${missing_message}" ]; then
    echo ">> Cannot safely restart placement: ${missing_message}" >&2
    exit 1
fi

restart_stamp="$(date -u +%Y%m%dT%H%M%SZ)_$$"
tower_backup="mesh_towers_restart_backup_${restart_stamp}"
surface_backup="mesh_surface_h3_r8_restart_backup_${restart_stamp}"

restore_restart_snapshot() {
    status=$?

    if [ "${status}" -ne 0 ]; then
        echo ">> Placement restart failed; restoring mesh_towers and surface metrics from ${tower_backup}" >&2
        psql --no-psqlrc --set=ON_ERROR_STOP=1 <<SQL
begin;
truncate mesh_towers restart identity;
insert into mesh_towers (tower_id, h3, source, recalculation_count, created_at)
select tower_id, h3, source, recalculation_count, created_at
from ${tower_backup};
select setval(
    pg_get_serial_sequence('mesh_towers', 'tower_id'),
    greatest(coalesce((select max(tower_id) from mesh_towers), 1), 1),
    true
);
update mesh_surface_h3_r8 surface
set has_tower = backup.has_tower,
    distance_to_closest_tower = backup.distance_to_closest_tower,
    clearance = backup.clearance,
    path_loss = backup.path_loss,
    visible_population = backup.visible_population,
    visible_uncovered_population = backup.visible_uncovered_population,
    visible_tower_count = backup.visible_tower_count
from ${surface_backup} backup
where surface.h3 = backup.h3;
commit;
SQL
    fi

    psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "drop table if exists ${tower_backup}; drop table if exists ${surface_backup};" >/dev/null || true
    exit "${status}"
}

psql --no-psqlrc --set=ON_ERROR_STOP=1 <<SQL
create table ${tower_backup} as
select tower_id, h3, source, recalculation_count, created_at
from mesh_towers;

create table ${surface_backup} as
select
    h3,
    has_tower,
    distance_to_closest_tower,
    clearance,
    path_loss,
    visible_population,
    visible_uncovered_population,
    visible_tower_count
from mesh_surface_h3_r8;
SQL

trap restore_restart_snapshot EXIT

echo ">> Applying configured coarse stage"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_coarse_grid.sql
psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "call mesh_coarse_grid();"

echo ">> Clearing restartable placement towers"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "delete from mesh_towers where source in ('route', 'cluster_slim', 'bridge', 'greedy'); truncate mesh_greedy_iterations;"

echo ">> Applying configured population anchor stage"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_population.sql

echo ">> Rebuilding route bootstrap pairs from current towers"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_route_bootstrap_pairs.sql
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_route_bootstrap.sql

echo ">> Applying configured route bridge stage"
scripts/mesh_route_bridge_configured.sh
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_prune_unreached_mqtt.sql
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/assert_mesh_towers_single_los_component.sql

echo ">> Applying configured cluster-slim stage"
PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_cluster_slim.sql
scripts/mesh_route_cluster_slim_configured.sh
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/assert_mesh_towers_single_los_component.sql

echo ">> Contracting soft population anchors"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_population_anchor_contract.sql
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/assert_mesh_towers_single_los_component.sql

echo ">> Contracting generated tower pairs"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_generated_pair_contract.sql
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/assert_mesh_towers_single_los_component.sql

echo ">> Rerouting local route relay segments"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_segment_reroute.sql
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/assert_mesh_towers_single_los_component.sql

echo ">> Refreshing route visibility diagnostics"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/assert_mesh_towers_single_los_component.sql

echo ">> Applying configured greedy stage"
scripts/mesh_run_greedy_configured.sh

echo ">> Applying configured tower wiggle stage"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_tower_wiggle.sql
scripts/mesh_tower_wiggle_configured.sh

echo ">> Applying manually reviewed route redundancy anchors"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_route_manual_redundancy.sql

echo ">> Refreshing visibility diagnostics after tower wiggle"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/assert_mesh_towers_single_los_component.sql
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/assert_mesh_visibility_no_bridges.sql

trap - EXIT
psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "drop table if exists ${tower_backup}; drop table if exists ${surface_backup};" >/dev/null
