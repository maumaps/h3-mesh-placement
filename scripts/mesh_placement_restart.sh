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

echo ">> Applying configured cluster-slim stage"
PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_cluster_slim.sql
scripts/mesh_route_cluster_slim_configured.sh

echo ">> Contracting soft population anchors"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_population_anchor_contract.sql

echo ">> Contracting generated tower pairs"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_generated_pair_contract.sql

echo ">> Rerouting local route relay segments"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_segment_reroute.sql

echo ">> Refreshing route visibility diagnostics"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql

echo ">> Applying configured greedy stage"
scripts/mesh_run_greedy_configured.sh

echo ">> Applying configured tower wiggle stage"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_tower_wiggle.sql
scripts/mesh_tower_wiggle_configured.sh
