#!/usr/bin/env bash
set -euo pipefail

if [ "$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select case when to_regclass('mesh_route_missing_pairs') is null then 0 else 1 end")" -eq 0 ]; then
    echo ">> Building LOS pair queue (mesh_route_missing_pairs not found)"
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/fill_mesh_los_cache_prepare.sql
fi

echo ">> Refreshing queue indexes"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/fill_mesh_los_cache_queue_indexes.sql
scripts/fill_mesh_los_cache_batches.sh
echo ">> Finalizing route graph"
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/fill_mesh_los_cache_finalize.sql
