#!/usr/bin/env bash
set -euo pipefail

enabled="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select value::boolean from mesh_pipeline_settings where setting = 'enable_greedy'")"

if [ "${enabled}" != t ]; then
    echo ">> Greedy placement disabled by mesh_pipeline_settings.enable_greedy"
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "delete from mesh_towers where source in ('greedy', 'bridge'); truncate mesh_greedy_iterations;"
    exit 0
fi

max_iters="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select value::integer from mesh_pipeline_settings where setting = 'greedy_iterations'")"

PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy_prepare.sql
PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "call mesh_run_greedy_prepare();"

for iter in $(seq 1 "${max_iters}"); do
    echo ">> Greedy iteration ${iter}"
    PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy.sql
    PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql
done

PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy_finalize.sql
PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql
