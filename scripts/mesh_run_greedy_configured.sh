#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=pg_settings.sh
source "$(dirname "$0")/pg_settings.sh"

enabled="$(pg_setting_bool enable_greedy)"

if [ "${enabled}" != t ]; then
    echo ">> Greedy placement disabled by mesh_pipeline_settings.enable_greedy"
    deleted="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "with d as (delete from mesh_towers where source in ('greedy', 'bridge') returning h3) select count(*) from d;")"
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "truncate mesh_greedy_iterations;"
    echo ">> Cleared ${deleted:-0} previous greedy/bridge towers (stage disabled)"
    exit 0
fi

max_iters="$(pg_setting_int greedy_iterations)"

PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy_prepare.sql
PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "call mesh_run_greedy_prepare();"

for iter in $(seq 1 "${max_iters}"); do
    placed="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select count(*) from mesh_towers where source = 'greedy'")"
    echo ">> Greedy iteration ${iter}/${max_iters} (${placed} towers placed so far)"
    PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy.sql
    PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql
done

final_placed="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select count(*) from mesh_towers where source = 'greedy'")"
echo ">> Greedy complete: ${final_placed} towers placed over ${max_iters} iteration(s)"

PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy_finalize.sql
PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql
