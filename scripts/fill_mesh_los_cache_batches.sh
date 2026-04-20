#!/usr/bin/env bash
set -euo pipefail

batch_limit="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select value::integer from mesh_pipeline_settings where setting = 'los_batch_limit'")"

while [ "$(PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select case when exists (select 1 from mesh_route_missing_pairs limit 1) then 1 else 0 end")" -eq 1 ]; do
    PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0"         psql --no-psqlrc             --set=ON_ERROR_STOP=1             -v batch_limit="${batch_limit}"             -f scripts/fill_mesh_los_cache_batch.sql
done
