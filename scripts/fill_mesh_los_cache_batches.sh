#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=pg_settings.sh
source "$(dirname "$0")/pg_settings.sh"

batch_limit="$(pg_setting_int los_batch_limit)"

total="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select count(*) from mesh_route_missing_pairs")"
echo ">> Processing ${total} missing LOS pairs in batches of ${batch_limit}"
batch_num=0

while [ "$(PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select case when exists (select 1 from mesh_route_missing_pairs limit 1) then 1 else 0 end")" -eq 1 ]; do
    batch_num=$((batch_num + 1))
    remaining="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select count(*) from mesh_route_missing_pairs")"
    echo ">> Batch ${batch_num}: ${remaining} pairs remaining"
    PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0"         psql --no-psqlrc             --set=ON_ERROR_STOP=1             -v batch_limit="${batch_limit}"             -f scripts/fill_mesh_los_cache_batch.sql
done
echo ">> LOS cache batches complete (${batch_num} batches)"
