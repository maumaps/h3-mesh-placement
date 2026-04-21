#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=pg_settings.sh
source "$(dirname "$0")/pg_settings.sh"

batch_limit="$(pg_setting_int los_batch_limit)"

PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0"     psql --no-psqlrc         --set=ON_ERROR_STOP=1         -v batch_limit="${batch_limit}"         -f scripts/fill_mesh_los_cache_batch.sql
