#!/usr/bin/env bash
set -euo pipefail

# Absorb the finite GNU parallel job token so it cannot leak into psql argv.
job_id="$1"
export job_id

# Run exactly one committed LOS batch for this job slot.
PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" \
    psql --no-psqlrc \
        --set=ON_ERROR_STOP=1 \
        -v batch_limit=50000 \
        -f scripts/fill_mesh_los_cache_batch.sql
