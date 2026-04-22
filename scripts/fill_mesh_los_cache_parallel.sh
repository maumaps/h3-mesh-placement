#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=pg_settings.sh
source "$(dirname "$0")/pg_settings.sh"

# Guard against launching the parallel backfill before the queue exists.
if [ "$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select case when to_regclass('mesh_route_missing_pairs') is null then 0 else 1 end")" -eq 0 ]; then
    echo ">> mesh_route_missing_pairs is missing; run db/procedure/fill_mesh_los_cache_prepare first" >&2
    exit 1
fi

# Refresh queue indexes once before workers start claiming committed batches.
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/fill_mesh_los_cache_queue_indexes.sql

# Read user-tunable parallelism and batch sizing from the single pipeline config.
batch_limit="$(pg_setting_int los_batch_limit)"
parallel_jobs="$(pg_setting_int los_parallel_jobs)"

# Snapshot the current queue length into a finite job list so GNU parallel ETA is meaningful.
job_count="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select ceil(count(*)::numeric / ${batch_limit})::integer from mesh_route_missing_pairs")"
if [ "${job_count}" -le 0 ]; then
    echo ">> mesh_route_missing_pairs is already empty"
    exit 0
fi

# Use ETA only when running interactively, because GNU parallel writes progress to /dev/tty.
parallel_eta_args=()
if [ -t 1 ] || [ -t 2 ]; then
    parallel_eta_args+=(--eta)
fi

# Let GNU parallel use its CPU-count default when the config value is 0.
# Operators can still pin a smaller worker count by setting los_parallel_jobs.
parallel_job_args=()
if [ "${parallel_jobs}" -gt 0 ]; then
    parallel_job_args+=(--jobs "${parallel_jobs}")
fi

# Feed one finite job per batch into GNU parallel so each worker claims once and exits.
seq "${job_count}" | parallel \
    "${parallel_job_args[@]}" \
    "${parallel_eta_args[@]}" \
    --halt now,fail=1 \
    --line-buffer \
    scripts/fill_mesh_los_cache_parallel_job.sh {}
