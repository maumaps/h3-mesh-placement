#!/usr/bin/env bash
set -euo pipefail

# Guard against launching the parallel backfill before the queue exists.
if [ "$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select case when to_regclass('mesh_route_missing_pairs') is null then 0 else 1 end")" -eq 0 ]; then
    echo "mesh_route_missing_pairs is missing; run db/procedure/fill_mesh_los_cache_prepare first"
    exit 1
fi

# Refresh queue indexes once before workers start claiming committed batches.
psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/fill_mesh_los_cache_queue_indexes.sql

# Snapshot the current queue length into a finite job list so GNU parallel ETA is meaningful.
job_count="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select ceil(count(*)::numeric / 50000)::integer from mesh_route_missing_pairs")"
if [ "${job_count}" -le 0 ]; then
    echo "mesh_route_missing_pairs is already empty"
    exit 0
fi

# Use ETA only when running interactively, because GNU parallel writes progress to /dev/tty.
parallel_eta_args=()
if [ -t 1 ] || [ -t 2 ]; then
    parallel_eta_args+=(--eta)
fi

# Feed one finite job per batch into GNU parallel so each worker claims once and exits.
seq "${job_count}" | parallel \
    --jobs 8 \
    "${parallel_eta_args[@]}" \
    --halt now,fail=1 \
    --line-buffer \
    scripts/fill_mesh_los_cache_parallel_job.sh {}
