#!/usr/bin/env bash
set -euo pipefail

backup_path="${1:-data/backups/mesh_los_cache.latest.dump}"

if [ ! -s "${backup_path}" ]; then
    echo ">> LOS cache backup not found or empty: ${backup_path}" >&2
    exit 1
fi

pg_restore --list "${backup_path}" >/dev/null

quarantine_name="mesh_los_cache_before_restore_$(date -u +%Y%m%dT%H%M%SZ)"
failed_restore_name="mesh_los_cache_failed_restore_$(date -u +%Y%m%dT%H%M%SZ)"

existing_cache="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select to_regclass('mesh_los_cache') is not null")"
if [ "${existing_cache}" = "t" ]; then
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "alter table mesh_los_cache rename to ${quarantine_name};"
    echo ">> Existing mesh_los_cache preserved as ${quarantine_name}"
fi

if ! pg_restore --no-owner --no-acl --dbname="${PGDATABASE:-$USER}" "${backup_path}"; then
    restored_cache="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select to_regclass('mesh_los_cache') is not null")"
    if [ "${restored_cache}" = "t" ]; then
        psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "alter table mesh_los_cache rename to ${failed_restore_name};"
        echo ">> Failed restore table preserved as ${failed_restore_name}" >&2
    fi

    quarantined_cache="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select to_regclass('${quarantine_name}') is not null")"
    if [ "${quarantined_cache}" = "t" ]; then
        psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "alter table ${quarantine_name} rename to mesh_los_cache;"
        echo ">> Previous mesh_los_cache restored from ${quarantine_name}" >&2
    fi

    exit 1
fi

psql --no-psqlrc --set=ON_ERROR_STOP=1 <<'SQL'
-- Recreate the covering index expected by cache readers after restore.
create index if not exists mesh_los_cache_pkey_include
    on mesh_los_cache (
        src_h3,
        dst_h3,
        mast_height_src,
        mast_height_dst,
        frequency_hz
    )
    include (
        clearance,
        path_loss_db,
        distance_m,
        d1_m,
        d2_m,
        computed_at
    );
SQL
