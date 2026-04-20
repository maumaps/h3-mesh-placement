#!/usr/bin/env bash
set -euo pipefail

mkdir -p data/backups

row_count="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At <<'SQL'
select case
    when to_regclass('mesh_los_cache') is null then '-1'
    else (select count(*)::text from mesh_los_cache)
end;
SQL
)"

if [ "${row_count}" = "-1" ]; then
    echo ">> Refusing to back up LOS cache: mesh_los_cache table is missing" >&2
    exit 1
fi

if [ "${row_count}" = "0" ] && [ "${ALLOW_EMPTY_LOS_CACHE_BACKUP:-0}" != "1" ]; then
    echo ">> Refusing to overwrite LOS cache backup with an empty cache; set ALLOW_EMPTY_LOS_CACHE_BACKUP=1 to force" >&2
    exit 1
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
timestamped_dump="data/backups/mesh_los_cache.${timestamp}.dump"
latest_dump="data/backups/mesh_los_cache.latest.dump"

pg_dump --format=custom --no-owner --no-acl --table=mesh_los_cache --file="${timestamped_dump}"
cp "${timestamped_dump}" "${latest_dump}"

pg_restore --list "${latest_dump}" >/dev/null
printf '%s rows backed up to %s\n' "${row_count}" "${latest_dump}"
