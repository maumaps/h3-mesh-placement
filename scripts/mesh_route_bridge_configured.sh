#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=pg_settings.sh
source "$(dirname "$0")/pg_settings.sh"

enabled="$(pg_setting_bool enable_route_bridge)"

if [ "${enabled}" = t ]; then
    echo ">> Clearing previous route bridge towers"
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "delete from mesh_towers where source = 'route';"
    PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0"         psql --no-psqlrc             --set=ON_ERROR_STOP=1             -f procedures/mesh_route_bridge.sql
else
    echo ">> Route bridge disabled by mesh_pipeline_settings.enable_route_bridge"
    echo ">> Clearing previous route bridge towers (stage disabled)"
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "delete from mesh_towers where source = 'route';"
fi
