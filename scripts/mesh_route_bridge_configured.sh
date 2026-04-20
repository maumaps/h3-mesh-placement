#!/usr/bin/env bash
set -euo pipefail

enabled="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select value::boolean from mesh_pipeline_settings where setting = 'enable_route_bridge'")"

if [ "${enabled}" = t ]; then
    echo ">> Clearing previous route bridge towers"
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "delete from mesh_towers where source = 'route';"
    PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0"         psql --no-psqlrc             --set=ON_ERROR_STOP=1             -f procedures/mesh_route_bridge.sql
else
    echo ">> Route bridge disabled by mesh_pipeline_settings.enable_route_bridge"
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "delete from mesh_towers where source = 'route';"
fi
