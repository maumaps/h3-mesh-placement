#!/usr/bin/env bash
# Shared helpers for reading mesh_pipeline_settings from the database.
# Source this file; do not execute it directly.

pg_setting_bool() {
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -At \
        -c "select value::boolean from mesh_pipeline_settings where setting = '$1'"
}

pg_setting_int() {
    psql --no-psqlrc --set=ON_ERROR_STOP=1 -At \
        -c "select value::integer from mesh_pipeline_settings where setting = '$1'"
}
