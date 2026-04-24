#!/usr/bin/env bash
set -euo pipefail

max_iters="$1"
worker_index="$2"
iter=0

while :; do
    iter=$((iter + 1))
    echo ">> Wiggle worker ${worker_index} iteration ${iter}"
    moved="$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select mesh_tower_wiggle(false);")"
    moved="${moved:-0}"

    if [ "${moved}" -eq 0 ]; then
        echo ">> Wiggle worker ${worker_index} idle after $((iter - 1)) iteration(s)"
        break
    fi

    if [ "${max_iters}" -gt 0 ] && [ "${iter}" -ge "${max_iters}" ]; then
        echo ">> Wiggle worker ${worker_index} hit iteration cap ${max_iters}"
        break
    fi
done
