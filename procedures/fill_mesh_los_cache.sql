-- This stage is now orchestrated by make in three committed steps:
-- scripts/fill_mesh_los_cache_prepare.sql
-- scripts/fill_mesh_los_cache_batch.sql
-- scripts/fill_mesh_los_cache_finalize.sql
--
-- Run `make db/procedure/fill_mesh_los_cache` so each batch commits and reruns
-- can resume from preserved staging instead of losing multi-hour progress.
do
$$
begin
    raise exception 'Run `make db/procedure/fill_mesh_los_cache`; this stage is split into prepare/batch/finalize scripts so long cache fills can resume safely';
end;
$$;
