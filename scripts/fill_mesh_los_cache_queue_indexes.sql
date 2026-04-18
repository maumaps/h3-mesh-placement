set client_min_messages = notice;

-- Refuse to mutate indexes if the missing-pair queue is absent; callers should
-- materialize the queue first via the prepare stage.
do
$$
declare
    changed boolean := false;
begin
    if to_regclass('mesh_route_missing_pairs') is null then
        raise exception 'mesh_route_missing_pairs is missing; run the fill_mesh_los_cache prepare stage first';
    end if;

    -- Build the batch-order index only when it is truly absent. This avoids taking
    -- an unnecessary DDL lock on every extra resume worker once the queue is
    -- already indexed.
    if not exists (
        select 1
        from pg_indexes
        where schemaname = 'public'
          and tablename = 'mesh_route_missing_pairs'
          and indexname = 'mesh_route_missing_pairs_batch_order_idx'
    ) then
        execute $sql$
            create index mesh_route_missing_pairs_batch_order_idx
                on mesh_route_missing_pairs (
                    building_endpoint_count desc,
                    disconnected_priority,
                    priority,
                    building_count desc,
                    src_h3,
                    dst_h3
                )
        $sql$;
        changed := true;
    end if;

    -- Build the exact-match index only when it is truly absent for the same
    -- reason: additional resume workers should skip straight to batch work once
    -- queue indexes exist.
    if not exists (
        select 1
        from pg_indexes
        where schemaname = 'public'
          and tablename = 'mesh_route_missing_pairs'
          and indexname = 'mesh_route_missing_pairs_src_dst_idx'
    ) then
        execute $sql$
            create index mesh_route_missing_pairs_src_dst_idx
                on mesh_route_missing_pairs (src_h3, dst_h3)
        $sql$;
        changed := true;
    end if;

    -- Refresh planner statistics only when index DDL changed the table access
    -- paths; extra resume workers should skip this full-table pass.
    if changed then
        execute 'analyze mesh_route_missing_pairs';
    end if;
end;
$$;
