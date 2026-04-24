set client_min_messages = notice;

-- Drop MQTT targets that remain outside the seed-reachable LOS component after
-- route construction had a chance to connect them.
do
$$
declare
    max_distance double precision := 100000;
    mast_height double precision := 28;
    frequency double precision := 868000000;
    pruned_mqtt_count integer;
    pruned_generated_count integer;
begin
    if to_regclass('mesh_towers') is null then
        raise exception 'mesh_towers is missing; cannot prune unreached MQTT nodes';
    end if;

    if to_regclass('mesh_los_cache') is null then
        raise exception 'mesh_los_cache is missing; cannot prune unreached MQTT nodes';
    end if;

    if to_regclass('mesh_pipeline_settings') is not null then
        select coalesce((
            select value::double precision
            from mesh_pipeline_settings
            where setting = 'max_los_distance_m'
        ), max_distance)
        into max_distance;

        select coalesce((
            select value::double precision
            from mesh_pipeline_settings
            where setting = 'mast_height_m'
        ), mast_height)
        into mast_height;

        select coalesce((
            select value::double precision
            from mesh_pipeline_settings
            where setting = 'frequency_hz'
        ), frequency)
        into frequency;
    end if;

    create temporary table tmp_seed_reachable_towers (
        tower_id integer primary key
    ) on commit drop;

    with recursive visible_edges as (
        -- Reuse cached LOS directly so this pruning can run before the
        -- expensive mesh_visibility_edges refresh.
        select distinct
            src.tower_id as source_id,
            dst.tower_id as target_id
        from mesh_towers src
        join mesh_towers dst on dst.tower_id <> src.tower_id
        join lateral (
            select 1
            from mesh_los_cache link
            where link.mast_height_src = mast_height
              and link.mast_height_dst = mast_height
              and link.frequency_hz = frequency
              and link.clearance > 0
              and link.distance_m <= max_distance
              and (
                    (link.src_h3 = src.h3 and link.dst_h3 = dst.h3)
                    or (link.src_h3 = dst.h3 and link.dst_h3 = src.h3)
                )
            limit 1
        ) link on true
    ),
    seed_towers as (
        select tower_id
        from mesh_towers
        where source = 'seed'
    ),
    reached(tower_id) as (
        select tower_id
        from seed_towers

        union

        select visible_edges.target_id
        from reached
        join visible_edges on visible_edges.source_id = reached.tower_id
    )
    insert into tmp_seed_reachable_towers (tower_id)
    select tower_id
    from reached;

    delete from mesh_towers t
    where t.source = 'mqtt'
      and not exists (
            select 1
            from tmp_seed_reachable_towers reached
            where reached.tower_id = t.tower_id
        );
    get diagnostics pruned_mqtt_count = row_count;

    delete from mesh_towers t
    where t.source in ('route', 'cluster_slim', 'bridge', 'greedy')
      and not exists (
            select 1
            from tmp_seed_reachable_towers reached
            where reached.tower_id = t.tower_id
        );
    get diagnostics pruned_generated_count = row_count;

    raise notice 'Pruned % unreached MQTT tower(s) and % generated unreachable relay tower(s)',
        pruned_mqtt_count,
        pruned_generated_count;
end;
$$;
