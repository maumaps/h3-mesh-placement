set client_min_messages = notice;

-- Assert that the live tower LOS graph is one connected component.
do
$$
declare
    max_distance double precision := 100000;
    mast_height double precision := 28;
    frequency double precision := 868000000;
    tower_total integer;
    reached_total integer;
    visible_edge_total integer;
    disconnected_towers text;
begin
    if to_regclass('mesh_towers') is null then
        raise exception 'mesh_towers is missing; cannot verify the tower LOS component invariant';
    end if;

    if to_regclass('mesh_los_cache') is null then
        raise exception 'mesh_los_cache is missing; cannot verify the tower LOS component invariant';
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

    select count(*) into tower_total from mesh_towers;

    if tower_total <= 1 then
        raise notice 'Tower LOS component invariant holds: % live tower(s)', tower_total;
        return;
    end if;

    with visible_edges as (
        -- Use cached positive-clearance LOS pairs directly so this invariant can
        -- run immediately after each route-mutating stage without waiting for
        -- mesh_visibility_edges to be refreshed.
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
    )
    select count(*) into visible_edge_total from visible_edges;

    with recursive visible_edges as (
        -- Treat LOS as an undirected radio link; the cache stores canonical H3
        -- pairs, but either endpoint can use the link.
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
    start_tower as (
        -- Starting from the lowest id keeps failures deterministic and makes the
        -- disconnected set easy to compare between reruns.
        select min(tower_id) as tower_id
        from mesh_towers
    ),
    reached(tower_id) as (
        select tower_id
        from start_tower

        union

        select visible_edges.target_id
        from reached
        join visible_edges on visible_edges.source_id = reached.tower_id
    )
    select count(*) into reached_total from reached;

    if reached_total = tower_total then
        raise notice 'Tower LOS component invariant holds: % live towers, % directed visible links',
            tower_total,
            visible_edge_total;
        return;
    end if;

    with recursive visible_edges as (
        -- Repeat the walk for the diagnostic list so the exception tells the
        -- operator exactly which towers broke the invariant.
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
    start_tower as (
        select min(tower_id) as tower_id
        from mesh_towers
    ),
    reached(tower_id) as (
        select tower_id
        from start_tower

        union

        select visible_edges.target_id
        from reached
        join visible_edges on visible_edges.source_id = reached.tower_id
    )
    select string_agg(
        format('%s:%s:%s', t.tower_id, t.source, t.h3::text),
        ', '
        order by t.tower_id
    )
    into disconnected_towers
    from mesh_towers t
    left join reached on reached.tower_id = t.tower_id
    where reached.tower_id is null;

    raise exception
        'Tower LOS component invariant failed: reached % of % live tower(s) through % directed visible link(s). Disconnected towers: %',
        reached_total,
        tower_total,
        visible_edge_total,
        disconnected_towers;
end;
$$;
