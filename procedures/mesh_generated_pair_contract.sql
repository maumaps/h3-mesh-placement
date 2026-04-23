set client_min_messages = notice;

-- Contract close generated tower pairs when one synthetic H3 preserves their combined cached LOS role.
-- This is intentionally separate from population-anchor contraction: both towers here are generated route-like
-- nodes, so deletion is allowed only after a replacement cell sees every non-population neighbor that either
-- source tower saw in mesh_los_cache.
do
$$
declare
    enabled boolean := true;
    max_distance double precision := 100000;
    mast_height double precision := 28;
    frequency double precision := 868000000;
    merge_distance double precision := 10000;
    generated_sources constant text[] := array['route', 'cluster_slim', 'bridge', 'coarse'];
    target_sources constant text[] := array['population', 'route', 'cluster_slim', 'bridge', 'coarse'];
    pair record;
    replacement record;
    current_component_count integer;
    actual_component_count integer;
    removed_generated integer := 0;
begin
    if to_regclass('mesh_pipeline_settings') is null then
        raise notice 'mesh_pipeline_settings missing, skipping generated pair contraction';
        return;
    end if;

    select coalesce((
        select value::boolean
        from mesh_pipeline_settings
        where setting = 'enable_generated_pair_contract'
    ), true)
    into enabled;

    if not enabled then
        raise notice 'Generated pair contraction disabled by mesh_pipeline_settings.enable_generated_pair_contract';
        return;
    end if;

    if to_regclass('mesh_towers') is null
       or to_regclass('mesh_surface_h3_r8') is null
       or to_regclass('mesh_los_cache') is null then
        raise notice 'Required placement tables missing, skipping generated pair contraction';
        return;
    end if;

    select coalesce((
        select value::double precision
        from mesh_pipeline_settings
        where setting = 'max_los_distance_m'
    ), 100000)
    into max_distance;

    select coalesce((
        select value::double precision
        from mesh_pipeline_settings
        where setting = 'mast_height_m'
    ), 28)
    into mast_height;

    select coalesce((
        select value::double precision
        from mesh_pipeline_settings
        where setting = 'frequency_hz'
    ), 868000000)
    into frequency;

    select greatest(coalesce((
        select value::double precision
        from mesh_pipeline_settings
        where setting = 'generated_tower_merge_distance_m'
    ), 10000), 0)
    into merge_distance;

    if merge_distance <= 0 then
        raise notice 'Generated pair contraction disabled because generated_tower_merge_distance_m <= 0';
        return;
    end if;

    create temporary table if not exists mesh_generated_pair_contract_deleted_h3 (
        h3 h3index primary key
    ) on commit drop;
    truncate mesh_generated_pair_contract_deleted_h3;

    create temporary table if not exists mesh_generated_pair_contract_affected_points (
        centroid_geog public.geography not null
    ) on commit drop;
    truncate mesh_generated_pair_contract_affected_points;

    for pair in
        select
            a.tower_id as keep_tower_id,
            a.h3 as keep_h3,
            a.centroid_geog as keep_centroid_geog,
            b.tower_id as remove_tower_id,
            b.h3 as remove_h3,
            b.centroid_geog as remove_centroid_geog,
            ST_Centroid(ST_Collect(a.h3::geometry, b.h3::geometry))::public.geography as midpoint_geog
        from mesh_towers a
        join mesh_towers b
          on a.tower_id < b.tower_id
         and a.source = any(generated_sources)
         and b.source = any(generated_sources)
         and ST_DWithin(a.centroid_geog, b.centroid_geog, merge_distance)
        order by ST_Distance(a.centroid_geog, b.centroid_geog), a.tower_id, b.tower_id
    loop
        -- A previous contraction in this transaction may have removed or moved one side.
        if not exists (select 1 from mesh_towers where tower_id = pair.keep_tower_id and h3 = pair.keep_h3)
           or not exists (select 1 from mesh_towers where tower_id = pair.remove_tower_id and h3 = pair.remove_h3) then
            continue;
        end if;

        with required_neighbors as materialized (
            select distinct
                nb.tower_id,
                nb.h3
            from mesh_towers nb
            join mesh_los_cache keep_link
              on keep_link.src_h3 = least(pair.keep_h3, nb.h3)
             and keep_link.dst_h3 = greatest(pair.keep_h3, nb.h3)
             and keep_link.mast_height_src = mast_height
             and keep_link.mast_height_dst = mast_height
             and keep_link.frequency_hz = frequency
             and keep_link.clearance > 0
             and keep_link.distance_m <= max_distance
            where nb.tower_id not in (pair.keep_tower_id, pair.remove_tower_id)
              and nb.source <> 'population'

            union

            select distinct
                nb.tower_id,
                nb.h3
            from mesh_towers nb
            join mesh_los_cache remove_link
              on remove_link.src_h3 = least(pair.remove_h3, nb.h3)
             and remove_link.dst_h3 = greatest(pair.remove_h3, nb.h3)
             and remove_link.mast_height_src = mast_height
             and remove_link.mast_height_dst = mast_height
             and remove_link.frequency_hz = frequency
             and remove_link.clearance > 0
             and remove_link.distance_m <= max_distance
            where nb.tower_id not in (pair.keep_tower_id, pair.remove_tower_id)
              and nb.source <> 'population'
        ),
        synthetic_candidates as (
            select
                s.h3,
                s.centroid_geog,
                coalesce(nullif(s.visible_population, 0), s.population_70km, s.population, 0) as score,
                coalesce(s.has_building, false) as has_building,
                coalesce(s.building_count, 0) as building_count,
                ST_Distance(s.centroid_geog, pair.midpoint_geog) as midpoint_distance_m
            from mesh_surface_h3_r8 s
            where s.is_in_boundaries
              and s.has_road
              and not s.is_in_unfit_area
              and s.h3 not in (pair.keep_h3, pair.remove_h3)
              and ST_DWithin(s.centroid_geog, pair.midpoint_geog, merge_distance)
              and not exists (
                    select 1
                    from mesh_towers existing
                    where existing.h3 = s.h3
                      and existing.tower_id not in (pair.keep_tower_id, pair.remove_tower_id)
                )
              and not exists (
                    select 1
                    from required_neighbors required
                    where not exists (
                            select 1
                            from mesh_los_cache candidate_link
                            where candidate_link.src_h3 = least(s.h3, required.h3)
                              and candidate_link.dst_h3 = greatest(s.h3, required.h3)
                              and candidate_link.mast_height_src = mast_height
                              and candidate_link.mast_height_dst = mast_height
                              and candidate_link.frequency_hz = frequency
                              and candidate_link.clearance > 0
                              and candidate_link.distance_m <= max_distance
                        )
                )
        )
        select *
        into replacement
        from synthetic_candidates
        order by
            has_building desc,
            score desc,
            building_count desc,
            midpoint_distance_m asc,
            h3 asc
        limit 1;

        if replacement.h3 is null then
            continue;
        end if;

        -- Preserve the current live LOS component count: replacing a close
        -- pair with one synthetic H3 must not split the graph that route
        -- building already stitched together.
        with recursive current_towers as (
            select
                tower_id,
                h3
            from mesh_towers
        ),
        current_visible_edges as (
            select distinct
                src.tower_id as source_id,
                dst.tower_id as target_id
            from current_towers src
            join current_towers dst on dst.tower_id <> src.tower_id
            join mesh_los_cache link
              on link.src_h3 = least(src.h3, dst.h3)
             and link.dst_h3 = greatest(src.h3, dst.h3)
             and link.mast_height_src = mast_height
             and link.mast_height_dst = mast_height
             and link.frequency_hz = frequency
             and link.clearance > 0
             and link.distance_m <= max_distance
        ),
        current_walk(root_id, tower_id, path) as (
            select
                current_towers.tower_id as root_id,
                current_towers.tower_id,
                array[current_towers.tower_id]
            from current_towers

            union

            select
                current_walk.root_id,
                current_visible_edges.target_id,
                current_walk.path || current_visible_edges.target_id
            from current_walk
            join current_visible_edges on current_visible_edges.source_id = current_walk.tower_id
            where not current_visible_edges.target_id = any(current_walk.path)
        ),
        current_components as (
            select
                tower_id,
                min(root_id) as component_id
            from current_walk
            group by tower_id
        )
        select count(distinct component_id)
        into current_component_count
        from current_components;

        begin
            if to_regclass('mesh_tower_wiggle_queue') is not null then
                delete from mesh_tower_wiggle_queue
                where tower_id = pair.remove_tower_id;

                update mesh_tower_wiggle_queue
                set is_dirty = true
                where tower_id = pair.keep_tower_id;
            end if;

            delete from mesh_towers
            where tower_id = pair.remove_tower_id;

            update mesh_towers
            set h3 = replacement.h3
            where tower_id = pair.keep_tower_id;

            with recursive current_towers as (
                select
                    tower_id,
                    h3
                from mesh_towers
            ),
            current_visible_edges as (
                select distinct
                    src.tower_id as source_id,
                    dst.tower_id as target_id
                from current_towers src
                join current_towers dst on dst.tower_id <> src.tower_id
                join mesh_los_cache link
                  on link.src_h3 = least(src.h3, dst.h3)
                 and link.dst_h3 = greatest(src.h3, dst.h3)
                 and link.mast_height_src = mast_height
                 and link.mast_height_dst = mast_height
                 and link.frequency_hz = frequency
                 and link.clearance > 0
                 and link.distance_m <= max_distance
            ),
            current_walk(root_id, tower_id, path) as (
                select
                    current_towers.tower_id as root_id,
                    current_towers.tower_id,
                    array[current_towers.tower_id]
                from current_towers

                union

                select
                    current_walk.root_id,
                    current_visible_edges.target_id,
                    current_walk.path || current_visible_edges.target_id
                from current_walk
                join current_visible_edges on current_visible_edges.source_id = current_walk.tower_id
                where not current_visible_edges.target_id = any(current_walk.path)
            ),
            current_components as (
                select
                    tower_id,
                    min(root_id) as component_id
                from current_walk
                group by tower_id
            )
            select count(distinct component_id)
            into actual_component_count
            from current_components;

            if actual_component_count > current_component_count then
                raise exception 'mesh_generated_pair_contract_split: %/% would grow LOS components from % to %',
                    pair.keep_tower_id,
                    pair.remove_tower_id,
                    current_component_count,
                    actual_component_count;
            end if;

            insert into mesh_generated_pair_contract_deleted_h3 (h3)
            values (pair.keep_h3), (pair.remove_h3)
            on conflict do nothing;

            insert into mesh_generated_pair_contract_affected_points (centroid_geog)
            values (pair.keep_centroid_geog), (pair.remove_centroid_geog), (replacement.centroid_geog);

            update mesh_surface_h3_r8 s
            set has_tower = false,
                clearance = null,
                path_loss = null,
                visible_population = null,
                visible_uncovered_population = null
            where s.h3 in (pair.keep_h3, pair.remove_h3)
              and not exists (
                    select 1
                    from mesh_towers mt
                    where mt.h3 = s.h3
                );

            update mesh_surface_h3_r8
            set has_tower = true,
                clearance = null,
                path_loss = null,
                visible_population = null,
                visible_uncovered_population = 0,
                distance_to_closest_tower = 0
            where h3 = replacement.h3;

            removed_generated := removed_generated + 1;

            raise notice 'Contracted generated towers %/% into tower % at %',
                pair.keep_tower_id,
                pair.remove_tower_id,
                pair.keep_tower_id,
                replacement.h3;
        exception
            when raise_exception then
                if sqlerrm like 'mesh_generated_pair_contract_split:%' then
                    raise notice 'Skipping generated tower pair %/% because contracting it would split the live LOS graph',
                        pair.keep_tower_id,
                        pair.remove_tower_id;
                else
                    raise;
                end if;
        end;
    end loop;

    if removed_generated = 0 then
        raise notice 'Generated pair contraction found no contractible pairs';
        return;
    end if;

    with affected_cells as materialized (
        select
            s.h3,
            s.centroid_geog
        from mesh_surface_h3_r8 s
        where exists (
            select 1
            from mesh_generated_pair_contract_affected_points affected
            where ST_DWithin(s.centroid_geog, affected.centroid_geog, max_distance)
        )
    ),
    recomputed_distances as (
        select
            ac.h3,
            min(ST_Distance(ac.centroid_geog, t.centroid_geog)) as distance_m
        from affected_cells ac
        join mesh_towers t on true
        group by ac.h3
    )
    update mesh_surface_h3_r8 s
    set distance_to_closest_tower = rd.distance_m,
        clearance = null,
        path_loss = null,
        visible_population = null,
        visible_uncovered_population = case
            when s.has_tower then 0
            else null
        end
    from recomputed_distances rd
    where s.h3 = rd.h3;

    if to_regclass('mesh_tower_wiggle_queue') is not null then
        update mesh_tower_wiggle_queue q
        set is_dirty = true
        from mesh_towers t
        where t.tower_id = q.tower_id
          and t.source = any(target_sources)
          and exists (
                select 1
                from mesh_generated_pair_contract_affected_points affected
                where ST_DWithin(t.centroid_geog, affected.centroid_geog, max_distance)
            );
    end if;

    raise notice 'Generated pair contraction removed % generated tower(s)', removed_generated;
end;
$$;
