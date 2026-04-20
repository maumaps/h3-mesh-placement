set client_min_messages = notice;

-- Locally reroute two-relay generated route chains when cached LOS proves a better pair exists.
do
$$
declare
    enabled boolean := true;
    max_distance double precision := 80000;
    mast_height double precision := 28;
    frequency double precision := 868000000;
    candidate_limit integer := 512;
    max_moves integer := 32;
    moved_count integer := 0;
    generated_sources constant text[] := array['route', 'cluster_slim', 'bridge', 'coarse'];
    chain record;
    replacement record;
begin
    if to_regclass('mesh_pipeline_settings') is null then
        raise notice 'mesh_pipeline_settings missing, skipping route segment reroute';
        return;
    end if;

    select coalesce((
        select value::boolean
        from mesh_pipeline_settings
        where setting = 'enable_route_segment_reroute'
    ), true)
    into enabled;

    if not enabled then
        raise notice 'Route segment reroute disabled by mesh_pipeline_settings.enable_route_segment_reroute';
        return;
    end if;

    if to_regclass('mesh_towers') is null
       or to_regclass('mesh_surface_h3_r8') is null
       or to_regclass('mesh_los_cache') is null then
        raise notice 'Required placement tables missing, skipping route segment reroute';
        return;
    end if;

    select coalesce((
        select value::double precision
        from mesh_pipeline_settings
        where setting = 'max_los_distance_m'
    ), 80000)
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
        select value::integer
        from mesh_pipeline_settings
        where setting = 'route_segment_reroute_candidate_limit'
    ), 512), 1)
    into candidate_limit;

    select greatest(coalesce((
        select value::integer
        from mesh_pipeline_settings
        where setting = 'route_segment_reroute_max_moves'
    ), 32), 0)
    into max_moves;

    if max_moves = 0 then
        raise notice 'Route segment reroute disabled because route_segment_reroute_max_moves <= 0';
        return;
    end if;

    -- Store affected points so the distance/reception cache can be invalidated around moved route relays.
    create temporary table if not exists mesh_route_segment_reroute_affected_points (
        centroid_geog public.geography not null
    ) on commit drop;
    truncate mesh_route_segment_reroute_affected_points;

    for chain in
        with visible_neighbors as materialized (
            -- Build the undirected cached LOS graph between current towers.
            select distinct
                tower.tower_id,
                tower.h3,
                neighbor.tower_id as neighbor_tower_id,
                neighbor.h3 as neighbor_h3
            from mesh_towers tower
            join mesh_towers neighbor on neighbor.tower_id <> tower.tower_id
            join mesh_los_cache link
              on link.src_h3 = least(tower.h3, neighbor.h3)
             and link.dst_h3 = greatest(tower.h3, neighbor.h3)
             and link.mast_height_src = mast_height
             and link.mast_height_dst = mast_height
             and link.frequency_hz = frequency
             and link.clearance > 0
            where neighbor.source <> 'population'
        ), neighbor_counts as materialized (
            -- Only pure two-neighbor generated relays are safe to pair-reroute.
            select
                tower_id,
                count(*) as neighbor_count
            from visible_neighbors
            group by tower_id
        ), candidate_chains as materialized (
            -- Find endpoint -> left relay -> right relay -> endpoint chains.
            select
                left_endpoint.tower_id as left_endpoint_id,
                left_endpoint.h3 as left_endpoint_h3,
                left_endpoint.centroid_geog as left_endpoint_geog,
                left_relay.tower_id as left_relay_id,
                left_relay.h3 as left_relay_h3,
                left_relay.centroid_geog as left_relay_geog,
                right_relay.tower_id as right_relay_id,
                right_relay.h3 as right_relay_h3,
                right_relay.centroid_geog as right_relay_geog,
                right_endpoint.tower_id as right_endpoint_id,
                right_endpoint.h3 as right_endpoint_h3,
                right_endpoint.centroid_geog as right_endpoint_geog,
                coalesce(left_surface.building_count, 0) + coalesce(right_surface.building_count, 0) as current_building_count,
                coalesce(left_surface.population_70km, 0) + coalesce(right_surface.population_70km, 0) as current_population_70km,
                coalesce(left_surface.population, 0) + coalesce(right_surface.population, 0) as current_local_population
            from mesh_towers left_relay
            join mesh_towers right_relay
              on left_relay.tower_id < right_relay.tower_id
             and left_relay.source = any(generated_sources)
             and right_relay.source = any(generated_sources)
            join neighbor_counts left_count
              on left_count.tower_id = left_relay.tower_id
             and left_count.neighbor_count = 2
            join neighbor_counts right_count
              on right_count.tower_id = right_relay.tower_id
             and right_count.neighbor_count = 2
            join visible_neighbors middle_link
              on middle_link.tower_id = left_relay.tower_id
             and middle_link.neighbor_tower_id = right_relay.tower_id
            join visible_neighbors left_link
              on left_link.tower_id = left_relay.tower_id
             and left_link.neighbor_tower_id <> right_relay.tower_id
            join mesh_towers left_endpoint on left_endpoint.tower_id = left_link.neighbor_tower_id
            join visible_neighbors right_link
              on right_link.tower_id = right_relay.tower_id
             and right_link.neighbor_tower_id <> left_relay.tower_id
            join mesh_towers right_endpoint on right_endpoint.tower_id = right_link.neighbor_tower_id
            join mesh_surface_h3_r8 left_surface on left_surface.h3 = left_relay.h3
            join mesh_surface_h3_r8 right_surface on right_surface.h3 = right_relay.h3
            where left_endpoint.tower_id <> right_endpoint.tower_id
              and left_endpoint.source <> 'population'
              and right_endpoint.source <> 'population'
        )
        select *
        from candidate_chains
        order by
            current_building_count asc,
            current_population_70km asc,
            left_relay_id,
            right_relay_id
    loop
        exit when moved_count >= max_moves;

        if not exists (select 1 from mesh_towers where tower_id = chain.left_relay_id and h3 = chain.left_relay_h3)
           or not exists (select 1 from mesh_towers where tower_id = chain.right_relay_id and h3 = chain.right_relay_h3) then
            continue;
        end if;

        with left_candidates as materialized (
            -- Candidate first relays must see the left endpoint and be free except for the current left relay.
            select
                surface.h3,
                surface.centroid_geog,
                coalesce(surface.has_building, false) as has_building,
                coalesce(surface.building_count, 0) as building_count,
                coalesce(surface.population, 0) as local_population,
                coalesce(surface.population_70km, 0) as population_70km
            from mesh_los_cache link
            join mesh_surface_h3_r8 surface
              on surface.h3 = case when link.src_h3 = chain.left_endpoint_h3 then link.dst_h3 else link.src_h3 end
            where (link.src_h3 = chain.left_endpoint_h3 or link.dst_h3 = chain.left_endpoint_h3)
              and link.mast_height_src = mast_height
              and link.mast_height_dst = mast_height
              and link.frequency_hz = frequency
              and link.clearance > 0
              and surface.is_in_boundaries
              and surface.has_road
              and not surface.is_in_unfit_area
              and surface.h3 not in (chain.left_endpoint_h3, chain.right_endpoint_h3, chain.right_relay_h3)
              and ST_DWithin(surface.centroid_geog, chain.left_endpoint_geog, max_distance)
              and not exists (
                    select 1
                    from mesh_towers existing
                    where existing.h3 = surface.h3
                      and existing.tower_id <> chain.left_relay_id
                )
            order by
                coalesce(surface.has_building, false) desc,
                coalesce(surface.building_count, 0) desc,
                coalesce(surface.population_70km, 0) desc,
                coalesce(surface.population, 0) desc,
                surface.h3
            limit candidate_limit
        ), right_candidates as materialized (
            -- Candidate second relays must see the right endpoint and be free except for the current right relay.
            select
                surface.h3,
                surface.centroid_geog,
                coalesce(surface.has_building, false) as has_building,
                coalesce(surface.building_count, 0) as building_count,
                coalesce(surface.population, 0) as local_population,
                coalesce(surface.population_70km, 0) as population_70km
            from mesh_los_cache link
            join mesh_surface_h3_r8 surface
              on surface.h3 = case when link.src_h3 = chain.right_endpoint_h3 then link.dst_h3 else link.src_h3 end
            where (link.src_h3 = chain.right_endpoint_h3 or link.dst_h3 = chain.right_endpoint_h3)
              and link.mast_height_src = mast_height
              and link.mast_height_dst = mast_height
              and link.frequency_hz = frequency
              and link.clearance > 0
              and surface.is_in_boundaries
              and surface.has_road
              and not surface.is_in_unfit_area
              and surface.h3 not in (chain.left_endpoint_h3, chain.right_endpoint_h3, chain.left_relay_h3)
              and ST_DWithin(surface.centroid_geog, chain.right_endpoint_geog, max_distance)
              and not exists (
                    select 1
                    from mesh_towers existing
                    where existing.h3 = surface.h3
                      and existing.tower_id <> chain.right_relay_id
                )
            order by
                coalesce(surface.has_building, false) desc,
                coalesce(surface.building_count, 0) desc,
                coalesce(surface.population_70km, 0) desc,
                coalesce(surface.population, 0) desc,
                surface.h3
            limit candidate_limit
        ), chain_candidates as materialized (
            -- A replacement pair is valid only when the two new relay cells also see each other.
            select
                left_candidate.h3 as left_h3,
                left_candidate.centroid_geog as left_geog,
                right_candidate.h3 as right_h3,
                right_candidate.centroid_geog as right_geog,
                left_candidate.building_count + right_candidate.building_count as building_count,
                left_candidate.population_70km + right_candidate.population_70km as population_70km,
                left_candidate.local_population + right_candidate.local_population as local_population,
                ST_Distance(left_candidate.centroid_geog, right_candidate.centroid_geog) as middle_distance_m
            from left_candidates left_candidate
            join right_candidates right_candidate on right_candidate.h3 <> left_candidate.h3
            join mesh_los_cache middle_link
              on middle_link.src_h3 = least(left_candidate.h3, right_candidate.h3)
             and middle_link.dst_h3 = greatest(left_candidate.h3, right_candidate.h3)
             and middle_link.mast_height_src = mast_height
             and middle_link.mast_height_dst = mast_height
             and middle_link.frequency_hz = frequency
             and middle_link.clearance > 0
        )
        select *
        into replacement
        from chain_candidates candidate
        where candidate.left_h3 <> chain.right_relay_h3
          and candidate.right_h3 <> chain.left_relay_h3
          and (
                candidate.building_count > chain.current_building_count
             or (candidate.building_count = chain.current_building_count
                 and candidate.population_70km > chain.current_population_70km)
             or (candidate.building_count = chain.current_building_count
                 and candidate.population_70km = chain.current_population_70km
                 and candidate.local_population > chain.current_local_population)
          )
        order by
            candidate.building_count desc,
            candidate.population_70km desc,
            candidate.local_population desc,
            candidate.middle_distance_m asc,
            candidate.left_h3,
            candidate.right_h3
        limit 1;

        if replacement.left_h3 is null then
            continue;
        end if;

        insert into mesh_route_segment_reroute_affected_points (centroid_geog)
        values
            (chain.left_relay_geog),
            (chain.right_relay_geog),
            (replacement.left_geog),
            (replacement.right_geog);

        update mesh_towers
        set h3 = case
                when tower_id = chain.left_relay_id then replacement.left_h3
                when tower_id = chain.right_relay_id then replacement.right_h3
                else h3
            end,
            recalculation_count = recalculation_count + 1
        where tower_id in (chain.left_relay_id, chain.right_relay_id);

        update mesh_surface_h3_r8 surface
        set has_tower = false,
            clearance = null,
            path_loss = null,
            visible_population = null,
            visible_uncovered_population = null
        where surface.h3 in (chain.left_relay_h3, chain.right_relay_h3)
          and not exists (
                select 1
                from mesh_towers tower
                where tower.h3 = surface.h3
            );

        update mesh_surface_h3_r8 surface
        set has_tower = true,
            clearance = null,
            path_loss = null,
            visible_population = null,
            visible_uncovered_population = 0,
            distance_to_closest_tower = 0
        where surface.h3 in (replacement.left_h3, replacement.right_h3);

        if to_regclass('mesh_tower_wiggle_queue') is not null then
            update mesh_tower_wiggle_queue
            set is_dirty = true
            where tower_id in (chain.left_relay_id, chain.right_relay_id);
        end if;

        moved_count := moved_count + 1;

        raise notice 'Rerouted route segment % -> % via towers %/% from %/% to %/%',
            chain.left_endpoint_id,
            chain.right_endpoint_id,
            chain.left_relay_id,
            chain.right_relay_id,
            chain.left_relay_h3,
            chain.right_relay_h3,
            replacement.left_h3,
            replacement.right_h3;
    end loop;

    if moved_count = 0 then
        raise notice 'Route segment reroute found no improving two-relay chains';
        return;
    end if;

    with affected_cells as materialized (
        -- Invalidate local surface metrics around old and new relay positions.
        select surface.h3, surface.centroid_geog
        from mesh_surface_h3_r8 surface
        where exists (
            select 1
            from mesh_route_segment_reroute_affected_points affected
            where ST_DWithin(surface.centroid_geog, affected.centroid_geog, max_distance)
        )
    ), recomputed_distances as (
        -- Recompute nearest-tower distance only in the affected area.
        select
            affected_cells.h3,
            min(ST_Distance(affected_cells.centroid_geog, tower.centroid_geog)) as distance_m
        from affected_cells
        join mesh_towers tower on true
        group by affected_cells.h3
    )
    update mesh_surface_h3_r8 surface
    set distance_to_closest_tower = recomputed_distances.distance_m,
        clearance = null,
        path_loss = null,
        visible_population = null,
        visible_uncovered_population = case
            when surface.has_tower then 0
            else null
        end
    from recomputed_distances
    where surface.h3 = recomputed_distances.h3;

    if to_regclass('mesh_tower_wiggle_queue') is not null then
        update mesh_tower_wiggle_queue queue
        set is_dirty = true
        from mesh_towers tower
        where tower.tower_id = queue.tower_id
          and tower.source = any(generated_sources)
          and exists (
                select 1
                from mesh_route_segment_reroute_affected_points affected
                where ST_DWithin(tower.centroid_geog, affected.centroid_geog, max_distance)
            );
    end if;

    raise notice 'Route segment reroute improved % two-relay chain(s)', moved_count;
end;
$$;
