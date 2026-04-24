set client_min_messages = notice;

drop procedure if exists mesh_tower_wiggle();
drop function if exists mesh_tower_wiggle();

-- Recenter one coarse/route/bridge/cluster-slim tower toward denser visible population while preserving current LOS neighbors.
create or replace function mesh_tower_wiggle(reset_run boolean default false)
    returns integer
    language plpgsql
as
$$
declare
    max_distance double precision := 100000;
    separation_default constant double precision := 0;
    generated_tower_merge_distance double precision := 10000;
    mast_height double precision := 28;
    frequency double precision := 868000000;
    target_sources constant text[] := array['population', 'route', 'cluster_slim', 'bridge', 'coarse'];
    prunable_sources constant text[] := array['route', 'cluster_slim', 'bridge', 'coarse'];
    wiggle_candidate_limit integer := 256;
    processed integer := 0;
    anchor record;
    best record;
    merge_target record;
    old_centroid public.geography;
    new_centroid public.geography;
    current_component_count integer;
    actual_component_count integer;
    prune_rejected boolean := false;
begin
    if to_regclass('mesh_pipeline_settings') is not null then
        -- Read the same RF dimensions used by cache fill and route cleanup so
        -- cached-neighbor preservation checks use the active LOS cache rows.
        select coalesce((
            select value::double precision
            from mesh_pipeline_settings
            where setting = 'max_los_distance_m'
        ), 100000)
        into max_distance;

        select greatest(coalesce((
            select value::integer
            from mesh_pipeline_settings
            where setting = 'wiggle_candidate_limit'
        ), 256), 1)
        into wiggle_candidate_limit;

        select greatest(coalesce((
            select value::double precision
            from mesh_pipeline_settings
            where setting = 'generated_tower_merge_distance_m'
        ), 10000), 0)
        into generated_tower_merge_distance;

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
    end if;

    if to_regclass('mesh_towers') is null then
        raise notice 'mesh_towers table missing, skipping tower wiggle';
        return 0;
    end if;

    if to_regclass('mesh_surface_h3_r8') is null then
        raise notice 'mesh_surface_h3_r8 table missing, skipping tower wiggle';
        return 0;
    end if;

    -- Normalize missing counters so increments never stay null.
    update mesh_towers
    set recalculation_count = 0
    where recalculation_count is null
      and source = any(target_sources);

    -- Track pending wiggle recalculations for movable towers.
    if to_regclass('mesh_tower_wiggle_queue') is null then
        -- Queue of towers that still need wiggle evaluation.
        create table if not exists mesh_tower_wiggle_queue (
            tower_id integer primary key,
            is_dirty boolean not null default true
        );
    end if;

    if reset_run then
        delete from mesh_tower_wiggle_queue;
    end if;

    -- Seed the queue with eligible towers that are not yet tracked.
    insert into mesh_tower_wiggle_queue (tower_id, is_dirty)
    select t.tower_id, true
    from mesh_towers t
    where t.source = any(target_sources)
      and not exists (
            select 1
            from mesh_tower_wiggle_queue q
            where q.tower_id = t.tower_id
        );

    if reset_run then
        update mesh_tower_wiggle_queue q
        set is_dirty = true
        from mesh_towers t
        where t.tower_id = q.tower_id
          and t.source = any(target_sources);
    end if;

    if not exists (select 1 from mesh_tower_wiggle_queue q where q.is_dirty) then
        raise notice 'Wiggle idle: no dirty towers remain';
        return 0;
    end if;

    -- Pull the dirtiest tower: lowest recalculation count, then most visible population.
    select
        q.tower_id,
        t.h3,
        t.source,
        t.centroid_geog,
        coalesce(t.recalculation_count, 0) as recalculation_count,
        coalesce(s.visible_population, s.population, 0) as priority_population,
        coalesce(s.min_distance_to_closest_tower, separation_default) as min_distance
    into anchor
    from mesh_tower_wiggle_queue q
    join mesh_towers t on t.tower_id = q.tower_id
    left join mesh_surface_h3_r8 s on s.h3 = t.h3
    where q.is_dirty
      and t.source = any(target_sources)
    order by coalesce(t.recalculation_count, 0) asc,
             coalesce(s.visible_population, s.population, 0) desc,
             t.tower_id asc
    limit 1
    for update of q skip locked;

    if not found then
        raise notice 'Wiggle idle: no dirty towers remain';
        return 0;
    end if;

    raise notice 'Wiggle evaluating tower % at % (recalcs %, visible_pop %)',
        anchor.tower_id,
        anchor.h3,
        coalesce(anchor.recalculation_count, 0),
        coalesce(anchor.priority_population, 0);

    -- Identify every tower currently visible from the anchor tower using only
    -- the precious LOS cache. Wiggle must not start fresh terrain LOS
    -- calculations while it is doing interactive local refinement.
    with visible_neighbors as materialized (
        select
            nb.tower_id,
            nb.h3,
            nb.centroid_geog
        from mesh_towers nb
        join mesh_los_cache mlc
          on mlc.src_h3 = least(anchor.h3, nb.h3)
         and mlc.dst_h3 = greatest(anchor.h3, nb.h3)
         and mlc.mast_height_src = mast_height
         and mlc.mast_height_dst = mast_height
         and mlc.frequency_hz = frequency
         and mlc.clearance > 0
         and mlc.distance_m <= max_distance
        where nb.tower_id <> anchor.tower_id
          and ST_DWithin(nb.centroid_geog, anchor.centroid_geog, max_distance)
    ),
    -- Build a local candidate pool before candidate-to-neighbor LOS checks.
    -- The preservation checks use only mesh_los_cache primary-key lookups, so
    -- we can test the whole 100 km pool and avoid missing mountain relay moves
    -- that are not in the first few hundred demand-ranked cells.
    candidate_pool as materialized (
        select
            s.h3,
            s.centroid_geog,
            coalesce(nullif(s.visible_population, 0), s.population_70km, s.population, 0) as candidate_population,
            coalesce(s.has_building, false) as has_building,
            coalesce(s.building_count, 0) as building_count,
            case when s.h3 = anchor.h3 then 0 else 1 end as current_rank,
            ST_Distance(s.centroid_geog, anchor.centroid_geog) as anchor_distance_m,
            coalesce(
                ST_Distance(
                    s.centroid_geog,
                    (
                        select ST_Centroid(ST_Collect(vn.h3::geometry))::public.geography
                        from visible_neighbors vn
                    )
                ),
                ST_Distance(s.centroid_geog, anchor.centroid_geog)
            ) as neighbor_centroid_distance_m
        from mesh_surface_h3_r8 s
        where s.is_in_boundaries
          and s.has_road
          and not s.is_in_unfit_area
          and ST_DWithin(s.centroid_geog, anchor.centroid_geog, max_distance)
          and (s.has_tower is not true or s.h3 = anchor.h3)
          and not exists (
                select 1
                from mesh_towers mt
                where mt.tower_id <> anchor.tower_id
                  and ST_DWithin(s.centroid_geog, mt.centroid_geog, anchor.min_distance)
            )
    ),
    initial_candidates as materialized (
        select
            cp.h3,
            cp.centroid_geog,
            cp.candidate_population,
            cp.has_building,
            cp.building_count,
            cp.current_rank
        from candidate_pool cp
    ),
    -- Enumerate candidate cells that keep current visible neighbors in LOS.
    viable_candidates as materialized (
        select
            ic.h3,
            ic.centroid_geog,
            ic.candidate_population,
            ic.has_building,
            ic.building_count,
            ic.current_rank
        from initial_candidates ic
        where not exists (
                select 1
                from visible_neighbors nb
                where not ST_DWithin(ic.centroid_geog, nb.centroid_geog, max_distance)
                   or not exists (
                        select 1
                        from mesh_los_cache mlc
                        where mlc.src_h3 = least(ic.h3, nb.h3)
                          and mlc.dst_h3 = greatest(ic.h3, nb.h3)
                          and mlc.mast_height_src = mast_height
                          and mlc.mast_height_dst = mast_height
                          and mlc.frequency_hz = frequency
                          and mlc.clearance > 0
                          and mlc.distance_m <= max_distance
                    )
            )
    ),
    marginal_candidates as materialized (
        select vc.*
        from viable_candidates vc
        order by
            vc.current_rank asc,
            vc.candidate_population desc,
            vc.has_building desc,
            vc.building_count desc,
            vc.h3
        limit wiggle_candidate_limit
    ),
    -- Estimate diversity with cached marginal population only after the cheap
    -- LOS-neighbor preservation filter. This avoids fresh terrain LOS inside
    -- wiggle and prevents adjacent route relays from optimizing for the same
    -- already-served settlement when cached population links are available.
    candidate_cached_population as materialized (
        select
            vc.h3,
            coalesce(sum(pop.population), 0) as cached_visible_population,
            coalesce(sum(pop.population) filter (where covered.pop_h3 is null), 0) as cached_marginal_population
        from marginal_candidates vc
        join mesh_surface_h3_r8 pop
          on pop.population > 0
         and ST_DWithin(vc.centroid_geog, pop.centroid_geog, max_distance)
        join mesh_los_cache candidate_pop_link
          on candidate_pop_link.src_h3 = least(vc.h3, pop.h3)
         and candidate_pop_link.dst_h3 = greatest(vc.h3, pop.h3)
         and candidate_pop_link.mast_height_src = mast_height
         and candidate_pop_link.mast_height_dst = mast_height
         and candidate_pop_link.frequency_hz = frequency
         and candidate_pop_link.clearance > 0
         and candidate_pop_link.distance_m <= max_distance
        left join lateral (
            select pop.h3 as pop_h3
            from mesh_towers other_tower
            join mesh_los_cache other_pop_link
              on other_pop_link.src_h3 = least(other_tower.h3, pop.h3)
             and other_pop_link.dst_h3 = greatest(other_tower.h3, pop.h3)
             and other_pop_link.mast_height_src = mast_height
             and other_pop_link.mast_height_dst = mast_height
             and other_pop_link.frequency_hz = frequency
             and other_pop_link.clearance > 0
             and other_pop_link.distance_m <= max_distance
            where other_tower.tower_id <> anchor.tower_id
              and ST_DWithin(other_tower.centroid_geog, pop.centroid_geog, max_distance)
            limit 1
        ) covered on true
        group by vc.h3
    ),
    candidate_visible_population as (
        select
            vc.h3,
            vc.candidate_population as visible_population,
            coalesce(ccp.cached_visible_population, 0) as cached_visible_population,
            coalesce(ccp.cached_marginal_population, 0) as cached_marginal_population,
            vc.has_building,
            vc.building_count,
            vc.current_rank
        from marginal_candidates vc
        left join candidate_cached_population ccp on ccp.h3 = vc.h3
    )
    select
        cv.h3,
        cv.visible_population,
        cv.has_building,
        cv.building_count
    into best
    from candidate_visible_population cv
    order by
        case when cv.cached_visible_population > 0 then 0 else 1 end,
        cv.cached_marginal_population desc,
        cv.visible_population desc,
        cv.has_building desc,
        cv.building_count desc,
        cv.current_rank asc,
        cv.h3
    limit 1;

    if best.h3 is null then
        best.h3 := anchor.h3;
        best.visible_population := anchor.priority_population;
    end if;

    -- Candidate scoring can run in parallel workers, but the graph mutation and
    -- component check must stay serialized so two concurrent moves cannot both
    -- validate against the same stale tower set.
    perform pg_advisory_xact_lock(hashtext('mesh_tower_wiggle_write'));

    with recursive visible_edges as (
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
    walk(root_id, tower_id) as (
        select tower_id, tower_id
        from mesh_towers

        union

        select walk.root_id, visible_edges.target_id
        from walk
        join visible_edges on visible_edges.source_id = walk.tower_id
    ),
    components as (
        select tower_id, min(root_id) as component_id
        from walk
        group by tower_id
    )
    select count(distinct component_id)
    into current_component_count
    from components;

    -- If a generated routing tower can collapse into a nearby existing tower
    -- without losing its cached-LOS neighbors, it is redundant. Delete it
    -- instead of moving it into a local back-and-forth blob. The distance
    -- setting only finds candidate duplicates; the actual deletion requires
    -- the merge target to preserve the anchor's cached visible-neighbor set.
    select
        mt.tower_id,
        mt.h3,
        mt.centroid_geog,
        mt.source
    into merge_target
    from mesh_towers mt
    where anchor.source = any(prunable_sources)
      and mt.tower_id <> anchor.tower_id
      and ST_DWithin(mt.centroid_geog, best.h3::geography, generated_tower_merge_distance)
    order by ST_Distance(mt.centroid_geog, best.h3::geography),
             case when mt.source = 'population' then 0 else 1 end,
             mt.tower_id
    limit 1;

    if merge_target.tower_id is not null
       and not exists (
            select 1
            from mesh_towers nb
            join mesh_los_cache anchor_link
              on anchor_link.src_h3 = least(anchor.h3, nb.h3)
             and anchor_link.dst_h3 = greatest(anchor.h3, nb.h3)
             and anchor_link.mast_height_src = mast_height
             and anchor_link.mast_height_dst = mast_height
             and anchor_link.frequency_hz = frequency
             and anchor_link.clearance > 0
             and anchor_link.distance_m <= max_distance
            where nb.tower_id <> anchor.tower_id
              and nb.tower_id <> merge_target.tower_id
              and ST_DWithin(nb.centroid_geog, anchor.centroid_geog, max_distance)
              and not exists (
                    select 1
                    from mesh_los_cache merged_link
                    where merged_link.src_h3 = least(merge_target.h3, nb.h3)
                      and merged_link.dst_h3 = greatest(merge_target.h3, nb.h3)
                      and merged_link.mast_height_src = mast_height
                      and merged_link.mast_height_dst = mast_height
                      and merged_link.frequency_hz = frequency
                      and merged_link.clearance > 0
                      and merged_link.distance_m <= max_distance
                )
        ) then
        begin
            old_centroid := anchor.centroid_geog;
            new_centroid := merge_target.centroid_geog;

            delete from mesh_tower_wiggle_queue
            where tower_id = anchor.tower_id;

            delete from mesh_towers
            where tower_id = anchor.tower_id;

            with recursive visible_edges as (
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
            walk(root_id, tower_id) as (
                select tower_id, tower_id
                from mesh_towers

                union

                select walk.root_id, visible_edges.target_id
                from walk
                join visible_edges on visible_edges.source_id = walk.tower_id
            ),
            components as (
                select tower_id, min(root_id) as component_id
                from walk
                group by tower_id
            )
            select count(distinct component_id)
            into actual_component_count
            from components;

            if actual_component_count > current_component_count then
                raise exception 'mesh_tower_wiggle_split: pruning tower % would grow LOS components from % to %',
                    anchor.tower_id,
                    current_component_count,
                    actual_component_count;
            end if;

            update mesh_surface_h3_r8 s
            set has_tower = false,
                clearance = null,
                path_loss = null,
                visible_population = null,
                visible_uncovered_population = null
            where s.h3 = anchor.h3
              and not exists (
                    select 1
                    from mesh_towers mt
                    where mt.h3 = s.h3
                );

            with affected_cells as materialized (
                select h3, centroid_geog
                from mesh_surface_h3_r8
                where ST_DWithin(centroid_geog, old_centroid, max_distance)
                   or ST_DWithin(centroid_geog, new_centroid, max_distance)
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

            with dirty_neighbors as materialized (
                select q.tower_id
                from mesh_tower_wiggle_queue q
                join mesh_towers nb on nb.tower_id = q.tower_id
                where nb.source = any(target_sources)
                  and ST_DWithin(nb.centroid_geog, old_centroid, max_distance)
                for update of q skip locked
            )
            update mesh_tower_wiggle_queue q
            set is_dirty = true
            from dirty_neighbors
            where dirty_neighbors.tower_id = q.tower_id;

            raise notice 'Wiggle pruned redundant tower % at %, merged into tower % at %',
                anchor.tower_id,
                anchor.h3,
                merge_target.tower_id,
                merge_target.h3;

            return 1;
        exception
            when raise_exception then
                if sqlerrm like 'mesh_tower_wiggle_split:%' then
                    prune_rejected := true;
                    raise notice 'Wiggle skipped pruning tower % at % because it would split the live LOS graph',
                        anchor.tower_id,
                        anchor.h3;
                else
                    raise;
                end if;
        end;
    end if;

    if best.h3 <> anchor.h3
       and exists (
            select 1
            from mesh_towers occupied
            where occupied.h3 = best.h3
              and occupied.tower_id <> anchor.tower_id
        ) then
        raise notice 'Wiggle kept tower % at % because concurrent worker occupied candidate %',
            anchor.tower_id,
            anchor.h3,
            best.h3;
        best.h3 := anchor.h3;
        best.visible_population := anchor.priority_population;
    end if;

    if best.h3 = anchor.h3 then
        update mesh_towers
        set recalculation_count = coalesce(recalculation_count, 0) + 1
        where tower_id = anchor.tower_id;

        update mesh_tower_wiggle_queue
        set is_dirty = false
        where tower_id = anchor.tower_id;

        processed := 1;

        raise notice 'Wiggle kept tower % at % (score %)',
            anchor.tower_id,
            anchor.h3,
            coalesce(best.visible_population, 0);

        return processed;
    end if;

    begin
        -- Persist the move only if the live LOS graph stays in the same number
        -- of connected components after the tower changes H3.
        update mesh_towers
        set h3 = best.h3,
            recalculation_count = coalesce(recalculation_count, 0) + 1
        where tower_id = anchor.tower_id;

        with recursive visible_edges as (
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
        walk(root_id, tower_id) as (
            select tower_id, tower_id
            from mesh_towers

            union

            select walk.root_id, visible_edges.target_id
            from walk
            join visible_edges on visible_edges.source_id = walk.tower_id
        ),
        components as (
            select tower_id, min(root_id) as component_id
            from walk
            group by tower_id
        )
        select count(distinct component_id)
        into actual_component_count
        from components;

        if actual_component_count > current_component_count then
            raise exception 'mesh_tower_wiggle_split: moving tower % would grow LOS components from % to %',
                anchor.tower_id,
                current_component_count,
                actual_component_count;
        end if;

        update mesh_tower_wiggle_queue
        set is_dirty = false
        where tower_id = anchor.tower_id;

        raise notice 'Wiggle moving tower % from % to % (visible pop % -> %)',
            anchor.tower_id,
            anchor.h3,
            best.h3,
            coalesce(anchor.priority_population, 0),
            coalesce(best.visible_population, 0);

        -- Drop tower flags from the old hex and promote the new one.
        update mesh_surface_h3_r8
        set has_tower = false,
            clearance = null,
            path_loss = null,
            visible_uncovered_population = null
        where h3 = anchor.h3;

        update mesh_surface_h3_r8
        set has_tower = true,
            clearance = null,
            path_loss = null,
            visible_uncovered_population = 0,
            distance_to_closest_tower = 0
        where h3 = best.h3;

        -- Recompute nearest-tower distances for the region affected by the move.
        select centroid_geog into old_centroid from mesh_surface_h3_r8 where h3 = anchor.h3;
        select centroid_geog into new_centroid from mesh_surface_h3_r8 where h3 = best.h3;

        with affected_cells as materialized (
            select h3, centroid_geog
            from mesh_surface_h3_r8
            where ST_DWithin(centroid_geog, old_centroid, max_distance)
               or ST_DWithin(centroid_geog, new_centroid, max_distance)
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

        -- Defer heavy local RF and population refresh to the later route-refresh stage.

        -- Mark cached-LOS neighbors for another pass now that visibility changed.
        with dirty_neighbors as materialized (
            select q.tower_id
            from mesh_tower_wiggle_queue q
            join mesh_towers nb on nb.tower_id = q.tower_id
            join mesh_los_cache mlc
              on mlc.src_h3 = least(nb.h3, best.h3)
             and mlc.dst_h3 = greatest(nb.h3, best.h3)
             and mlc.mast_height_src = mast_height
             and mlc.mast_height_dst = mast_height
             and mlc.frequency_hz = frequency
             and mlc.clearance > 0
             and mlc.distance_m <= max_distance
            where nb.tower_id <> anchor.tower_id
              and nb.source = any(target_sources)
              and ST_DWithin(nb.centroid_geog, new_centroid, max_distance)
            for update of q skip locked
        )
        update mesh_tower_wiggle_queue q
        set is_dirty = true
        from dirty_neighbors
        where dirty_neighbors.tower_id = q.tower_id;

        processed := 1;

        raise notice 'Wiggle deferred local RF and visibility refresh after moving tower %', anchor.tower_id;

        return processed;
    exception
        when raise_exception then
            if sqlerrm like 'mesh_tower_wiggle_split:%' then
                best.h3 := anchor.h3;
                best.visible_population := anchor.priority_population;
                raise notice 'Wiggle kept tower % at % because moving it would split the live LOS graph',
                    anchor.tower_id,
                    anchor.h3;
            else
                raise;
            end if;
    end;

    update mesh_towers
    set recalculation_count = coalesce(recalculation_count, 0) + 1
    where tower_id = anchor.tower_id;

    update mesh_tower_wiggle_queue
    set is_dirty = false
    where tower_id = anchor.tower_id;

    processed := 1;

    return processed;
end;
$$;
