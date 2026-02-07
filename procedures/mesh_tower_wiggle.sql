set client_min_messages = notice;

drop procedure if exists mesh_tower_wiggle();
drop function if exists mesh_tower_wiggle();

-- Recenter one population/route/bridge/cluster-slim tower toward denser visible population while preserving current LOS neighbors.
create or replace function mesh_tower_wiggle(reset_run boolean default false)
    returns integer
    language plpgsql
as
$$
declare
    max_distance constant double precision := 70000;
    separation_default constant double precision := 5000;
    target_sources constant text[] := array['route', 'cluster_slim', 'bridge', 'population'];
    processed integer := 0;
    anchor record;
    best record;
    old_centroid public.geography;
    new_centroid public.geography;
begin
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
    limit 1;

    if not found then
        raise notice 'Wiggle idle: no dirty towers remain';
        return 0;
    end if;

    raise notice 'Wiggle evaluating tower % at % (recalcs %, visible_pop %)',
        anchor.tower_id,
        anchor.h3,
        coalesce(anchor.recalculation_count, 0),
        coalesce(anchor.priority_population, 0);

    -- Identify every tower currently visible from the anchor tower.
    with visible_neighbors as materialized (
        select
            nb.tower_id,
            nb.h3,
            nb.centroid_geog
        from mesh_towers nb
        where nb.tower_id <> anchor.tower_id
          and ST_DWithin(nb.centroid_geog, anchor.centroid_geog, max_distance)
          and h3_los_between_cells(anchor.h3, nb.h3)
    ),
    -- Enumerate candidate cells that keep those neighbors in LOS and obey spacing/eligibility checks.
    viable_candidates as materialized (
        select
            s.h3,
            s.centroid_geog
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
          and not exists (
                select 1
                from visible_neighbors nb
                where not ST_DWithin(s.centroid_geog, nb.centroid_geog, max_distance)
                   or not h3_los_between_cells(s.h3, nb.h3)
            )
    ),
    -- Score every viable cell by the visible population it can cover.
    candidate_visible_population as (
        select
            vc.h3,
            coalesce(sum(pop.population) filter (where h3_los_between_cells(vc.h3, pop.h3)), 0) as visible_population
        from viable_candidates vc
        join mesh_surface_h3_r8 pop
          on pop.population > 0
         and ST_DWithin(vc.centroid_geog, pop.centroid_geog, max_distance)
        group by vc.h3
    )
    select
        cv.h3,
        cv.visible_population
    into best
    from candidate_visible_population cv
    order by cv.visible_population desc,
             case when cv.h3 = anchor.h3 then 0 else 1 end,
             cv.h3
    limit 1;

    if best.h3 is null then
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

    -- Persist the move and bump the recalculation counter.
    update mesh_towers
    set h3 = best.h3,
        recalculation_count = coalesce(recalculation_count, 0) + 1
    where tower_id = anchor.tower_id;

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
        visible_uncovered_population = case
            when s.has_tower then 0
            else null
        end
    from recomputed_distances rd
    where s.h3 = rd.h3;

    -- Refresh LOS counts and RF metrics around the old and new tower slots.
    perform mesh_surface_refresh_visible_tower_counts(anchor.h3, max_distance, max_distance);
    perform mesh_surface_refresh_visible_tower_counts(best.h3, max_distance, max_distance);
    perform mesh_surface_refresh_reception_metrics(anchor.h3, max_distance, max_distance);
    perform mesh_surface_refresh_reception_metrics(best.h3, max_distance, max_distance);
    perform mesh_surface_fill_visible_population(best.h3);

    -- Mark neighbors for another pass now that visibility changed.
    update mesh_tower_wiggle_queue q
    set is_dirty = true
    from mesh_towers nb
    where nb.tower_id = q.tower_id
      and nb.tower_id <> anchor.tower_id
      and nb.source = any(target_sources)
      and ST_DWithin(nb.centroid_geog, new_centroid, max_distance)
      and h3_los_between_cells(nb.h3, best.h3);

    processed := 1;

    if to_regprocedure('mesh_visibility_edges_refresh()') is not null then
        call mesh_visibility_edges_refresh();
    end if;

    return processed;
end;
$$;
