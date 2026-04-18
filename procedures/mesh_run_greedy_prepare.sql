set client_min_messages = notice;

drop procedure if exists mesh_run_greedy_prepare();
-- Reset greedy-placement artifacts while preserving any towers the routing stage just promoted.
create or replace procedure mesh_run_greedy_prepare()
language plpgsql
as
$$
begin
    raise notice 'Resetting greedy artifacts';

    -- Remove only towers installed by the greedy loop itself so upstream stages stay intact.
    delete from mesh_towers
    where source in ('greedy', 'bridge');

    truncate mesh_greedy_iterations;

    -- Align the serial so subsequent inserts keep growing instead of reusing deleted ids.
    perform setval(
        pg_get_serial_sequence('mesh_towers', 'tower_id'),
        coalesce((select max(tower_id) from mesh_towers), 0)
    );

    -- Rehydrate derived surface flags based on the current tower registry.
    update mesh_surface_h3_r8
    set has_tower = false
    where has_tower;

    update mesh_surface_h3_r8 s
    set has_tower = true
    from mesh_towers t
    where s.h3 = t.h3;

    -- Refresh distance-to-nearest-tower for the entire surface so greedy starts from current routing output.
    update mesh_surface_h3_r8 s
    set distance_to_closest_tower = sub.dist_m
    from (
        select
            s2.h3,
            ST_Distance(s2.centroid_geog, nearest.centroid_geog) as dist_m
        from mesh_surface_h3_r8 s2
        cross join lateral (
            select t.centroid_geog
            from mesh_towers t
            order by s2.centroid_geog <-> t.centroid_geog
            limit 1
        ) as nearest
    ) sub
    where s.h3 = sub.h3;

    -- Clear cached RF and visibility-derived metrics.
    -- The first greedy iteration recomputes these incrementally, so prepare must stay cheap.
    update mesh_surface_h3_r8
    set visible_tower_count = null,
        clearance = null,
        path_loss = null,
        visible_uncovered_population = case
            when has_tower then 0
            else null
        end;
end;
$$;
