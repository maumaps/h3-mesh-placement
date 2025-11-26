set client_min_messages = warning;

drop function if exists mesh_surface_refresh_reception_metrics(h3index, double precision, double precision, integer);
-- Refresh clearance and path loss for cells around a tower that were invalidated during greedy placement
create or replace function mesh_surface_refresh_reception_metrics(
        center_h3 h3index,
        radius double precision default 70000,
        los_distance double precision default 70000,
        neighbor_limit integer default 5
    )
    returns void
    language plpgsql
    volatile
    parallel restricted
as
$$
declare
    center_geog public.geography;
begin
    if center_h3 is null then
        return;
    end if;

    select centroid_geog
    into center_geog
    from mesh_surface_h3_r8
    where h3 = center_h3;

    if center_geog is null then
        return;
    end if;

    with target_cells as materialized (
        select
            s.h3,
            s.centroid_geog
        from mesh_surface_h3_r8 s
        where s.has_tower is not true
          and ST_DWithin(s.centroid_geog, center_geog, radius)
          and s.distance_to_closest_tower < los_distance
          and (s.clearance is null or s.path_loss is null)
    ),
    candidate_towers as (
        select
            tc.h3 as cell_h3,
            nearest.tower_h3
        from target_cells tc
        join lateral (
            select t.h3 as tower_h3,
                   t.centroid_geog
            from mesh_towers t
            where t.h3 <> tc.h3
            order by tc.centroid_geog <-> t.centroid_geog
            limit neighbor_limit
        ) nearest
          on ST_DWithin(tc.centroid_geog, nearest.centroid_geog, los_distance)
    ),
    target_metrics as (
        select distinct on (ct.cell_h3)
            ct.cell_h3,
            metrics.clearance,
            metrics.path_loss_db
        from candidate_towers ct
        cross join lateral h3_visibility_metrics(
            ct.cell_h3,
            ct.tower_h3,
            28,
            28,
            868e6
        ) as metrics(clearance, path_loss_db)
        order by ct.cell_h3, metrics.path_loss_db asc
    )
    update mesh_surface_h3_r8 s
    set clearance = tm.clearance,
        path_loss = tm.path_loss_db
    from target_metrics tm
    where s.h3 = tm.cell_h3;
end;
$$;
