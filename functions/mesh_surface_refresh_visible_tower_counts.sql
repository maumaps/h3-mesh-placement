set client_min_messages = warning;

drop function if exists mesh_surface_refresh_visible_tower_counts(h3index, double precision, double precision);
-- Refresh visible_tower_count values near a center H3 cell
create or replace function mesh_surface_refresh_visible_tower_counts(
        center_h3 h3index,
        radius double precision default 70000,
        los_distance double precision default 70000
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

    -- Use stored centroid to match other distance-based recalculations
    select centroid_geog
    into center_geog
    from mesh_surface_h3_r8
    where h3 = center_h3;

    if center_geog is null then
        return;
    end if;

    with affected_cells as materialized (
        select h3, centroid_geog
        from mesh_surface_h3_r8
        where ST_DWithin(centroid_geog, center_geog, radius)
    ),
    visible_counts as (
        select
            ac.h3,
            count(*) as visible_count
        from affected_cells ac
        join mesh_towers t
          on t.h3 <> ac.h3
         and ST_DWithin(ac.centroid_geog, t.centroid_geog, los_distance)
        where h3_los_between_cells(ac.h3, t.h3)
        group by ac.h3
    )
    update mesh_surface_h3_r8 s
    set visible_tower_count = coalesce(vc.visible_count, 0)
    from affected_cells ac
    left join visible_counts vc on vc.h3 = ac.h3
    where s.h3 = ac.h3;

    return;
end;
$$;
