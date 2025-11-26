set client_min_messages = warning;

begin;

do
$$
declare
    sample_tower record;
    tower_geog public.geography;
    updated_cells integer;
    stale_cells integer;
    refresh_radius constant double precision := 10000;
begin
    select
        t.h3,
        t.centroid_geog
    into sample_tower
    from mesh_towers t
    limit 1;

    if sample_tower.h3 is null then
        raise exception 'mesh_surface_refresh_visible_tower_counts test requires at least one tower';
    end if;

    tower_geog := sample_tower.centroid_geog;

    update mesh_surface_h3_r8
    set visible_tower_count = -1
    where ST_DWithin(centroid_geog, tower_geog, refresh_radius);

    get diagnostics updated_cells = row_count;

    if updated_cells = 0 then
        raise exception 'mesh_surface_refresh_visible_tower_counts test requires cells near tower %', sample_tower.h3::text;
    end if;

    perform mesh_surface_refresh_visible_tower_counts(
        sample_tower.h3,
        refresh_radius,
        70000
    );

    select count(*)
    into stale_cells
    from mesh_surface_h3_r8
    where ST_DWithin(centroid_geog, tower_geog, refresh_radius)
      and visible_tower_count = -1;

    if stale_cells > 0 then
        raise exception 'visible_tower_count not refreshed for % cells near tower %',
            stale_cells,
            sample_tower.h3::text;
    end if;
end;
$$;

rollback;
