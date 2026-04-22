set client_min_messages = warning;

begin;

do
$$
declare
    missing_h3 h3index;
    neighbor_h3 h3index;
    clearance double precision;
    is_visible boolean;
begin
    -- Pick two nearby valid cells, then null one endpoint inside this transaction
    -- so the LOS helpers exercise the missing-elevation branch deterministically.
    select s.h3
    into missing_h3
    from mesh_surface_h3_r8 s
    where s.ele is not null
    order by s.h3
    limit 1;

    if missing_h3 is null then
        raise exception 'Expected at least one mesh_surface_h3_r8 cell with elevation to validate missing-DEM handling';
    end if;

    select s.h3
    into neighbor_h3
    from mesh_surface_h3_r8 s
    where s.h3 <> missing_h3
      and s.ele is not null
      and ST_DWithin(s.centroid_geog, missing_h3::geography, 100000)
    order by s.centroid_geog <-> missing_h3::geography
    limit 1;

    if neighbor_h3 is null then
        raise exception 'Expected a nearby mesh_surface_h3_r8 neighbor with elevation for source cell %', missing_h3::text;
    end if;

    update mesh_surface_h3_r8
    set ele = null
    where h3 = missing_h3;

    delete
    from mesh_los_cache
    where src_h3 = least(missing_h3, neighbor_h3)
      and dst_h3 = greatest(missing_h3, neighbor_h3)
      and mast_height_src = 28
      and mast_height_dst = 28
      and frequency_hz = 868e6;

    clearance := h3_visibility_clearance(
        missing_h3,
        neighbor_h3,
        28,
        28,
        868e6
    );

    if clearance is not null then
        raise exception 'Expected null clearance for missing-elevation pair % -> %, got %', missing_h3::text, neighbor_h3::text, clearance;
    end if;

    is_visible := h3_los_between_cells(missing_h3, neighbor_h3);

    if is_visible is distinct from false then
        raise exception 'Expected LOS=false for missing-elevation pair % -> %, got %', missing_h3::text, neighbor_h3::text, is_visible;
    end if;
end;
$$;

rollback;
