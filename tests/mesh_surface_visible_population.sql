set client_min_messages = warning;

begin;

do
$$
declare
    sample_h3 h3index;
    sample_centroid public.geography;
    stored_visible numeric;
    expected_visible numeric;
begin
    select h3, centroid_geog
    into sample_h3, sample_centroid
    from mesh_surface_h3_r8
    where can_place_tower
    limit 1;

    if sample_h3 is null then
        raise exception 'Visible population test could not find can_place_tower cell';
    end if;

    stored_visible := mesh_surface_fill_visible_population(sample_h3);

    select coalesce(sum(population), 0)
    into expected_visible
    from mesh_surface_h3_r8 t
    where t.population > 0
      and ST_DWithin(sample_centroid, t.centroid_geog, 70000)
      and h3_los_between_cells(sample_h3, t.h3);

    if stored_visible is distinct from expected_visible then
        raise exception 'Expected visible_population=% for %, found %',
            expected_visible,
            sample_h3::text,
            stored_visible;
    end if;
end;
$$;

rollback;
