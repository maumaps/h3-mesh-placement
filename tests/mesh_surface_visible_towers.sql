set client_min_messages = warning;

begin;

do
$$
declare
    sample record;
    expected bigint;
    samples_checked integer := 0;
    max_samples constant integer := 10;
begin
    for sample in
        select
            h3,
            centroid_geog,
            visible_tower_count
        from mesh_surface_h3_r8
        where distance_to_closest_tower < 70000
        order by distance_to_closest_tower
        limit max_samples
    loop
        samples_checked := samples_checked + 1;

        select count(*)
        into expected
        from mesh_towers t
        where t.h3 <> sample.h3
          and ST_DWithin(sample.centroid_geog, t.centroid_geog, 70000)
          and h3_los_between_cells(sample.h3, t.h3);

        if sample.visible_tower_count is distinct from expected then
            raise exception 'visible_tower_count mismatch for %: expected %, stored %, sample %/%',
                sample.h3::text,
                expected,
                sample.visible_tower_count,
                samples_checked,
                max_samples;
        end if;
    end loop;

    if samples_checked = 0 then
        raise exception 'visible_tower_count test found no surface cells within 70 km of a tower';
    end if;
end;
$$;

rollback;
