set client_min_messages = warning;

begin;

do
$$
declare
    missing_building_count bigint;
    inconsistent_flag_count bigint;
    positive_building_count bigint;
begin
    -- Ensure the surface received at least some building counts from the OSM-derived layer.
    select count(*)
    into positive_building_count
    from mesh_surface_h3_r8
    where coalesce(building_count, 0) > 0;

    if positive_building_count = 0 then
        raise exception
            'mesh_surface_h3_r8 has no positive building_count rows after building import';
    end if;

    -- Ensure every in-domain building-backed H3 imported into buildings_h3_r8 is present on the surface.
    select count(*)
    into missing_building_count
    from buildings_h3_r8 b
    join mesh_surface_domain_h3_r8 d on d.h3 = b.h3
    left join mesh_surface_h3_r8 s on s.h3 = b.h3
    where s.h3 is null;

    if missing_building_count > 0 then
        raise exception
            'mesh_surface_h3_r8 is missing % in-domain rows that exist in buildings_h3_r8',
            missing_building_count;
    end if;

    -- Ensure the generated has_building flag stays consistent with the stored count.
    select count(*)
    into inconsistent_flag_count
    from mesh_surface_h3_r8 s
    where (coalesce(s.building_count, 0) > 0) is distinct from s.has_building;

    if inconsistent_flag_count > 0 then
        raise exception
            'mesh_surface_h3_r8 has % rows where has_building does not match building_count > 0',
            inconsistent_flag_count;
    end if;
end;
$$;

rollback;
