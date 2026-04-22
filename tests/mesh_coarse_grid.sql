set client_min_messages = warning;

begin;

-- Shadow pipeline settings so the fixture is not controlled by live operator config.
create temporary table mesh_pipeline_settings (
    setting text primary key,
    value text not null
) on commit drop;

insert into mesh_pipeline_settings (setting, value)
values
    ('enable_coarse', 'true'),
    ('max_los_distance_m', '100000'),
    ('coarse_resolution', '4');

-- Shadow production planning tables so this fixture never mutates live placement state.
create temporary table mesh_surface_h3_r8 (like public.mesh_surface_h3_r8 including all) on commit drop;
create temporary sequence mesh_towers_test_tower_id_seq;
create temporary table mesh_towers (like public.mesh_towers including all) on commit drop;
alter table mesh_towers alter column tower_id set default nextval('mesh_towers_test_tower_id_seq');
alter sequence mesh_towers_test_tower_id_seq owned by mesh_towers.tower_id;

do
$$
declare
    occupied_seed h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(44.80, 41.70), 4326), 8);
    preferred_base h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(43.40, 40.90), 4326), 8);
    occupied_candidate h3index;
    non_building_candidate h3index;
    building_candidate h3index;
    occupied_parent h3index;
    preferred_parent h3index;
begin
    occupied_parent := h3_cell_to_parent(occupied_seed, 4);
    preferred_parent := h3_cell_to_parent(preferred_base, 4);

    select candidate
    into occupied_candidate
    from h3_grid_disk(occupied_seed, 6) as candidate
    where candidate <> occupied_seed
      and h3_cell_to_parent(candidate, 4) = occupied_parent
    limit 1;

    select candidate
    into building_candidate
    from h3_grid_disk(preferred_base, 6) as candidate
    where candidate <> preferred_base
      and h3_cell_to_parent(candidate, 4) = preferred_parent
    limit 1;

    if occupied_candidate is null or building_candidate is null then
        raise exception 'mesh_coarse_grid test could not find neighboring candidates in matching coarse cells';
    end if;

    non_building_candidate := preferred_base;

    insert into mesh_surface_h3_r8 (
        h3,
        ele,
        has_road,
        building_count,
        population,
        has_tower,
        is_in_boundaries,
        is_in_unfit_area,
        min_distance_to_closest_tower,
        population_70km,
        visible_population,
        visible_uncovered_population,
        visible_tower_count,
        distance_to_closest_tower
    )
    values
        (occupied_seed, 0, true, 0, 10, true, true, false, 5000, 10, 10, 10, 2, 0),
        (occupied_candidate, 0, true, 5, 40, false, true, false, 5000, 40, 40, 40, 2, 120000),
        (non_building_candidate, 0, true, 0, 500, false, true, false, 5000, 500, 500, 500, 2, 120000),
        (building_candidate, 0, true, 2, 50, false, true, false, 5000, 50, 50, 50, 2, 120000);

    insert into mesh_towers (h3, source)
    values (occupied_seed, 'seed');
end;
$$;

call mesh_coarse_grid();

do
$$
declare
    occupied_seed h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(44.80, 41.70), 4326), 8);
    preferred_base h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(43.40, 40.90), 4326), 8);
    occupied_parent h3index := h3_cell_to_parent(occupied_seed, 4);
    preferred_parent h3index := h3_cell_to_parent(preferred_base, 4);
    inserted_preferred h3index;
    inserted_count integer;
    occupied_count integer;
begin
    select count(*)
    into inserted_count
    from mesh_towers
    where source = 'coarse';

    if inserted_count <> 1 then
        raise exception 'mesh_coarse_grid should insert exactly one coarse tower in this fixture, found %',
            inserted_count;
    end if;

    select h3
    into inserted_preferred
    from mesh_towers
    where source = 'coarse';

    if h3_cell_to_parent(inserted_preferred, 4) <> preferred_parent then
        raise exception 'mesh_coarse_grid inserted tower % in parent %, expected preferred parent %',
            inserted_preferred::text,
            h3_cell_to_parent(inserted_preferred, 4)::text,
            preferred_parent::text;
    end if;

    if inserted_preferred = preferred_base then
        raise exception 'mesh_coarse_grid should prefer building-bearing candidate over non-building winner inside one coarse cell, but kept %',
            inserted_preferred::text;
    end if;

    select count(*)
    into occupied_count
    from mesh_towers
    where source = 'coarse'
      and h3_cell_to_parent(h3, 4) = occupied_parent;

    if occupied_count <> 0 then
        raise exception 'mesh_coarse_grid should skip coarse cells that already contain towers, but inserted % coarse tower(s) into occupied parent %',
            occupied_count,
            occupied_parent::text;
    end if;
end;
$$;

rollback;
