set client_min_messages = warning;

-- Validate mesh_tower_clusters() separates disconnected components
begin;

do
$$
declare
    mast_height constant double precision := 28;
    frequency constant double precision := 868e6;
    tower_a1_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.0), 4326), 8);
    tower_a2_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.01, 0.0), 4326), 8);
    tower_b1_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(10.0, 0.0), 4326), 8);
    tower_b2_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(10.01, 0.0), 4326), 8);
    tower_c1_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(25.0, 0.0), 4326), 8);
    tower_a1_id integer;
    tower_a2_id integer;
    tower_b1_id integer;
    tower_b2_id integer;
    tower_c1_id integer;
begin
    insert into mesh_towers (h3, source) values (tower_a1_h3, 'test_cluster')
        returning tower_id into tower_a1_id;
    insert into mesh_towers (h3, source) values (tower_a2_h3, 'test_cluster')
        returning tower_id into tower_a2_id;
    insert into mesh_towers (h3, source) values (tower_b1_h3, 'test_cluster')
        returning tower_id into tower_b1_id;
    insert into mesh_towers (h3, source) values (tower_b2_h3, 'test_cluster')
        returning tower_id into tower_b2_id;
    insert into mesh_towers (h3, source) values (tower_c1_h3, 'test_cluster')
        returning tower_id into tower_c1_id;

    with needed_cells as (
        select h3_grid_path_cells(tower_a1_h3, tower_a2_h3) as h3
        union all
        select h3_grid_path_cells(tower_b1_h3, tower_b2_h3)
    )
    insert into mesh_surface_h3_r8 (h3, ele)
    select nc.h3, 0
    from needed_cells nc
    on conflict (h3) do nothing;

    if not ST_DWithin(
        h3_cell_to_geometry(tower_a1_h3)::geography,
        h3_cell_to_geometry(tower_a2_h3)::geography,
        70000
    ) then
        raise exception 'Test towers % and % ended up farther than 70km apart, cannot validate connectivity',
            tower_a1_h3::text,
            tower_a2_h3::text;
    end if;

    if not ST_DWithin(
        h3_cell_to_geometry(tower_b1_h3)::geography,
        h3_cell_to_geometry(tower_b2_h3)::geography,
        70000
    ) then
        raise exception 'Test towers % and % ended up farther than 70km apart, cannot validate connectivity',
            tower_b1_h3::text,
            tower_b2_h3::text;
    end if;

    if not h3_los_between_cells(tower_a1_h3, tower_a2_h3) then
        raise exception 'Expected LOS between % and % after seeding mesh_surface rows',
            tower_a1_h3::text,
            tower_a2_h3::text;
    end if;

    if not h3_los_between_cells(tower_b1_h3, tower_b2_h3) then
        raise exception 'Expected LOS between % and % after seeding mesh_surface rows',
            tower_b1_h3::text,
            tower_b2_h3::text;
    end if;

    create temporary table test_clusters on commit drop as
    select *
    from mesh_tower_clusters();

    if not exists (
        select 1
        from test_clusters
        where tower_id = tower_a1_id
          and cluster_id = (
              select cluster_id from test_clusters where tower_id = tower_a2_id
          )
    ) then
        raise exception 'Expected towers % and % to be in same cluster but results differ (cluster_a1 %, cluster_a2 %)',
            tower_a1_id,
            tower_a2_id,
            (select cluster_id from test_clusters where tower_id = tower_a1_id),
            (select cluster_id from test_clusters where tower_id = tower_a2_id);
    end if;

    if exists (
        select 1
        from test_clusters
        where tower_id = tower_a1_id
          and cluster_id = (
              select cluster_id from test_clusters where tower_id = tower_b1_id
          )
    ) then
        raise exception 'Clusters for (% %, % %) unexpectedly merged into %',
            tower_a1_id,
            tower_a2_id,
            tower_b1_id,
            tower_b2_id,
            (select cluster_id from test_clusters where tower_id = tower_a1_id);
    end if;

    if (select cluster_id from test_clusters where tower_id = tower_c1_id) is distinct from tower_c1_id then
        raise exception 'Isolated tower % should form its own cluster but got id %',
            tower_c1_id,
            (select cluster_id from test_clusters where tower_id = tower_c1_id);
    end if;
end;
$$;

rollback;
