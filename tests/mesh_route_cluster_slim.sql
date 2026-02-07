set client_min_messages = warning;

begin;

-- Stub heavy dependencies so the test can run against temporary tables only.
create or replace procedure mesh_visibility_edges_refresh()
    language plpgsql
as
$$
begin
    -- Intentionally empty for the test harness.
    return;
end;
$$;

create or replace function mesh_surface_refresh_visible_tower_counts(
        center_h3 h3index,
        radius double precision default 70000,
        los_distance double precision default 70000
    )
    returns void
    language plpgsql
as
$$
begin
    return;
end;
$$;

create or replace function mesh_surface_refresh_reception_metrics(
        center_h3 h3index,
        radius double precision default 70000,
        los_distance double precision default 70000,
        neighbor_limit integer default 5
    )
    returns void
    language plpgsql
as
$$
begin
    return;
end;
$$;

create or replace function h3_los_between_cells(h3_a h3index, h3_b h3index)
    returns boolean
    language plpgsql
as
$$
begin
    return true;
end;
$$;

truncate mesh_route_cluster_slim_failures;

-- Shadow core tables with lightweight temporary versions for deterministic routing.
create temporary table mesh_surface_h3_r8 (
    h3 h3index primary key,
    centroid_geog public.geography not null,
    geom geometry not null,
    ele double precision default 0,
    has_tower boolean not null default false,
    clearance double precision,
    path_loss double precision,
    visible_uncovered_population double precision,
    distance_to_closest_tower double precision,
    visible_tower_count integer
) on commit drop;

create temporary table mesh_route_nodes (
    node_id integer primary key,
    h3 h3index not null
) on commit drop;

create temporary table mesh_route_edges (
    edge_id serial primary key,
    source integer not null,
    target integer not null,
    cost double precision not null,
    reverse_cost double precision not null
) on commit drop;

create temporary table mesh_towers (
    tower_id serial primary key,
    h3 h3index not null unique,
    source text not null,
    centroid_geog public.geography generated always as (h3_cell_to_geometry(h3)::public.geography) stored
) on commit drop;

create temporary table mesh_visibility_edges (
    source_id integer not null,
    target_id integer not null,
    source_h3 h3index not null,
    target_h3 h3index not null,
    type text not null,
    distance_m double precision not null,
    is_visible boolean not null,
    is_between_clusters boolean not null,
    cluster_hops integer not null,
    geom geometry not null
) on commit drop;

do
$$
declare
    seed_one h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.0), 4326), 8);
    seed_two h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.9, 0.0), 4326), 8);
    bridge_one h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.4, 0.4), 4326), 8);
    bridge_two h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.8, 0.5), 4326), 8);
    seed_mid h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.2, 0.2), 4326), 8);
    non_seed_mid h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.6, 0.45), 4326), 8);
    seed_one_id integer;
    seed_two_id integer;
    bridge_one_id integer;
    bridge_two_id integer;
    promoted integer;
    failure_snapshot jsonb;
begin
    -- Populate minimal surface rows covering every H3 referenced in the test.
    insert into mesh_surface_h3_r8 (h3, centroid_geog, geom, ele, has_tower, visible_uncovered_population, distance_to_closest_tower, visible_tower_count)
    values
        (seed_one, h3_cell_to_geometry(seed_one)::public.geography, h3_cell_to_boundary_geometry(seed_one), 0, true, 0, 0, 2),
        (seed_two, h3_cell_to_geometry(seed_two)::public.geography, h3_cell_to_boundary_geometry(seed_two), 0, true, 0, 0, 2),
        (bridge_one, h3_cell_to_geometry(bridge_one)::public.geography, h3_cell_to_boundary_geometry(bridge_one), 0, true, 0, 0, 2),
        (bridge_two, h3_cell_to_geometry(bridge_two)::public.geography, h3_cell_to_boundary_geometry(bridge_two), 0, true, 0, 0, 2),
        (seed_mid, h3_cell_to_geometry(seed_mid)::public.geography, h3_cell_to_boundary_geometry(seed_mid), 0, false, 100, 15000, 0),
        (non_seed_mid, h3_cell_to_geometry(non_seed_mid)::public.geography, h3_cell_to_boundary_geometry(non_seed_mid), 0, false, 100, 15000, 0);

    -- Register towers with deterministic IDs so mesh_visibility_edges can reference them.
    insert into mesh_towers (h3, source)
    values
        (seed_one, 'seed')
    returning tower_id into seed_one_id;

    insert into mesh_towers (h3, source)
    values
        (seed_two, 'seed')
    returning tower_id into seed_two_id;

    insert into mesh_towers (h3, source)
    values
        (bridge_one, 'bridge')
    returning tower_id into bridge_one_id;

    insert into mesh_towers (h3, source)
    values
        (bridge_two, 'bridge')
    returning tower_id into bridge_two_id;

    -- Map every H3 to a routing node so pgr_dijkstra can explore both corridors.
    insert into mesh_route_nodes (node_id, h3)
    values
        (1, seed_one),
        (2, bridge_one),
        (3, bridge_two),
        (4, seed_two),
        (10, seed_mid),
        (11, non_seed_mid);

    -- Build a simple directed graph that routes through the intermediate nodes.
    insert into mesh_route_edges (source, target, cost, reverse_cost)
    values
        (1, 10, 1, 1),
        (10, 2, 1, 1),
        (2, 11, 1, 1),
        (11, 3, 1, 1);

    -- Declare two over-limit visibility pairs, only one of which touches a seed tower.
    insert into mesh_visibility_edges (
        source_id,
        target_id,
        source_h3,
        target_h3,
        type,
        distance_m,
        is_visible,
        is_between_clusters,
        cluster_hops,
        geom
    )
    values
        (seed_one_id, bridge_one_id, seed_one, bridge_one, 'seed-bridge', 200000, true, false, 10,
            ST_MakeLine(h3_cell_to_geometry(seed_one)::geometry, h3_cell_to_geometry(bridge_one)::geometry)),
        (bridge_one_id, bridge_two_id, bridge_one, bridge_two, 'bridge-bridge', 150000, true, false, 8,
            ST_MakeLine(h3_cell_to_geometry(bridge_one)::geometry, h3_cell_to_geometry(bridge_two)::geometry));

    -- Process a single iteration so we can observe which corridor wins.
    call mesh_route_cluster_slim(1, promoted);

    if coalesce(promoted, 0) = 0 then
        raise exception 'Cluster slim iteration did not promote any towers; mesh_towers snapshot %',
            array(select h3 from mesh_towers order by h3);
    end if;

    if not exists (
        select 1
        from mesh_towers
        where source = 'cluster_slim'
          and h3 in (seed_mid, non_seed_mid)
    ) then
        raise exception 'Expected seed corridor to promote one of %, %, but mesh_towers now has %',
            seed_mid,
            non_seed_mid,
            array(select h3 from mesh_towers order by h3);
    end if;

    select coalesce(json_agg(row_to_json(mesh_route_cluster_slim_failures))::jsonb, '[]'::jsonb)
    into failure_snapshot
    from mesh_route_cluster_slim_failures;

    if not exists (
        select 1
        from mesh_route_cluster_slim_failures
        where source_id = seed_one_id
          and target_id = bridge_one_id
          and status = 'completed'
    ) then
        raise exception 'Expected seed pair % -> % to be completed; failure log snapshot %',
            seed_one_id,
            bridge_one_id,
            failure_snapshot;
    end if;

    if exists (
        select 1
        from mesh_route_cluster_slim_failures
        where source_id = bridge_one_id
          and target_id = bridge_two_id
          and status = 'completed'
    ) then
        raise exception 'Non-seed pair % -> % should not complete first; failure log snapshot %',
            bridge_one_id,
            bridge_two_id,
            failure_snapshot;
    end if;
end;
$$;

rollback;
