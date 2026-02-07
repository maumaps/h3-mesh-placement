set client_min_messages = warning;

begin;

-- Stub LOS helper so refresh logic runs quickly and deterministically.
create or replace function h3_los_between_cells(h3_a h3index, h3_b h3index)
    returns boolean
    language plpgsql
as
$$
begin
    return true;
end;
$$;

-- Stub routed geometry helper to avoid pgRouting corridor lookups in this fixture.
create or replace function mesh_visibility_invisible_route_geom(h3_a h3index, h3_b h3index)
    returns geometry
    language plpgsql
as
$$
begin
    return null;
end;
$$;

-- Stub cluster labeling so all towers appear in one cluster.
create or replace function mesh_tower_clusters()
    returns table (tower_id integer, cluster_id integer)
    language sql
as
$$
    select t.tower_id, 1
    from mesh_towers t;
$$;

-- Create a minimal mesh_towers table for deterministic type ordering.
create temporary table mesh_towers (
    tower_id serial primary key,
    h3 h3index not null unique,
    source text not null,
    centroid_geog public.geography generated always as (h3_cell_to_geometry(h3)::public.geography) stored
) on commit drop;

-- Create a minimal elevation table so mesh_visibility_edges_refresh treats every tower as eligible.
create temporary table gebco_elevation_h3_r8 (
    h3 h3index primary key
) on commit drop;

-- Create mesh_visibility_edges with the new type column for refresh output.
create temporary table mesh_visibility_edges (
    source_id integer not null,
    target_id integer not null,
    source_h3 h3index not null,
    target_h3 h3index not null,
    type text not null,
    distance_m double precision not null,
    is_visible boolean not null,
    is_between_clusters boolean not null,
    cluster_hops integer,
    geom geometry not null
) on commit drop;

-- Populate towers with sources that should be ordered by the type rules.
do
$$
declare
    route_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.0), 4326), 8);
    seed_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.3, 0.0), 4326), 8);
    greedy_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.6, 0.0), 4326), 8);
begin
    -- Insert towers in a non-sorted order so type ordering does not depend on tower_id.
    insert into mesh_towers (h3, source)
    values
        (route_h3, 'route'),
        (seed_h3, 'seed'),
        (greedy_h3, 'greedy');

    -- Register elevation coverage for every tower so none are filtered out.
    insert into gebco_elevation_h3_r8 (h3)
    values
        (route_h3),
        (seed_h3),
        (greedy_h3);
end;
$$;

-- Build the visibility diagnostics so type labels are populated.
call mesh_visibility_edges_refresh();

-- Verify type labels are canonical regardless of tower ordering.
do
$$
declare
    rec record;
    actual_type text;
begin
    -- Enumerate expected source pairings to validate the canonical ordering.
    for rec in
        select *
        from (
            values
                ('seed', 'route', 'seed-route'),
                ('route', 'greedy', 'route-greedy'),
                ('seed', 'greedy', 'seed-greedy')
        ) as expected(source_a, source_b, expected_type)
    loop
        -- Read the stored type for the source pair independent of edge direction.
        select e.type
        into actual_type
        from mesh_visibility_edges e
        join mesh_towers src on src.tower_id = e.source_id
        join mesh_towers dst on dst.tower_id = e.target_id
        where (src.source = rec.source_a and dst.source = rec.source_b)
           or (src.source = rec.source_b and dst.source = rec.source_a);

        if actual_type is null then
            raise exception 'mesh_visibility_edges type missing for sources % and %; expected %',
                rec.source_a, rec.source_b, rec.expected_type;
        end if;

        if actual_type <> rec.expected_type then
            raise exception 'mesh_visibility_edges type mismatch for sources % and %: stored %, expected %',
                rec.source_a, rec.source_b, actual_type, rec.expected_type;
        end if;
    end loop;
end;
$$;

rollback;
