set client_min_messages = warning;

begin;

-- Stub heavy dependencies so the wiggle logic can run against lightweight tables.
create or replace function h3_los_between_cells(h3_a h3index, h3_b h3index)
    returns boolean
    language plpgsql
as
$$
begin
    return true;
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

create or replace function mesh_surface_fill_visible_population(target_h3 h3index)
    returns numeric
    language plpgsql
as
$$
declare
    cell_pop numeric;
begin
    select population into cell_pop from mesh_surface_h3_r8 where h3 = target_h3;

    update mesh_surface_h3_r8
    set visible_population = cell_pop
    where h3 = target_h3;

    return coalesce(cell_pop, 0);
end;
$$;

create or replace procedure mesh_visibility_edges_refresh()
    language plpgsql
as
$$
begin
    return;
end;
$$;

-- Minimal surface snapshot for the wiggle run.
create temporary table mesh_surface_h3_r8 (
    h3 h3index primary key,
    centroid_geog public.geography not null,
    is_in_boundaries boolean default true,
    has_road boolean default true,
    is_in_unfit_area boolean default false,
    has_tower boolean default false,
    visible_population numeric,
    population numeric default 0,
    min_distance_to_closest_tower double precision default 5000,
    distance_to_closest_tower double precision default 0,
    visible_uncovered_population numeric,
    clearance double precision,
    path_loss double precision
) on commit drop;

-- Tower registry with recalculation counters.
create temporary table mesh_towers (
    tower_id serial primary key,
    h3 h3index not null unique,
    source text not null,
    recalculation_count integer not null default 0,
    centroid_geog public.geography generated always as (h3_cell_to_geometry(h3)::public.geography) stored
) on commit drop;

create temporary table mesh_tower_wiggle_queue (
    tower_id integer primary key,
    is_dirty boolean not null default true
) on commit drop;

do
$$
declare
    route_anchor h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.0), 4326), 8);
    cluster_anchor h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.2, 0.0), 4326), 8);
    route_candidate h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.4, 0.0), 4326), 8);
    route_far_population h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.9, 0.0), 4326), 8);
    moved_count integer;
    route_recalc integer;
    cluster_recalc integer;
    relocated_h3 h3index;
begin
    insert into mesh_surface_h3_r8 (
        h3,
        centroid_geog,
        is_in_boundaries,
        has_road,
        is_in_unfit_area,
        has_tower,
        visible_population,
        population,
        min_distance_to_closest_tower,
        distance_to_closest_tower
    )
    values
        (route_anchor, h3_cell_to_geometry(route_anchor)::public.geography, true, true, false, true, 40, 40, 5000, 0),
        (cluster_anchor, h3_cell_to_geometry(cluster_anchor)::public.geography, true, true, false, true, 10, 10, 5000, 0),
        (route_candidate, h3_cell_to_geometry(route_candidate)::public.geography, true, true, false, false, 0, 100, 5000, 0),
        (route_far_population, h3_cell_to_geometry(route_far_population)::public.geography, true, true, false, false, 0, 80, 5000, 0);

    insert into mesh_towers (h3, source)
    values
        (route_anchor, 'route'),
        (cluster_anchor, 'cluster_slim');

    select mesh_tower_wiggle(true) into moved_count;

    if moved_count <> 1 then
        raise exception 'First wiggle call should process one tower after reset, saw %', moved_count;
    end if;

    select h3, recalculation_count
    into relocated_h3, route_recalc
    from mesh_towers
    where source = 'route';

    if relocated_h3 is distinct from route_candidate then
        raise exception 'Route tower did not relocate to expected cell %; current placement %',
            route_candidate::text,
            coalesce(relocated_h3::text, '<null>');
    end if;

    if route_recalc <> 1 then
        raise exception 'Route tower recalculation_count should increment to 1 after wiggle, got % for tower at %',
            route_recalc,
            relocated_h3::text;
    end if;

    select mesh_tower_wiggle(false) into moved_count;

    if moved_count <> 1 then
        raise exception 'Second wiggle call should process neighboring cluster tower, saw %', moved_count;
    end if;

    select recalculation_count
    into cluster_recalc
    from mesh_towers
    where source = 'cluster_slim';

    if cluster_recalc <> 1 then
        raise exception 'Cluster tower should be re-dirtied after neighbor move; expected recalculation_count 1 but saw %',
            cluster_recalc;
    end if;

    if exists (
        select 1
        from mesh_surface_h3_r8
        where h3 = route_anchor
          and has_tower
    ) then
        raise exception 'Old route anchor % still marked as tower after relocation', route_anchor::text;
    end if;

    if not exists (
        select 1
        from mesh_surface_h3_r8
        where h3 = route_candidate
          and has_tower
    ) then
        raise exception 'Route candidate % not marked as tower after relocation', route_candidate::text;
    end if;

    select mesh_tower_wiggle(false) into moved_count;

    if moved_count <> 0 then
        raise exception 'Wiggle loop should stop once queue is clean, last call returned %', moved_count;
    end if;
end;
$$;

rollback;
