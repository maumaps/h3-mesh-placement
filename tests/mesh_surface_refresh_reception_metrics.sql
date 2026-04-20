set client_min_messages = warning;

begin;

-- Shadow the surface table so reception refresh writes only fixture rows.
create temporary table mesh_surface_h3_r8 (
    h3 h3index primary key,
    centroid_geog geography not null,
    has_tower boolean not null default false,
    distance_to_closest_tower double precision not null,
    clearance double precision,
    path_loss double precision
) on commit drop;

-- Shadow the tower registry with the tower used for nearest-neighbor metrics.
create temporary table mesh_towers (
    h3 h3index primary key,
    centroid_geog geography not null
) on commit drop;

-- Shadow the LOS cache so h3_visibility_metrics() returns the documented fixture values.
create temporary table mesh_los_cache (
    src_h3 h3index not null,
    dst_h3 h3index not null,
    mast_height_src double precision not null,
    mast_height_dst double precision not null,
    frequency_hz double precision not null,
    distance_m double precision not null,
    clearance double precision not null,
    d1_m double precision not null,
    d2_m double precision not null,
    path_loss_db double precision not null,
    computed_at timestamptz not null default now(),
    primary key (src_h3, dst_h3, mast_height_src, mast_height_dst, frequency_hz)
) on commit drop;

do
$$
declare
    tower_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(44.77012421468743, 41.72621783475549), 4326), 8);
    surface_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(44.79012421468743, 41.72621783475549), 4326), 8);
    link_distance double precision;
    refreshed_clearance double precision;
    refreshed_path_loss double precision;
begin
    link_distance := ST_Distance(surface_h3::geography, tower_h3::geography);

    if surface_h3 = tower_h3 then
        raise exception 'Reception refresh fixture needs distinct H3 cells, but both coordinates mapped to %', tower_h3::text;
    end if;

    insert into mesh_surface_h3_r8 (
        h3,
        centroid_geog,
        has_tower,
        distance_to_closest_tower,
        clearance,
        path_loss
    )
    values
        (tower_h3, tower_h3::geography, true, 0, null, null),
        (surface_h3, surface_h3::geography, false, link_distance, null, null);

    insert into mesh_towers (h3, centroid_geog)
    values (tower_h3, tower_h3::geography);

    insert into mesh_los_cache (
        src_h3,
        dst_h3,
        mast_height_src,
        mast_height_dst,
        frequency_hz,
        distance_m,
        clearance,
        d1_m,
        d2_m,
        path_loss_db
    )
    values (
        least(surface_h3, tower_h3),
        greatest(surface_h3, tower_h3),
        28,
        28,
        868e6,
        link_distance,
        12.5,
        link_distance / 2,
        link_distance / 2,
        99
    );

    perform mesh_surface_refresh_reception_metrics(
        tower_h3,
        10000,
        80000,
        5
    );

    select clearance, path_loss
    into refreshed_clearance, refreshed_path_loss
    from mesh_surface_h3_r8
    where h3 = surface_h3;

    if refreshed_clearance is distinct from 12.5 or refreshed_path_loss is distinct from 99 then
        raise exception 'Reception metrics not restored for % near tower %: expected clearance/path_loss 12.5/99, got %/%',
            surface_h3::text,
            tower_h3::text,
            refreshed_clearance,
            refreshed_path_loss;
    end if;
end;
$$;

rollback;
