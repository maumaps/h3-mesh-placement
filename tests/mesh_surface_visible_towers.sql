set client_min_messages = warning;

begin;

-- Shadow the surface table so the consistency check cannot depend on stale live metrics.
create temporary table mesh_surface_h3_r8 (
    h3 h3index primary key,
    centroid_geog geography not null,
    distance_to_closest_tower double precision not null,
    visible_tower_count integer not null
) on commit drop;

-- Shadow the tower registry with one nearby tower used by the fixture cell.
create temporary table mesh_towers (
    h3 h3index primary key,
    centroid_geog geography not null
) on commit drop;

-- Shadow the LOS cache so h3_los_between_cells() cannot compute or write live cache rows.
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

-- Seed one Georgia-area surface cell and one visible tower with cached LOS metrics.
do
$$
declare
    surface_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(44.77012421468743, 41.72621783475549), 4326), 8);
    tower_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(44.79012421468743, 41.72621783475549), 4326), 8);
    link_distance double precision;
begin
    link_distance := ST_Distance(surface_h3::geography, tower_h3::geography);

    if surface_h3 = tower_h3 then
        raise exception 'Visibility fixture needs distinct H3 cells, but both coordinates mapped to %', surface_h3::text;
    end if;

    insert into mesh_surface_h3_r8 (
        h3,
        centroid_geog,
        distance_to_closest_tower,
        visible_tower_count
    )
    values (
        surface_h3,
        surface_h3::geography,
        link_distance,
        1
    );

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
        12,
        link_distance / 2,
        link_distance / 2,
        100
    );
end;
$$;

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
        where distance_to_closest_tower < 80000
        order by distance_to_closest_tower
        limit max_samples
    loop
        samples_checked := samples_checked + 1;

        select count(*)
        into expected
        from mesh_towers t
        where t.h3 <> sample.h3
          and ST_DWithin(sample.centroid_geog, t.centroid_geog, 80000)
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

    if samples_checked <> 1 then
        raise exception 'visible_tower_count fixture should check exactly one surface cell, checked %', samples_checked;
    end if;
end;
$$;

rollback;
