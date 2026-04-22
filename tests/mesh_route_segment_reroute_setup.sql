set client_min_messages = notice;

-- Create temporary config with route segment reroute enabled for the fixture.
drop table if exists pg_temp.mesh_pipeline_settings;
create temporary table mesh_pipeline_settings (
    setting text primary key,
    value text not null
) on commit preserve rows;

insert into mesh_pipeline_settings (setting, value)
values
    ('enable_route_segment_reroute', 'true'),
    ('max_los_distance_m', '100000'),
    ('mast_height_m', '28'),
    ('frequency_hz', '868000000'),
    ('route_segment_reroute_candidate_limit', '32'),
    ('route_segment_reroute_max_moves', '8');

-- Create minimal tower registry needed by the reroute pass.
drop table if exists pg_temp.mesh_towers;
create temporary table mesh_towers (
    tower_id integer primary key,
    h3 h3index not null unique,
    source text not null,
    recalculation_count integer not null default 0,
    centroid_geog geography generated always as (h3_cell_to_geometry(h3)::geography) stored
) on commit preserve rows;

-- Create minimal surface table with the scoring fields used by the reroute pass.
drop table if exists pg_temp.mesh_surface_h3_r8;
create temporary table mesh_surface_h3_r8 (
    h3 h3index primary key,
    centroid_geog geography generated always as (h3_cell_to_geometry(h3)::geography) stored,
    has_road boolean,
    is_in_boundaries boolean,
    is_in_unfit_area boolean,
    has_building boolean,
    building_count integer,
    population numeric,
    population_70km numeric,
    has_tower boolean default false,
    clearance double precision,
    path_loss double precision,
    visible_population numeric,
    visible_uncovered_population numeric,
    distance_to_closest_tower double precision
) on commit preserve rows;

-- Create minimal LOS cache; links are stored in canonical H3 order just like production.
create temporary table mesh_los_cache (
    src_h3 h3index not null,
    dst_h3 h3index not null,
    mast_height_src double precision not null,
    mast_height_dst double precision not null,
    frequency_hz double precision not null,
    distance_m double precision not null,
    clearance double precision not null,
    d1_m double precision not null default 0,
    d2_m double precision not null default 0,
    path_loss_db double precision not null default 0,
    primary key (src_h3, dst_h3, mast_height_src, mast_height_dst, frequency_hz)
) on commit preserve rows;

-- Create a wiggle queue stub so the reroute pass can mark moved towers dirty.
drop table if exists pg_temp.mesh_tower_wiggle_queue;
create temporary table mesh_tower_wiggle_queue (
    tower_id integer primary key,
    is_dirty boolean not null default false
) on commit preserve rows;

-- Insert the route chain that should improve: endpoint 1 -> relay 2 -> relay 3 -> endpoint 4.
insert into mesh_towers (tower_id, h3, source)
values
    (1, '882c2e99c7fffff'::h3index, 'route'),
    (2, '882c2e419dfffff'::h3index, 'route'),
    (3, '882c2e566dfffff'::h3index, 'route'),
    (4, '882c05b52bfffff'::h3index, 'route'),
    (10, '882c2c6a07fffff'::h3index, 'route'),
    (11, '882c2c6d61fffff'::h3index, 'route'),
    (12, '882c285b63fffff'::h3index, 'route'),
    (13, '882c28583dfffff'::h3index, 'route');

-- Insert old, replacement, and blocked-chain surface cells.
insert into mesh_surface_h3_r8 (
    h3,
    has_road,
    is_in_boundaries,
    is_in_unfit_area,
    has_building,
    building_count,
    population,
    population_70km,
    has_tower,
    distance_to_closest_tower
)
values
    ('882c2e99c7fffff'::h3index, true, true, false, false, 0, 1, 800000, true, 0),
    ('882c2e419dfffff'::h3index, true, true, false, false, 0, 1, 390000, true, 0),
    ('882c2e566dfffff'::h3index, true, true, false, false, 0, 1, 350000, true, 0),
    ('882c05b52bfffff'::h3index, true, true, false, true, 9, 62, 590000, true, 0),
    ('882c2e4e07fffff'::h3index, true, true, false, true, 22, 37, 414110, false, 10000),
    ('882c05b747fffff'::h3index, true, true, false, false, 0, 1, 511088, false, 10000),
    ('882c2c6a07fffff'::h3index, true, true, false, false, 0, 1, 620000, true, 0),
    ('882c2c6d61fffff'::h3index, true, true, false, false, 0, 1, 590000, true, 0),
    ('882c285b63fffff'::h3index, true, true, false, true, 81, 237, 1646000, true, 0),
    ('882c28583dfffff'::h3index, true, true, false, true, 5, 21, 1683000, true, 0),
    ('882c2c6d69fffff'::h3index, true, true, false, true, 99, 500, 2000000, false, 10000),
    ('882c2c6d6dfffff'::h3index, true, true, false, true, 99, 500, 2000000, false, 10000);

insert into mesh_tower_wiggle_queue (tower_id, is_dirty)
select tower_id, false
from mesh_towers;

-- Insert cached visible links for both the improvable chain and the blocked chain.
do $$
declare
    link_pairs h3index[][] := array[
        array['882c2e99c7fffff'::h3index, '882c2e419dfffff'::h3index],
        array['882c2e419dfffff'::h3index, '882c2e566dfffff'::h3index],
        array['882c2e566dfffff'::h3index, '882c05b52bfffff'::h3index],
        array['882c2e99c7fffff'::h3index, '882c2e4e07fffff'::h3index],
        array['882c2e4e07fffff'::h3index, '882c05b747fffff'::h3index],
        array['882c05b747fffff'::h3index, '882c05b52bfffff'::h3index],
        array['882c2c6a07fffff'::h3index, '882c2c6d61fffff'::h3index],
        array['882c2c6d61fffff'::h3index, '882c28583dfffff'::h3index],
        array['882c2c6d61fffff'::h3index, '882c285b63fffff'::h3index],
        array['882c2c6a07fffff'::h3index, '882c2c6d69fffff'::h3index],
        array['882c2c6d69fffff'::h3index, '882c2c6d6dfffff'::h3index],
        array['882c2c6d6dfffff'::h3index, '882c28583dfffff'::h3index]
    ];
    link_pair h3index[];
begin
    foreach link_pair slice 1 in array link_pairs loop
        insert into mesh_los_cache (
            src_h3,
            dst_h3,
            mast_height_src,
            mast_height_dst,
            frequency_hz,
            distance_m,
            clearance
        )
        values (
            least(link_pair[1], link_pair[2]),
            greatest(link_pair[1], link_pair[2]),
            28,
            28,
            868000000,
            ST_Distance(link_pair[1]::geography, link_pair[2]::geography),
            1
        )
        on conflict do nothing;
    end loop;
end $$;
