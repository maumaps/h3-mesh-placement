set client_min_messages = warning;

begin;

create temporary table mesh_pipeline_settings (
    setting text primary key,
    value text not null
) on commit drop;

insert into mesh_pipeline_settings (setting, value)
values
    ('enable_generated_pair_contract', 'true'),
    ('generated_tower_merge_distance_m', '10000'),
    ('mast_height_m', '28'),
    ('frequency_hz', '868000000');

create temporary table mesh_surface_h3_r8 (
    h3 h3index primary key,
    centroid_geog public.geography not null,
    has_tower boolean default false,
    is_in_boundaries boolean default true,
    has_road boolean default true,
    is_in_unfit_area boolean default false,
    has_building boolean default true,
    population numeric default 0,
    population_70km numeric default 0,
    visible_population numeric,
    building_count integer default 1,
    distance_to_closest_tower double precision,
    clearance double precision,
    path_loss double precision,
    visible_uncovered_population numeric
) on commit drop;

create temporary table mesh_towers (
    tower_id integer primary key,
    h3 h3index not null unique,
    source text not null,
    recalculation_count integer not null default 0,
    centroid_geog public.geography generated always as (h3_cell_to_geometry(h3)::public.geography) stored
) on commit drop;

create temporary table mesh_tower_wiggle_queue (
    tower_id integer primary key,
    is_dirty boolean not null default true
) on commit drop;

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

create temporary table test_cells (
    label text primary key,
    h3 h3index not null unique
) on commit drop;

insert into test_cells (label, h3)
values
    ('left_route', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.000, 0.000), 4326), 8)),
    ('right_route', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.020, 0.000), 4326), 8)),
    ('synthetic_route', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.010, 0.000), 4326), 8)),
    ('required_left', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(-0.030, 0.000), 4326), 8)),
    ('required_right', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.050, 0.000), 4326), 8)),
    ('blocked_left', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(1.000, 0.000), 4326), 8)),
    ('blocked_right', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(1.020, 0.000), 4326), 8)),
    ('blocked_required_left', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.970, 0.000), 4326), 8)),
    ('blocked_required_right', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(1.050, 0.000), 4326), 8));

insert into mesh_surface_h3_r8 (h3, centroid_geog, has_tower, population_70km)
select
    h3,
    h3_cell_to_geometry(h3)::public.geography,
    label in ('left_route', 'right_route', 'required_left', 'required_right', 'blocked_left', 'blocked_right', 'blocked_required_left', 'blocked_required_right'),
    case label when 'synthetic_route' then 1000 else 1 end
from test_cells;

insert into mesh_towers (tower_id, h3, source)
select *
from (
    values
        (100, (select h3 from test_cells where label = 'left_route'), 'route'),
        (101, (select h3 from test_cells where label = 'right_route'), 'route'),
        (102, (select h3 from test_cells where label = 'required_left'), 'route'),
        (103, (select h3 from test_cells where label = 'required_right'), 'route'),
        (200, (select h3 from test_cells where label = 'blocked_left'), 'route'),
        (201, (select h3 from test_cells where label = 'blocked_right'), 'route'),
        (202, (select h3 from test_cells where label = 'blocked_required_left'), 'route'),
        (203, (select h3 from test_cells where label = 'blocked_required_right'), 'route')
) as towers(tower_id, h3, source);

insert into mesh_tower_wiggle_queue (tower_id, is_dirty)
select tower_id, true from mesh_towers;

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
select
    least(src.h3, dst.h3),
    greatest(src.h3, dst.h3),
    28,
    28,
    868000000,
    ST_Distance(src.h3::geography, dst.h3::geography),
    1,
    1,
    1,
    1
from (
    values
        ('left_route', 'required_left'),
        ('right_route', 'required_right'),
        ('synthetic_route', 'required_left'),
        ('synthetic_route', 'required_right'),
        ('blocked_left', 'blocked_required_left'),
        ('blocked_right', 'blocked_required_right')
) as links(src_label, dst_label)
join test_cells src on src.label = links.src_label
join test_cells dst on dst.label = links.dst_label;
