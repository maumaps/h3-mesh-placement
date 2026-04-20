set client_min_messages = warning;

begin;

create temporary table mesh_pipeline_settings (
    setting text primary key,
    value text not null
) on commit drop;

insert into mesh_pipeline_settings (setting, value)
values
    ('enable_population_anchor_contract', 'true'),
    ('population_anchor_contract_distance_m', '0'),
    ('mast_height_m', '28'),
    ('frequency_hz', '868000000');

create temporary table mesh_surface_h3_r8 (
    h3 h3index primary key,
    centroid_geog public.geography not null,
    has_tower boolean default false,
    is_in_boundaries boolean default true,
    has_road boolean default true,
    has_building boolean default true,
    is_in_unfit_area boolean default false,
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
    ('soft_population', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.000, 0.000), 4326), 8)),
    ('soft_replacement', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.010, 0.000), 4326), 8)),
    ('soft_leaf', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.020, 0.000), 4326), 8)),
    ('blocked_population', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(1.000, 0.000), 4326), 8)),
    ('blocked_route_a', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(1.010, 0.000), 4326), 8)),
    ('blocked_route_b', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(1.020, 0.000), 4326), 8)),
    ('high_population', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(2.000, 0.000), 4326), 8)),
    ('high_replacement', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(2.010, 0.000), 4326), 8)),
    ('high_leaf', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(2.020, 0.000), 4326), 8)),
    ('synthetic_population', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(4.000, 0.000), 4326), 8)),
    ('synthetic_route', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(4.050, 0.000), 4326), 8)),
    ('synthetic_candidate', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(4.025, 0.000), 4326), 8)),
    ('synthetic_required_a', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(4.200, 0.000), 4326), 8)),
    ('synthetic_required_b', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(4.220, 0.000), 4326), 8)),
    ('synthetic_required_c', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(4.240, 0.000), 4326), 8)),
    ('close_route_a', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(3.000, 0.000), 4326), 8)),
    ('close_route_b', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(3.010, 0.000), 4326), 8));

insert into mesh_surface_h3_r8 (h3, centroid_geog, has_tower, population, population_70km, building_count)
select
    h3,
    h3_cell_to_geometry(h3)::public.geography,
    label <> 'synthetic_candidate',
    case label when 'high_population' then 100000 else 1 end,
    100000,
    1
from test_cells;

insert into mesh_towers (tower_id, h3, source)
select *
from (
    values
        (100, (select h3 from test_cells where label = 'soft_population'), 'population'),
        (101, (select h3 from test_cells where label = 'soft_replacement'), 'route'),
        (102, (select h3 from test_cells where label = 'soft_leaf'), 'route'),
        (200, (select h3 from test_cells where label = 'blocked_population'), 'population'),
        (201, (select h3 from test_cells where label = 'blocked_route_a'), 'route'),
        (202, (select h3 from test_cells where label = 'blocked_route_b'), 'route'),
        (300, (select h3 from test_cells where label = 'high_population'), 'population'),
        (301, (select h3 from test_cells where label = 'high_replacement'), 'route'),
        (302, (select h3 from test_cells where label = 'high_leaf'), 'route'),
        (500, (select h3 from test_cells where label = 'synthetic_population'), 'population'),
        (501, (select h3 from test_cells where label = 'synthetic_route'), 'route'),
        (502, (select h3 from test_cells where label = 'synthetic_required_a'), 'route'),
        (503, (select h3 from test_cells where label = 'synthetic_required_b'), 'route'),
        (504, (select h3 from test_cells where label = 'synthetic_required_c'), 'route'),
        (401, (select h3 from test_cells where label = 'close_route_a'), 'route'),
        (402, (select h3 from test_cells where label = 'close_route_b'), 'route')
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
        ('soft_population', 'soft_replacement'),
        ('soft_population', 'soft_leaf'),
        ('soft_replacement', 'soft_leaf'),
        ('blocked_population', 'blocked_route_a'),
        ('blocked_population', 'blocked_route_b'),
        ('high_population', 'high_replacement'),
        ('high_population', 'high_leaf'),
        ('high_replacement', 'high_leaf'),
        ('synthetic_population', 'synthetic_route'),
        ('synthetic_population', 'synthetic_required_a'),
        ('synthetic_population', 'synthetic_required_c'),
        ('synthetic_route', 'synthetic_required_b'),
        ('synthetic_route', 'synthetic_required_c'),
        ('synthetic_candidate', 'synthetic_required_a'),
        ('synthetic_candidate', 'synthetic_required_b'),
        ('synthetic_candidate', 'synthetic_required_c'),
        ('close_route_a', 'close_route_b')
) as links(src_label, dst_label)
join test_cells src on src.label = links.src_label
join test_cells dst on dst.label = links.dst_label;
