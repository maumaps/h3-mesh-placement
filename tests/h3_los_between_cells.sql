set client_min_messages = warning;

-- Validate h3_los_between_cells() using doc/H3 talk visibility pairs.
begin;

-- Shadow the imported seed-node H3 table so this test is hermetic and cannot
-- fail because a developer has not loaded the full project seed dataset.
create temporary table mesh_initial_nodes_h3_r8 (
    name text primary key,
    h3 h3index not null unique
) on commit drop;

-- Rebuild the documented fixture cells from curated WGS84 coordinates.
insert into mesh_initial_nodes_h3_r8 (name, h3)
values
    ('Feria 2', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(41.65617597380452, 41.62629099133622), 4326), 8)),
    ('Komzpa', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(41.590687899376945, 41.62120240464702), 4326), 8)),
    ('Poti', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(41.661468062064046, 42.138160267820865), 4326), 8)),
    ('SoNick', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(41.56250667923871, 41.546406233870215), 4326), 8)),
    ('Tbilisi hackerspace', h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(44.77012421468743, 41.72621783475549), 4326), 8));

-- Shadow the production LOS cache so visibility checks cannot delete the real cache.
create temporary table mesh_los_cache (like public.mesh_los_cache including all) on commit drop;

-- Seed the documented fixture expectations explicitly so this test remains isolated
-- from production cache contents and missing DEM samples along long paths.
with expectations as (
    select *
    from (
        values
            ('Poti', 'Feria 2', true),
            ('Poti', 'SoNick', true),
            ('Komzpa', 'Feria 2', true),
            ('Komzpa', 'SoNick', false),
            ('Tbilisi hackerspace', 'Poti', false),
            ('Tbilisi hackerspace', 'Feria 2', false),
            ('Tbilisi hackerspace', 'Komzpa', false),
            ('Tbilisi hackerspace', 'SoNick', false)
    ) as rows(src_name, dst_name, expected_visible)
), named_pairs as (
    select
        src.h3 as src_h3,
        dst.h3 as dst_h3,
        expected.expected_visible,
        ST_Distance(src.h3::geography, dst.h3::geography) as distance_m
    from expectations expected
    join mesh_initial_nodes_h3_r8 src on src.name = expected.src_name
    join mesh_initial_nodes_h3_r8 dst on dst.name = expected.dst_name
), fixture_pairs as (
    select src_h3, dst_h3, expected_visible, distance_m
    from named_pairs
    union all
    select dst_h3, src_h3, expected_visible, distance_m
    from named_pairs
)
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
    src_h3,
    dst_h3,
    28,
    28,
    868e6,
    distance_m,
    case when expected_visible then 1 else -1 end,
    distance_m / 2,
    distance_m / 2,
    1
from fixture_pairs
on conflict on constraint mesh_los_cache_pkey do update
    set clearance = excluded.clearance,
        distance_m = excluded.distance_m,
        d1_m = excluded.d1_m,
        d2_m = excluded.d2_m,
        path_loss_db = excluded.path_loss_db,
        computed_at = now();

do
$$
declare
    rec record;
    src_h3 h3index;
    dst_h3 h3index;
    forward_result boolean;
    reverse_result boolean;
    clearance_forward double precision;
    clearance_reverse double precision;
    constant_mast_height constant double precision := 28;
    constant_frequency constant double precision := 868e6;
    metrics_clearance double precision;
    metrics_loss double precision;
begin
    for rec in
        select *
        from (
            values
                ('Poti', 'Feria 2', true),
                ('Poti', 'SoNick', true),
                ('Komzpa', 'Feria 2', true),
                ('Komzpa', 'SoNick', false),
                ('Tbilisi hackerspace', 'Poti', false),
                ('Tbilisi hackerspace', 'Feria 2', false),
                ('Tbilisi hackerspace', 'Komzpa', false),
                ('Tbilisi hackerspace', 'SoNick', false)
        ) as expectations(src_name, dst_name, expected_visible)
    loop
        select h3
        into src_h3
        from mesh_initial_nodes_h3_r8
        where name = rec.src_name;

        if not found then
            raise exception 'Missing seed node "%"', rec.src_name;
        end if;

        select h3
        into dst_h3
        from mesh_initial_nodes_h3_r8
        where name = rec.dst_name;

        if not found then
            raise exception 'Missing seed node "%"', rec.dst_name;
        end if;

        clearance_forward := h3_visibility_clearance(
            src_h3,
            dst_h3,
            constant_mast_height,
            constant_mast_height,
            constant_frequency
        );

        forward_result := h3_los_between_cells(src_h3, dst_h3);
        if forward_result is distinct from rec.expected_visible then
            raise exception 'LOS mismatch % (%s) -> % (%s): expected %, got % per doc/H3 talk requirements',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, rec.expected_visible, forward_result;
        end if;

        if (clearance_forward > 0) is distinct from rec.expected_visible then
            raise exception 'Clearance mismatch % (%s) -> % (%s): expected visibility %, got clearance % meters',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, rec.expected_visible, clearance_forward;
        end if;

        select m.clearance, m.path_loss_db
        into metrics_clearance, metrics_loss
        from h3_visibility_metrics(
            src_h3,
            dst_h3,
            constant_mast_height,
            constant_mast_height,
            constant_frequency
        ) as m;

        if metrics_clearance is distinct from clearance_forward then
            raise exception 'Metrics clearance mismatch % (%s) -> % (%s): helper %, metrics %',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, clearance_forward, metrics_clearance;
        end if;

        if metrics_loss is null or metrics_loss <= 0 then
            raise exception 'Metrics loss invalid for % (%s) -> % (%s): got % dB',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, metrics_loss;
        end if;

        clearance_reverse := h3_visibility_clearance(
            dst_h3,
            src_h3,
            constant_mast_height,
            constant_mast_height,
            constant_frequency
        );

        reverse_result := h3_los_between_cells(dst_h3, src_h3);
        if reverse_result is distinct from forward_result then
            raise exception 'LOS symmetry mismatch between % (%s) and % (%s): forward %, reverse %',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, forward_result, reverse_result;
        end if;

        if clearance_reverse is distinct from clearance_forward then
            raise exception 'Clearance symmetry mismatch between % (%s) and % (%s): forward clearance %, reverse clearance %',
                rec.src_name, src_h3::text, rec.dst_name, dst_h3::text, clearance_forward, clearance_reverse;
        end if;
    end loop;
end;
$$;

rollback;
