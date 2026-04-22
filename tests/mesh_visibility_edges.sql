set client_min_messages = warning;

-- Ensure mesh_visibility_edges-style rows match cached h3_visibility_clearance() expectations.
begin;

-- Shadow seed-node H3 data with the documented H3 talk fixture points.
create temporary table mesh_initial_nodes_h3_r8 (
    name text primary key,
    h3 h3index not null unique
) on commit drop;

-- Shadow tower IDs so pgRouting hop checks do not depend on the live tower registry.
create temporary table mesh_towers (
    tower_id integer primary key,
    h3 h3index not null unique
) on commit drop;

-- Shadow visibility edges with just the columns this regression validates.
create temporary table mesh_visibility_edges (
    source_id integer not null,
    target_id integer not null,
    source_h3 h3index not null,
    target_h3 h3index not null,
    is_visible boolean not null,
    distance_m double precision not null,
    cluster_hops integer,
    primary key (source_id, target_id)
) on commit drop;

-- Shadow the LOS cache so h3_visibility_clearance() reads fixture rows only.
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

-- Seed documented towers, expected pair visibility, and cached LOS metrics.
with seed_nodes as (
    select *
    from (
        values
            (1, 'Poti', 41.661468062064046, 42.138160267820865),
            (2, 'Feria 2', 41.65617597380452, 41.62629099133622),
            (3, 'SoNick', 41.56250667923871, 41.546406233870215),
            (4, 'Komzpa', 41.590687899376945, 41.62120240464702),
            (5, 'Tbilisi hackerspace', 44.77012421468743, 41.72621783475549)
    ) as rows(tower_id, name, lon, lat)
), inserted_nodes as (
    insert into mesh_initial_nodes_h3_r8 (name, h3)
    select
        name,
        h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(lon, lat), 4326), 8)
    from seed_nodes
    returning name, h3
)
insert into mesh_towers (tower_id, h3)
select seed_nodes.tower_id, inserted_nodes.h3
from seed_nodes
join inserted_nodes using (name);

with pair_expectations as (
    select *
    from (
        values
            ('Poti', 'Feria 2', true, 1),
            ('Poti', 'SoNick', true, 1),
            ('Poti', 'Komzpa', true, 1),
            ('Feria 2', 'Komzpa', true, 1),
            ('Feria 2', 'SoNick', true, 1),
            ('Komzpa', 'SoNick', false, 2),
            ('Tbilisi hackerspace', 'Poti', false, null::integer),
            ('Tbilisi hackerspace', 'Feria 2', false, null::integer),
            ('Tbilisi hackerspace', 'Komzpa', false, null::integer),
            ('Tbilisi hackerspace', 'SoNick', false, null::integer)
    ) as rows(src_name, dst_name, expected_visible, cluster_hops)
), prepared_pairs as (
    select
        least(src_tower.tower_id, dst_tower.tower_id) as source_id,
        greatest(src_tower.tower_id, dst_tower.tower_id) as target_id,
        case when src_tower.tower_id < dst_tower.tower_id then src.h3 else dst.h3 end as source_h3,
        case when src_tower.tower_id < dst_tower.tower_id then dst.h3 else src.h3 end as target_h3,
        least(src.h3, dst.h3) as cache_src_h3,
        greatest(src.h3, dst.h3) as cache_dst_h3,
        pair_expectations.expected_visible,
        pair_expectations.cluster_hops,
        ST_Distance(src.h3::geography, dst.h3::geography) as distance_m
    from pair_expectations
    join mesh_initial_nodes_h3_r8 src on src.name = pair_expectations.src_name
    join mesh_initial_nodes_h3_r8 dst on dst.name = pair_expectations.dst_name
    join mesh_towers src_tower on src_tower.h3 = src.h3
    join mesh_towers dst_tower on dst_tower.h3 = dst.h3
), inserted_edges as (
    insert into mesh_visibility_edges (
        source_id,
        target_id,
        source_h3,
        target_h3,
        is_visible,
        distance_m,
        cluster_hops
    )
    select
        source_id,
        target_id,
        source_h3,
        target_h3,
        expected_visible,
        distance_m,
        cluster_hops
    from prepared_pairs
    returning source_id
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
    cache_src_h3,
    cache_dst_h3,
    28,
    28,
    868e6,
    distance_m,
    case when expected_visible then 1 else -1 end,
    distance_m / 2,
    distance_m / 2,
    100
from prepared_pairs;

do
$$
declare
    rec record;
    constant_mast_height constant double precision := 28;
    constant_frequency constant double precision := 868e6;
begin
    for rec in
        with pairs as (
            select
                least(e.source_h3, e.target_h3) as src_h3,
                greatest(e.source_h3, e.target_h3) as dst_h3,
                coalesce(a.name, 'seed') as src_name,
                coalesce(b.name, 'seed') as dst_name,
                h3_visibility_clearance(
                    e.source_h3,
                    e.target_h3,
                    constant_mast_height,
                    constant_mast_height,
                    constant_frequency
                ) as clearance,
                e.distance_m,
                e.is_visible as stored_visible
            from mesh_visibility_edges e
            join mesh_initial_nodes_h3_r8 a on a.h3 = e.source_h3
            join mesh_initial_nodes_h3_r8 b on b.h3 = e.target_h3
        )
        select
            p.src_h3,
            p.dst_h3,
            p.src_name,
            p.dst_name,
            p.distance_m,
            p.clearance,
            (p.clearance > 0) as expected_visible,
            p.stored_visible
        from pairs p
    loop
        if rec.stored_visible is null then
            raise exception 'Seed visibility edge missing for % (%s) -> % (%s) at %.2f km; clearance % meters and expected visibility %',
                rec.src_name, rec.src_h3::text, rec.dst_name, rec.dst_h3::text, rec.distance_m / 1000.0, rec.clearance, rec.expected_visible;
        end if;

        if rec.stored_visible is distinct from rec.expected_visible then
            raise exception 'Seed visibility mismatch for % (%s) -> % (%s): stored % but clearance % meters implies %',
                rec.src_name, rec.src_h3::text, rec.dst_name, rec.dst_h3::text, rec.stored_visible, rec.clearance, rec.expected_visible;
        end if;
    end loop;
end;
$$;

do
$$
declare
    rec record;
begin
    for rec in
        with expectations as (
            select
                least(src_name, dst_name) as src_name,
                greatest(src_name, dst_name) as dst_name,
                expected_visible
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
            ) as v(src_name, dst_name, expected_visible)
        ),
        actual as (
            select
                least(coalesce(a.name, 'seed'), coalesce(b.name, 'seed')) as src_name,
                greatest(coalesce(a.name, 'seed'), coalesce(b.name, 'seed')) as dst_name,
                e.is_visible
            from mesh_visibility_edges e
            join mesh_initial_nodes_h3_r8 a
              on a.h3 = e.source_h3
            join mesh_initial_nodes_h3_r8 b
              on b.h3 = e.target_h3
        )
        select
            exp.src_name,
            exp.dst_name,
            exp.expected_visible,
            act.is_visible
        from expectations exp
        left join actual act
          on act.src_name = exp.src_name
         and act.dst_name = exp.dst_name
    loop
        if rec.is_visible is null then
            raise exception 'Seed visibility row missing for % -> %; expected visibility % per doc/H3 ground truth',
                rec.src_name, rec.dst_name, rec.expected_visible;
        end if;

        if rec.is_visible is distinct from rec.expected_visible then
            raise exception 'Seed visibility row mismatch for % -> %: stored %, expected % per doc/H3 ground truth',
                rec.src_name, rec.dst_name, rec.is_visible, rec.expected_visible;
        end if;
    end loop;
end;
$$;

-- Temporary helper storing LOS edges (<=100 km) so we can recompute hop counts for seed towers.
create temporary table tmp_test_visibility_cluster_edges as
select
    row_number() over () as edge_id,
    e.source_id,
    e.target_id,
    1::double precision as cost
from mesh_visibility_edges e
where e.is_visible
  and e.distance_m <= 100000;

do
$$
declare
    rec record;
begin
    for rec in
        with seed_towers as (
            -- Restrict hop validation to the documented seed towers so the loop stays small.
            select t.tower_id
            from mesh_towers t
            join mesh_initial_nodes_h3_r8 seeds on seeds.h3 = t.h3
        ),
        expected as (
            -- Recompute hop counts between every connected seed pair using pgRouting for comparison.
            select
                least(result.start_vid, result.end_vid) as source_id,
                greatest(result.start_vid, result.end_vid) as target_id,
                result.agg_cost::integer as hops
            from pgr_dijkstra(
                'select edge_id as id, source_id as source, target_id as target, cost, cost as reverse_cost from tmp_test_visibility_cluster_edges',
                coalesce(array(select tower_id::bigint from seed_towers order by tower_id), array[]::bigint[]),
                coalesce(array(select tower_id::bigint from seed_towers order by tower_id), array[]::bigint[]),
                false
            ) as result
            where result.start_vid < result.end_vid
              and result.agg_cost < 'Infinity'::double precision
              and result.node = result.end_vid
        )
        select
            e.source_id,
            e.target_id,
            exp.hops,
            e.cluster_hops
        from expected exp
        join mesh_visibility_edges e
          on e.source_id = exp.source_id
         and e.target_id = exp.target_id
    loop
        if rec.cluster_hops is null then
            raise exception 'cluster_hops missing for seed pair % -> %; expected % hops', rec.source_id, rec.target_id, rec.hops;
        end if;

        if rec.cluster_hops <> rec.hops then
            raise exception 'cluster_hops mismatch for seed pair % -> %: stored %, expected %', rec.source_id, rec.target_id, rec.cluster_hops, rec.hops;
        end if;
    end loop;
end;
$$;

rollback;
