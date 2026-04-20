set client_min_messages = warning;

begin;

-- Shadow the route bootstrap table so this test validates row-category invariants without live pipeline state.
create temporary table mesh_route_bootstrap_pairs (
    src_h3 h3index not null,
    dst_h3 h3index not null,
    distance_m double precision not null,
    src_point_kind text not null,
    dst_point_kind text not null
) on commit drop;

-- Shadow the surface table with every H3 referenced by the bootstrap fixture.
create temporary table mesh_surface_h3_r8 (
    h3 h3index primary key
) on commit drop;

-- Build a compact Georgia-area fixture covering placed towers, manual points, peaks, and coarse cluster links.
with fixture_points as (
    select *
    from (
        values
            ('placed_a', 44.77012421468743, 41.72621783475549),
            ('manual_a', 44.79012421468743, 41.72621783475549),
            ('peak_a', 44.82012421468743, 41.73621783475549),
            ('placed_b', 44.85012421468743, 41.74621783475549),
            ('coarse_a', 44.88012421468743, 41.75621783475549),
            ('placed_c', 44.91012421468743, 41.76621783475549)
    ) as rows(name, lon, lat)
), fixture_cells as (
    select
        name,
        h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(lon, lat), 4326), 8) as h3
    from fixture_points
), inserted_surface as (
    insert into mesh_surface_h3_r8 (h3)
    select distinct h3
    from fixture_cells
    returning h3
), pair_specs as (
    select *
    from (
        values
            ('placed_a', 'manual_a', 'placed_tower', 'manual_point'),
            ('peak_a', 'placed_b', 'peak_nearest_placeable', 'placed_tower'),
            ('coarse_a', 'placed_c', 'coarse_cluster_link', 'placed_tower')
    ) as rows(src_name, dst_name, src_point_kind, dst_point_kind)
)
insert into mesh_route_bootstrap_pairs (
    src_h3,
    dst_h3,
    distance_m,
    src_point_kind,
    dst_point_kind
)
select
    src.h3,
    dst.h3,
    ST_Distance(src.h3::geography, dst.h3::geography),
    pair_specs.src_point_kind,
    pair_specs.dst_point_kind
from pair_specs
join fixture_cells src on src.name = pair_specs.src_name
join fixture_cells dst on dst.name = pair_specs.dst_name;

do
$$
declare
    pair_count integer;
    invalid_distance_count integer;
    missing_surface_count integer;
    short_pair_count integer;
    placed_tower_pair_count integer;
    manual_pair_count integer;
    peak_pair_count integer;
    coarse_cluster_pair_count integer;
begin
    select count(*)
    into pair_count
    from mesh_route_bootstrap_pairs;

    if pair_count <= 0 then
        raise exception 'mesh_route_bootstrap_pairs should contain at least one CSV-derived pair, found %', pair_count;
    end if;

    select count(*)
    into invalid_distance_count
    from mesh_route_bootstrap_pairs
    where distance_m <= 0
       or distance_m > 80000;

    if invalid_distance_count <> 0 then
        raise exception 'mesh_route_bootstrap_pairs should keep only in-range LOS pairs, found % invalid rows', invalid_distance_count;
    end if;

    select count(*)
    into missing_surface_count
    from mesh_route_bootstrap_pairs bp
    left join mesh_surface_h3_r8 src_surface on src_surface.h3 = bp.src_h3
    left join mesh_surface_h3_r8 dst_surface on dst_surface.h3 = bp.dst_h3
    where src_surface.h3 is null
       or dst_surface.h3 is null;

    if missing_surface_count <> 0 then
        raise exception 'mesh_route_bootstrap_pairs should reference only surface H3 cells, found % missing-surface rows', missing_surface_count;
    end if;

    select count(*)
    into short_pair_count
    from mesh_route_bootstrap_pairs
    where distance_m < 10000;

    if short_pair_count <= 0 then
        raise exception 'mesh_route_bootstrap_pairs should include nearby CSV bootstrap pairs so routing can start from realistic corridors, found % short pairs', short_pair_count;
    end if;

    select count(*)
    into placed_tower_pair_count
    from mesh_route_bootstrap_pairs
    where src_point_kind = 'placed_tower'
       or dst_point_kind = 'placed_tower';

    if placed_tower_pair_count <= 0 then
        raise exception 'mesh_route_bootstrap_pairs should anchor bootstrap warmup to currently placed mesh towers, found % placed-tower pairs', placed_tower_pair_count;
    end if;

    select count(*)
    into manual_pair_count
    from mesh_route_bootstrap_pairs
    where src_point_kind = 'manual_point'
       or dst_point_kind = 'manual_point';

    if manual_pair_count <= 0 then
        raise exception 'mesh_route_bootstrap_pairs should include manually curated warmup points such as data/in/install_priority_bootstrap_manual.csv, found % manual pairs', manual_pair_count;
    end if;

    select count(*)
    into peak_pair_count
    from mesh_route_bootstrap_pairs
    where src_point_kind = 'peak_nearest_placeable'
       or dst_point_kind = 'peak_nearest_placeable';

    if peak_pair_count <= 0 then
        raise exception 'mesh_route_bootstrap_pairs should include nearest placeable OSM peaks for mountain bootstrap warmup, found % peak pairs', peak_pair_count;
    end if;

    select count(*)
    into coarse_cluster_pair_count
    from mesh_route_bootstrap_pairs
    where src_point_kind = 'coarse_cluster_link'
       or dst_point_kind = 'coarse_cluster_link';

    if coarse_cluster_pair_count <= 0 then
        raise exception 'mesh_route_bootstrap_pairs should include explicit disconnected coarse-cluster tower links so cache warmup can target remaining coarse gaps, found % coarse-cluster pairs', coarse_cluster_pair_count;
    end if;
end;
$$;

rollback;
