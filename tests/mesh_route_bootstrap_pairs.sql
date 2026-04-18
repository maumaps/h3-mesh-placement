set client_min_messages = warning;

begin;

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
