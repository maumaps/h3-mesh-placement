set client_min_messages = notice;

-- Pull user-tunable RF constants from the single pipeline config.
select value::double precision as mast_height
from mesh_pipeline_settings
where setting = 'mast_height_m'
\gset

select value::double precision as frequency
from mesh_pipeline_settings
where setting = 'frequency_hz'
\gset

-- Seed LOS cache entries from install-priority bootstrap pairs before the
-- generic all-pairs fill runs, so routing starts with known rollout corridors.
drop table if exists mesh_route_bootstrap_missing_pairs;
create table mesh_route_bootstrap_missing_pairs as
select
    bp.src_h3,
    bp.dst_h3,
    bp.bootstrap_rank,
    bp.distance_m
from mesh_route_bootstrap_pairs bp
left join mesh_los_cache mlc
    on mlc.src_h3 = bp.src_h3
   and mlc.dst_h3 = bp.dst_h3
   and mlc.mast_height_src = :mast_height
   and mlc.mast_height_dst = :mast_height
   and mlc.frequency_hz = :frequency
where mlc.src_h3 is null;

create index if not exists mesh_route_bootstrap_missing_pairs_src_dst_idx
    on mesh_route_bootstrap_missing_pairs (src_h3, dst_h3);

-- Compute LOS metrics for every missing bootstrap pair.
drop table if exists mesh_route_bootstrap_metrics;
create table mesh_route_bootstrap_metrics as
select
    mp.src_h3,
    mp.dst_h3,
    metrics.clearance,
    metrics.path_loss_db,
    metrics.distance_m,
    metrics.d1_m,
    metrics.d2_m
from mesh_route_bootstrap_missing_pairs mp
cross join lateral h3_visibility_clearance_compute(
    mp.src_h3,
    mp.dst_h3,
    :mast_height,
    :mast_height,
    :frequency
) as metrics(clearance, path_loss_db, distance_m, d1_m, d2_m);

-- Persist bootstrap LOS results so the later generic fill can skip them.
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
    path_loss_db,
    computed_at
)
select
    bm.src_h3,
    bm.dst_h3,
    :mast_height,
    :mast_height,
    :frequency,
    bm.distance_m,
    bm.clearance,
    bm.d1_m,
    bm.d2_m,
    bm.path_loss_db,
    now()
from mesh_route_bootstrap_metrics bm
on conflict on constraint mesh_los_cache_pkey do update
    set clearance = excluded.clearance,
        path_loss_db = excluded.path_loss_db,
        d1_m = excluded.d1_m,
        d2_m = excluded.d2_m,
        distance_m = excluded.distance_m,
        computed_at = now();

-- Rebuild the light routing graph from the current cache so mesh_route_bridge
-- can immediately use freshly bootstrapped links before the generic cache fill.
drop table if exists mesh_route_candidate_cells;
create table mesh_route_candidate_cells as
select
    s.h3,
    s.centroid_geog
from mesh_surface_h3_r8 s
where s.has_tower
   or s.can_place_tower;

alter table mesh_route_candidate_cells
    add primary key (h3);

create index if not exists mesh_route_candidate_cells_geog_idx
    on mesh_route_candidate_cells
    using gist (centroid_geog);

-- Only candidate cells and active towers should participate in the route graph.
do
$$
declare
    invalid_candidates integer;
begin
    select count(*)
    into invalid_candidates
    from mesh_route_candidate_cells c
    join mesh_surface_h3_r8 s on s.h3 = c.h3
    where s.has_tower is not true
      and (
        s.is_in_unfit_area
        or s.is_in_boundaries is not true
        or s.has_road is not true
    );

    if invalid_candidates > 0 then
        raise exception 'mesh_route_candidate_cells contains % unplaceable rows during bootstrap graph rebuild',
            invalid_candidates;
    end if;
end;
$$;

drop table if exists mesh_route_nodes;
create table mesh_route_nodes as
select
    row_number() over (order by h3) as node_id,
    h3
from mesh_route_candidate_cells;

alter table mesh_route_nodes
    add primary key (node_id);

create unique index if not exists mesh_route_nodes_h3_idx
    on mesh_route_nodes (h3);

drop table if exists mesh_route_edges;
create table mesh_route_edges as
select
    row_number() over () as edge_id,
    src.node_id as source,
    dst.node_id as target,
    mlc.path_loss_db as cost,
    mlc.path_loss_db as reverse_cost
from mesh_los_cache mlc
join mesh_route_nodes src on src.h3 = mlc.src_h3
join mesh_route_nodes dst on dst.h3 = mlc.dst_h3
where mlc.mast_height_src = :mast_height
  and mlc.mast_height_dst = :mast_height
  and mlc.frequency_hz = :frequency
  and mlc.clearance > 0
  and mlc.distance_m <= 80000;

alter table mesh_route_edges
    add primary key (edge_id);

create index if not exists mesh_route_edges_source_idx on mesh_route_edges (source);
create index if not exists mesh_route_edges_target_idx on mesh_route_edges (target);

select
    (select count(*) from mesh_route_bootstrap_pairs) as bootstrap_pair_count,
    (select count(*) from mesh_route_bootstrap_missing_pairs) as newly_seeded_pair_count,
    (select count(*) from mesh_route_bootstrap_metrics where clearance > 0) as visible_bootstrap_pair_count,
    (select count(*) from mesh_route_edges) as route_edge_count;

drop table if exists mesh_route_bootstrap_metrics;
drop table if exists mesh_route_bootstrap_missing_pairs;
