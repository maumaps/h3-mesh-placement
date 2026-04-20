set client_min_messages = notice;

-- Pull user-tunable route-graph filters from the single pipeline config.
select value::double precision as max_distance
from mesh_pipeline_settings
where setting = 'max_los_distance_m'
\gset

select value::double precision as mast_height
from mesh_pipeline_settings
where setting = 'mast_height_m'
\gset

select value::double precision as frequency
from mesh_pipeline_settings
where setting = 'frequency_hz'
\gset

-- Report cache coverage before the route graph is rebuilt. The main pipeline
-- intentionally allows partial cache fill so route stages can start from early
-- results, while the manual backfill target can drain the rest later.
with stats as (
    select
        coalesce((select count(*) from mesh_route_missing_pairs), 0) as missing_pairs,
        coalesce((select count(*) from mesh_route_pair_candidates), 0) as total_pairs,
        (select min(priority) from mesh_route_missing_pairs) as min_priority_m
)
select
    missing_pairs,
    total_pairs,
    case
        when total_pairs = 0 then 100::numeric
        else round(((total_pairs - missing_pairs)::numeric / total_pairs) * 100, 2)
    end as completion_pct,
    min_priority_m
from stats;

-- Cleanup staging after the route graph is rebuilt. Later backfill runs can
-- recreate these tables from the current mesh_towers and mesh_los_cache state.
drop table if exists mesh_route_missing_metrics;
drop table if exists mesh_route_missing_pairs;
drop table if exists mesh_route_candidate_invisible_dist;

-- Assign deterministic node ids for pgRouting from the same candidate H3 cells
-- that the cache fill operated on.
drop table if exists mesh_route_nodes;
create table mesh_route_nodes as
select
    row_number() over (order by h3) as node_id,
    h3
from mesh_route_candidate_cells;

-- pgRouting joins by node id while debugging often starts from H3, so keep
-- both primary and unique lookup paths.
alter table mesh_route_nodes
    add primary key (node_id);

create unique index if not exists mesh_route_nodes_h3_idx
    on mesh_route_nodes (h3);

-- Build the route graph only from visible cached LOS links that match the
-- configured mast height, frequency, and distance range.
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
  and mlc.distance_m <= :max_distance;

-- pgRouting expects a stable integer edge id and the route stages scan by
-- source/target, so keep the edge table indexed accordingly.
alter table mesh_route_edges
    add primary key (edge_id);

create index if not exists mesh_route_edges_source_idx on mesh_route_edges (source);
create index if not exists mesh_route_edges_target_idx on mesh_route_edges (target);

-- Precompute route-graph connected components once here so bridge and slim
-- stages can reuse them without rerunning pgRouting component analysis inside
-- every long transaction.
drop table if exists mesh_route_edge_components;
create table mesh_route_edge_components as
select
    component,
    node
from pgr_connectedComponents(
    'select edge_id as id, source, target, cost, reverse_cost from mesh_route_edges'
);

-- Later route stages join from mesh_route_nodes.node_id into the component map.
create index if not exists mesh_route_edge_components_node_idx
    on mesh_route_edge_components (node);

-- Once the route graph is rebuilt, the pair and candidate staging can be
-- dropped because their durable state now lives in mesh_los_cache.
drop index if exists mesh_route_candidate_cells_geog_idx;
drop table if exists mesh_route_candidate_cells;
drop table if exists mesh_route_pair_candidates;

-- Refresh planner statistics after the large cache upsert.
vacuum analyze mesh_los_cache;
