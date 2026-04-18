set client_min_messages = warning;

-- Backfill routed geometry for the visibility edges that drive LOS cache priority.
with edges_requiring_routes as (
    -- Use the same blocked diagnostic set as fill_mesh_los_cache_prepare so nearest-edge
    -- priority follows the routed corridor instead of the straight tower chord.
    select
        e.source_id,
        e.target_id,
        e.source_h3,
        e.target_h3,
        case
            when e.source_h3::text <= e.target_h3::text then e.source_h3
            else e.target_h3
        end as canonical_source_h3,
        case
            when e.source_h3::text <= e.target_h3::text then e.target_h3
            else e.source_h3
        end as canonical_target_h3
    from mesh_visibility_edges e
    where (not e.is_visible and e.is_between_clusters)
       or (e.cluster_hops is not null and e.cluster_hops >= 8)
),
edges_with_cache as (
    -- Reuse cached corridors immediately so reruns only build routes for new gaps.
    select
        err.source_id,
        err.target_id,
        case
            when err.source_h3 = err.canonical_source_h3 then cache.geom
            else ST_Reverse(cache.geom)
        end as routed_geom
    from edges_requiring_routes err
    join mesh_route_graph_cache cache
      on cache.source_h3 = err.canonical_source_h3
     and cache.target_h3 = err.canonical_target_h3
),
edges_cache_miss as (
    -- Only uncached gaps need a fresh pgRouting corridor build.
    select
        err.source_id,
        err.target_id,
        mesh_visibility_invisible_route_geom(err.source_h3, err.target_h3) as routed_geom
    from edges_requiring_routes err
    left join mesh_route_graph_cache cache
      on cache.source_h3 = err.canonical_source_h3
     and cache.target_h3 = err.canonical_target_h3
    where cache.source_h3 is null
),
edges_with_routes as (
    select source_id, target_id, routed_geom from edges_with_cache
    union all
    select source_id, target_id, routed_geom from edges_cache_miss
)
update mesh_visibility_edges e
set geom = err.routed_geom
from edges_with_routes err
where e.source_id = err.source_id
  and e.target_id = err.target_id
  and err.routed_geom is not null;
