set client_min_messages = notice;

-- Pull user-tunable LOS and pairing constants from the single pipeline config.
select value::double precision as max_distance
from mesh_pipeline_settings
where setting = 'max_los_distance_m'
\gset

select value::double precision as separation
from mesh_pipeline_settings
where setting = 'min_tower_separation_m'
\gset

select value::double precision as mast_height
from mesh_pipeline_settings
where setting = 'mast_height_m'
\gset

select value::double precision as frequency
from mesh_pipeline_settings
where setting = 'frequency_hz'
\gset

-- Rebuild the route-candidate working set from the current planning surface so
-- later cache fill and route graph steps use the same placeable H3 cells.
drop table if exists mesh_route_candidate_cells;
create table mesh_route_candidate_cells as
select
    s.h3,
    s.centroid_geog,
    s.centroid_geog::geometry as centroid_geom,
    coalesce(s.has_building, false) as has_building,
    coalesce(s.building_count, 0) as building_count
from mesh_surface_h3_r8 s
where s.has_tower
   or s.can_place_tower;

-- mesh_route_bridge consumes these H3 nodes later, so keep a compact primary
-- key for deterministic joins and deletes.
alter table mesh_route_candidate_cells
    add primary key (h3);

-- Spatial pairing later uses geography distance, so keep a gist index on the
-- candidate centroids to accelerate ST_DWithin and nearest-edge lookups.
create index if not exists mesh_route_candidate_cells_geog_idx
    on mesh_route_candidate_cells
    using gist (centroid_geog);

-- KNN nearest-edge and nearest-tower lookups use geometry ordering, so keep a
-- matching gist index on the cached geometry centroid too.
create index if not exists mesh_route_candidate_cells_geom_idx
    on mesh_route_candidate_cells
    using gist (centroid_geom);

-- Validate that every non-tower candidate still satisfies the surface gate so
-- route stages never plan through unplaceable cells by accident.
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
        raise exception 'mesh_route_candidate_cells contains % unplaceable rows; expected only towers or can_place_tower=true entries',
            invalid_candidates;
    end if;
end;
$$;

-- Materialize the blocked visibility edges once so the per-candidate nearest
-- lookup does not keep rechecking visibility flags and heap tuples in a rescan.
drop table if exists mesh_route_priority_edges;
create table mesh_route_priority_edges as
select
    e.geom,
    e.geom::geography as geom_geog
from mesh_visibility_edges e
where not e.is_visible
  and (e.is_between_clusters or e.cluster_hops > 7);

create index if not exists mesh_route_priority_edges_geom_idx
    on mesh_route_priority_edges
    using gist (geom);

-- Persist how close each candidate is to a blocked visibility edge so the
-- cache fill can prioritize blind spots before generic long-distance pairs.
drop table if exists mesh_route_candidate_invisible_dist;
create table mesh_route_candidate_invisible_dist as
select
    c.h3,
    coalesce(edge.distance_m, :max_distance) as distance_m
from mesh_route_candidate_cells c
left join lateral (
    select ST_DistanceSphere(c.centroid_geom, e.geom) as distance_m
    from mesh_route_priority_edges e
    order by c.centroid_geom <-> e.geom
    limit 1
) edge on true;

-- The priority lookup later joins by H3, so keep a compact primary key here.
alter table mesh_route_candidate_invisible_dist
    add primary key (h3);

-- Range scans on the priority distance are cheap with a BRIN index because the
-- table is small and only used for staging.
create index if not exists mesh_route_candidate_invisible_dist_distance_brin
    on mesh_route_candidate_invisible_dist
    using brin (distance_m);

-- Measure how close each candidate sits to towers outside the primary cluster.
-- These distances should drive cache fill first so disconnected clusters get
-- routeable LOS coverage before generic long-distance work.
drop table if exists mesh_route_disconnected_towers;
create table mesh_route_disconnected_towers as
with tower_clusters as (
    select *
    from mesh_tower_clusters()
),
cluster_sizes as (
    select
        tc.cluster_id,
        count(*) as tower_count
    from tower_clusters tc
    group by tc.cluster_id
),
primary_cluster as (
    select cs.cluster_id
    from cluster_sizes cs
    order by cs.tower_count desc, cs.cluster_id asc
    limit 1
)
select
    t.h3,
    t.centroid_geog,
    t.centroid_geog::geometry as centroid_geom
from mesh_towers t
join tower_clusters tc on tc.tower_id = t.tower_id
where exists (
        select 1
        from cluster_sizes cs
        join primary_cluster pc on true
        where cs.cluster_id <> pc.cluster_id
    )
  and tc.cluster_id <> (select pc.cluster_id from primary_cluster pc);

create index if not exists mesh_route_disconnected_towers_geog_idx
    on mesh_route_disconnected_towers
    using gist (centroid_geog);

create index if not exists mesh_route_disconnected_towers_geom_idx
    on mesh_route_disconnected_towers
    using gist (centroid_geom);

drop table if exists mesh_route_candidate_disconnected_dist;
create table mesh_route_candidate_disconnected_dist as
select
    c.h3,
    coalesce(dt.distance_m, :max_distance) as distance_m
from mesh_route_candidate_cells c
left join lateral (
    select ST_DistanceSphere(c.centroid_geom, dt.centroid_geom) as distance_m
    from mesh_route_disconnected_towers dt
    order by c.centroid_geom <-> dt.centroid_geom
    limit 1
) dt on true;

-- The disconnected-cluster priority lookup later joins by H3.
alter table mesh_route_candidate_disconnected_dist
    add primary key (h3);

create index if not exists mesh_route_candidate_disconnected_dist_distance_brin
    on mesh_route_candidate_disconnected_dist
    using brin (distance_m);

-- Generate every H3 pair that is separated enough to be meaningful but still
-- within the maximum route search radius.
drop table if exists mesh_route_pair_candidates;
create table mesh_route_pair_candidates as
with generic_pairs as (
    select
        c1.h3 as src_h3,
        c2.h3 as dst_h3,
        ST_Distance(c1.centroid_geog, c2.centroid_geog) as distance_m
    from mesh_route_candidate_cells c1
    join mesh_route_candidate_cells c2
      on c2.h3 > c1.h3
     and ST_DWithin(c1.centroid_geog, c2.centroid_geog, :max_distance)
)
select
    gp.src_h3,
    gp.dst_h3,
    gp.distance_m
from generic_pairs gp;

-- Cache lookups and later deletes are keyed by src/dst, so keep a btree index
-- on the pair table.
create index if not exists mesh_route_pair_candidates_src_dst_idx
    on mesh_route_pair_candidates (src_h3, dst_h3);

-- Keep only the pairs that do not already have LOS metrics in mesh_los_cache,
-- and attach building-first and invisible-edge priority metadata for batching.
drop table if exists mesh_route_missing_pairs;
create table mesh_route_missing_pairs as
select
    pr.src_h3,
    pr.dst_h3,
    (src_candidate.has_building::integer + dst_candidate.has_building::integer) as building_endpoint_count,
    (src_candidate.building_count + dst_candidate.building_count) as building_count,
    least(src_disconnected.distance_m, dst_disconnected.distance_m) as disconnected_priority,
    src_priority.distance_m + dst_priority.distance_m as priority
from mesh_route_pair_candidates pr
left join mesh_los_cache mlc
    on mlc.src_h3 = pr.src_h3
   and mlc.dst_h3 = pr.dst_h3
   and mlc.mast_height_src = :mast_height
   and mlc.mast_height_dst = :mast_height
   and mlc.frequency_hz = :frequency
join mesh_route_candidate_cells src_candidate on src_candidate.h3 = pr.src_h3
join mesh_route_candidate_cells dst_candidate on dst_candidate.h3 = pr.dst_h3
join mesh_route_candidate_invisible_dist src_priority on src_priority.h3 = pr.src_h3
join mesh_route_candidate_invisible_dist dst_priority on dst_priority.h3 = pr.dst_h3
join mesh_route_candidate_disconnected_dist src_disconnected on src_disconnected.h3 = pr.src_h3
join mesh_route_candidate_disconnected_dist dst_disconnected on dst_disconnected.h3 = pr.dst_h3
where mlc.src_h3 is null;

-- Report initial coverage before batching starts so the operator can see how
-- much of the local route graph still needs LOS work.
with stats as (
    select
        (select count(*) from mesh_route_missing_pairs) as missing_pairs,
        (select count(*) from mesh_route_pair_candidates) as total_pairs,
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

-- Exact-match deletes and cache joins need a btree on the canonical pair key.
create index if not exists mesh_route_missing_pairs_src_dst_idx
    on mesh_route_missing_pairs (src_h3, dst_h3);

-- Range-oriented diagnostics can still use BRIN over the same pair key.
create index if not exists mesh_route_missing_pairs_src_dst_brin
    on mesh_route_missing_pairs
    using brin (src_h3, dst_h3);

-- Every committed batch orders by the exact same priority tuple, so keep a
-- matching btree index to avoid a full parallel sort of mesh_route_missing_pairs
-- on every batch resume.
create index if not exists mesh_route_missing_pairs_batch_order_idx
    on mesh_route_missing_pairs (
        building_endpoint_count desc,
        disconnected_priority,
        priority,
        building_count desc,
        src_h3,
        dst_h3
    );

-- Building-first ordering uses these fields, so give BRIN access paths for the
-- bounded batch selection query.
create index if not exists mesh_route_missing_pairs_building_brin
    on mesh_route_missing_pairs
    using brin (building_endpoint_count, building_count);

-- Disconnected-cluster distance should sort before generic invisible-edge priority.
create index if not exists mesh_route_missing_pairs_disconnected_brin
    on mesh_route_missing_pairs
    using brin (disconnected_priority);

-- Invisible-edge priority is also part of the batch ordering.
create index if not exists mesh_route_missing_pairs_priority_brin
    on mesh_route_missing_pairs
    using brin (priority);
