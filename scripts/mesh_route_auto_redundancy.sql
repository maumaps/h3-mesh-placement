set client_min_messages = notice;

drop table if exists pg_temp.mesh_route_auto_redundancy_candidates;
-- Stage cached-LOS backup candidates for current bridge edges and cut nodes.
create temporary table mesh_route_auto_redundancy_candidates as
with recursive
settings as (
    -- Keep the redundancy repair on the same RF constants as the rest of the route graph.
    select
        coalesce((
            select value::double precision
            from mesh_pipeline_settings
            where setting = 'max_los_distance_m'
        ), 100000) as max_distance,
        coalesce((
            select value::double precision
            from mesh_pipeline_settings
            where setting = 'mast_height_m'
        ), 28) as mast_height,
        coalesce((
            select value::double precision
            from mesh_pipeline_settings
            where setting = 'frequency_hz'
        ), 868000000) as frequency
),
nodes as (
    -- MQTT rows are context roots for the installer, not generated rollout towers.
    select
        tower_id,
        source
    from mesh_towers
    where source <> 'mqtt'
),
visible_edges as (
    -- Work from refreshed visibility diagnostics so bridge findings match the review gate.
    select
        e.source_id,
        e.target_id,
        e.distance_m
    from mesh_visibility_edges e
    join nodes source_tower on source_tower.tower_id = e.source_id
    join nodes target_tower on target_tower.tower_id = e.target_id
    where e.is_visible
),
undirected_edges as (
    select source_id as a, target_id as b
    from visible_edges

    union all

    select target_id as a, source_id as b
    from visible_edges
),
removed_nodes as (
    select tower_id as removed_id
    from nodes
),
node_walk_starts as (
    -- For each removed node, start from the lowest surviving tower.
    select
        removed_nodes.removed_id,
        min(nodes.tower_id) as start_id
    from removed_nodes
    join nodes on nodes.tower_id <> removed_nodes.removed_id
    group by removed_nodes.removed_id
),
node_walk(removed_id, tower_id) as (
    select
        removed_id,
        start_id
    from node_walk_starts

    union

    select
        node_walk.removed_id,
        undirected_edges.b
    from node_walk
    join undirected_edges on undirected_edges.a = node_walk.tower_id
    where undirected_edges.b <> node_walk.removed_id
),
node_reach as (
    select
        removed_id,
        count(distinct tower_id) as reachable_count
    from node_walk
    group by removed_id
),
node_total as (
    select count(*) - 1 as remaining_count
    from nodes
),
articulation_report as (
    select
        removed_nodes.removed_id,
        coalesce(node_reach.reachable_count, 0) as reachable_count,
        node_total.remaining_count,
        least(
            coalesce(node_reach.reachable_count, 0),
            node_total.remaining_count - coalesce(node_reach.reachable_count, 0)
        ) as smaller_side_count
    from removed_nodes
    cross join node_total
    left join node_reach on node_reach.removed_id = removed_nodes.removed_id
    where coalesce(node_reach.reachable_count, 0) <> node_total.remaining_count
),
node_sides as (
    select
        articulation_report.removed_id,
        nodes.tower_id,
        case when node_walk.tower_id is null then 'unreached' else 'reached' end as side
    from articulation_report
    join nodes on nodes.tower_id <> articulation_report.removed_id
    left join node_walk
      on node_walk.removed_id = articulation_report.removed_id
     and node_walk.tower_id = nodes.tower_id
),
cut_pairs as (
    -- Pair towers from opposite sides of each cut node; a backup anchor that
    -- sees both endpoints removes the articulation dependency.
    select
        articulation_report.removed_id,
        left_side.tower_id as source_id,
        right_side.tower_id as target_id,
        articulation_report.smaller_side_count
    from articulation_report
    join node_sides left_side
      on left_side.removed_id = articulation_report.removed_id
     and left_side.side = 'reached'
    join node_sides right_side
      on right_side.removed_id = articulation_report.removed_id
     and right_side.side = 'unreached'
),
removed_edges as (
    select
        source_id,
        target_id
    from visible_edges
),
edge_walk_edges as (
    -- Remove one edge at a time and expose all remaining LOS edges in both directions.
    select
        visible_edges.source_id as a,
        visible_edges.target_id as b,
        removed_edges.source_id as removed_source,
        removed_edges.target_id as removed_target
    from visible_edges
    cross join removed_edges
    where not (
        visible_edges.source_id = removed_edges.source_id
        and visible_edges.target_id = removed_edges.target_id
    )

    union all

    select
        visible_edges.target_id as a,
        visible_edges.source_id as b,
        removed_edges.source_id as removed_source,
        removed_edges.target_id as removed_target
    from visible_edges
    cross join removed_edges
    where not (
        visible_edges.source_id = removed_edges.source_id
        and visible_edges.target_id = removed_edges.target_id
    )
),
edge_walk_starts as (
    -- Use the same deterministic start rule as the review assertion.
    select
        removed_edges.source_id as removed_source,
        removed_edges.target_id as removed_target,
        min(nodes.tower_id) as start_id
    from removed_edges
    cross join nodes
    group by
        removed_edges.source_id,
        removed_edges.target_id
),
edge_walk(removed_source, removed_target, tower_id) as (
    select
        removed_source,
        removed_target,
        start_id
    from edge_walk_starts

    union

    select
        edge_walk.removed_source,
        edge_walk.removed_target,
        edge_walk_edges.b
    from edge_walk
    join edge_walk_edges
      on edge_walk_edges.a = edge_walk.tower_id
     and edge_walk_edges.removed_source = edge_walk.removed_source
     and edge_walk_edges.removed_target = edge_walk.removed_target
),
edge_reach as (
    select
        removed_source,
        removed_target,
        count(distinct tower_id) as reachable_count
    from edge_walk
    group by
        removed_source,
        removed_target
),
edge_total as (
    select count(*) as remaining_count
    from nodes
),
bridge_edges as (
    select
        edge_reach.removed_source as source_id,
        edge_reach.removed_target as target_id,
        visible_edges.distance_m,
        least(
            edge_reach.reachable_count,
            edge_total.remaining_count - edge_reach.reachable_count
        ) as smaller_side_count
    from edge_reach
    cross join edge_total
    join visible_edges
      on visible_edges.source_id = edge_reach.removed_source
     and visible_edges.target_id = edge_reach.removed_target
    where edge_reach.reachable_count <> edge_total.remaining_count
),
bridge_candidates as (
    select distinct on (bridge_edges.source_id, bridge_edges.target_id)
        bridge_edges.source_id,
        bridge_edges.target_id,
        bridge_edges.smaller_side_count,
        surface.h3,
        coalesce(surface.has_building, false) as has_building,
        coalesce(surface.building_count, 0) as building_count,
        coalesce(surface.visible_population, 0) as visible_population,
        coalesce(surface.population_70km, 0) as population_70km,
        greatest(
            ST_Distance(surface.centroid_geog, source_tower.centroid_geog),
            ST_Distance(surface.centroid_geog, target_tower.centroid_geog)
        ) as max_endpoint_distance_m
    from bridge_edges
    cross join settings
    join mesh_towers source_tower on source_tower.tower_id = bridge_edges.source_id
    join mesh_towers target_tower on target_tower.tower_id = bridge_edges.target_id
    join mesh_los_cache source_link
      on source_link.mast_height_src = settings.mast_height
     and source_link.mast_height_dst = settings.mast_height
     and source_link.frequency_hz = settings.frequency
     and source_link.clearance > 0
     and source_link.distance_m <= settings.max_distance
     and (source_link.src_h3 = source_tower.h3 or source_link.dst_h3 = source_tower.h3)
    join mesh_surface_h3_r8 surface
      on surface.h3 = case
            when source_link.src_h3 = source_tower.h3 then source_link.dst_h3
            else source_link.src_h3
        end
    join mesh_los_cache target_link
      on target_link.mast_height_src = settings.mast_height
     and target_link.mast_height_dst = settings.mast_height
     and target_link.frequency_hz = settings.frequency
     and target_link.clearance > 0
     and target_link.distance_m <= settings.max_distance
     and (
            (target_link.src_h3 = target_tower.h3 and target_link.dst_h3 = surface.h3)
         or (target_link.dst_h3 = target_tower.h3 and target_link.src_h3 = surface.h3)
     )
    where surface.is_in_boundaries
      and surface.has_road
      and not surface.is_in_unfit_area
      and surface.h3 not in (source_tower.h3, target_tower.h3)
      and not exists (
            select 1
            from mesh_towers existing
            where existing.h3 = surface.h3
        )
    order by
        bridge_edges.source_id,
        bridge_edges.target_id,
        coalesce(surface.has_building, false) desc,
        coalesce(surface.building_count, 0) desc,
        coalesce(surface.visible_population, 0) desc,
        coalesce(surface.population_70km, 0) desc,
        max_endpoint_distance_m,
        surface.h3
),
cut_candidates as (
    select distinct on (cut_pairs.removed_id)
        cut_pairs.source_id,
        cut_pairs.target_id,
        cut_pairs.smaller_side_count,
        surface.h3,
        coalesce(surface.has_building, false) as has_building,
        coalesce(surface.building_count, 0) as building_count,
        coalesce(surface.visible_population, 0) as visible_population,
        coalesce(surface.population_70km, 0) as population_70km,
        greatest(
            ST_Distance(surface.centroid_geog, source_tower.centroid_geog),
            ST_Distance(surface.centroid_geog, target_tower.centroid_geog)
        ) as max_endpoint_distance_m
    from cut_pairs
    cross join settings
    join mesh_towers source_tower on source_tower.tower_id = cut_pairs.source_id
    join mesh_towers target_tower on target_tower.tower_id = cut_pairs.target_id
    join mesh_los_cache source_link
      on source_link.mast_height_src = settings.mast_height
     and source_link.mast_height_dst = settings.mast_height
     and source_link.frequency_hz = settings.frequency
     and source_link.clearance > 0
     and source_link.distance_m <= settings.max_distance
     and (source_link.src_h3 = source_tower.h3 or source_link.dst_h3 = source_tower.h3)
    join mesh_surface_h3_r8 surface
      on surface.h3 = case
            when source_link.src_h3 = source_tower.h3 then source_link.dst_h3
            else source_link.src_h3
        end
    join mesh_los_cache target_link
      on target_link.mast_height_src = settings.mast_height
     and target_link.mast_height_dst = settings.mast_height
     and target_link.frequency_hz = settings.frequency
     and target_link.clearance > 0
     and target_link.distance_m <= settings.max_distance
     and (
            (target_link.src_h3 = target_tower.h3 and target_link.dst_h3 = surface.h3)
         or (target_link.dst_h3 = target_tower.h3 and target_link.src_h3 = surface.h3)
     )
    where surface.is_in_boundaries
      and surface.has_road
      and not surface.is_in_unfit_area
      and surface.h3 not in (source_tower.h3, target_tower.h3)
      and not exists (
            select 1
            from mesh_towers existing
            where existing.h3 = surface.h3
        )
    order by
        cut_pairs.removed_id,
        coalesce(surface.has_building, false) desc,
        coalesce(surface.building_count, 0) desc,
        coalesce(surface.visible_population, 0) desc,
        coalesce(surface.population_70km, 0) desc,
        max_endpoint_distance_m,
        source_tower.tower_id,
        target_tower.tower_id,
        surface.h3
)
select *
from bridge_candidates
union all
select *
from cut_candidates;

drop table if exists pg_temp.mesh_route_auto_redundancy_inserted;
-- Keep inserted anchors available for local surface invalidation.
create temporary table mesh_route_auto_redundancy_inserted (
    h3 h3index primary key
) on commit preserve rows;

with unique_candidates as (
    -- One backup anchor can cover multiple bridge edges; insert it once.
    select distinct on (h3)
        h3,
        smaller_side_count,
        has_building,
        building_count,
        visible_population,
        population_70km,
        max_endpoint_distance_m
    from mesh_route_auto_redundancy_candidates
    order by
        h3,
        smaller_side_count desc,
        has_building desc,
        building_count desc,
        visible_population desc,
        population_70km desc,
        max_endpoint_distance_m
),
inserted as (
    insert into mesh_towers (h3, source)
    select
        h3,
        'route'
    from unique_candidates
    order by
        smaller_side_count desc,
        has_building desc,
        building_count desc,
        visible_population desc,
        population_70km desc,
        max_endpoint_distance_m,
        h3
    on conflict (h3) do nothing
    returning h3
)
insert into mesh_route_auto_redundancy_inserted (h3)
select h3
from inserted;

do
$$
declare
    bridge_candidate_count integer;
    inserted_count integer;
begin
    select count(*)
    into bridge_candidate_count
    from mesh_route_auto_redundancy_candidates;

    select count(*)
    into inserted_count
    from mesh_route_auto_redundancy_inserted;

    raise notice 'Auto route redundancy anchors: % bridge candidate(s), % inserted',
        bridge_candidate_count,
        inserted_count;
end;
$$;

update mesh_surface_h3_r8 surface
set has_tower = true,
    distance_to_closest_tower = 0,
    clearance = null,
    path_loss = null,
    visible_population = null,
    visible_uncovered_population = 0,
    visible_tower_count = null
from mesh_route_auto_redundancy_inserted inserted
where surface.h3 = inserted.h3;
