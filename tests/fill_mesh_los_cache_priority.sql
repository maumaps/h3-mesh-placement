set client_min_messages = warning;

begin;

-- Shadow the production LOS cache so this fixture never truncates or mutates precious cache state.
create temporary table mesh_los_cache (like public.mesh_los_cache including all) on commit drop;

truncate mesh_surface_h3_r8;
truncate mesh_towers;
truncate mesh_visibility_edges;

-- Seed a tiny mesh surface with two towers and a few bridge-capable cells.
do
$$
declare
    tower_a_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.0), 4326), 8);
    tower_b_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(1.2, 0.0), 4326), 8);
    bridge_one_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.4, 0.0), 4326), 8);
    bridge_two_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.8, 0.0), 4326), 8);
    short_gap_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.02, 0.0), 4326), 8);
    unfit_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(-0.6, 0.0), 4326), 8);
    roadless_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.6), 4326), 8);
    out_of_bounds_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, -0.6), 4326), 8);
begin
    with anchor_nodes as (
        select unnest(array[
            tower_a_h3,
            tower_b_h3,
            bridge_one_h3,
            bridge_two_h3,
            short_gap_h3
        ]) as h3
    ),
    paths as (
        select h3_grid_path_cells(n1.h3, n2.h3) as h3
        from anchor_nodes n1
        join anchor_nodes n2 on n1.h3 < n2.h3
    ),
    extra_cells as (
        select unnest(array[
            unfit_h3,
            roadless_h3,
            out_of_bounds_h3
        ]) as h3
    ),
    all_cells as (
        select h3 from anchor_nodes
        union
        select h3 from paths
        union
        select h3 from extra_cells
    )
    insert into mesh_surface_h3_r8 (
        h3,
        ele,
        has_road,
        building_count,
        population,
        has_tower,
        clearance,
        path_loss,
        is_in_boundaries,
        is_in_unfit_area,
        min_distance_to_closest_tower,
        visible_population,
        visible_uncovered_population,
        visible_tower_count,
        distance_to_closest_tower
    )
    select
        ac.h3,
        0,
        case
            when ac.h3 in (tower_a_h3, tower_b_h3, bridge_one_h3, bridge_two_h3, short_gap_h3) then true
            when ac.h3 in (unfit_h3, out_of_bounds_h3) then true
            else false
        end,
        case
            when ac.h3 = bridge_one_h3 then 0
            when ac.h3 = bridge_two_h3 then 12
            when ac.h3 in (tower_a_h3, tower_b_h3) then 3
            else 0
        end,
        0,
        case
            when ac.h3 in (tower_a_h3, tower_b_h3) then true
            else false
        end,
        null,
        null,
        case
            when ac.h3 = out_of_bounds_h3 then false
            else true
        end,
        case
            when ac.h3 = unfit_h3 then true
            else false
        end,
        case
            when ac.h3 in (bridge_one_h3, bridge_two_h3, short_gap_h3, unfit_h3, roadless_h3, out_of_bounds_h3) then 5000
            else 0
        end,
        null,
        null,
        0,
        case
            when ac.h3 in (bridge_one_h3, bridge_two_h3, short_gap_h3, unfit_h3, roadless_h3, out_of_bounds_h3) then 6000
            when ac.h3 in (tower_a_h3, tower_b_h3) then 0
            else 10000
        end
    from all_cells ac;

    insert into mesh_towers (h3, source)
    values
        (tower_a_h3, 'test_route_seed'),
        (tower_b_h3, 'test_route_seed');
end;
$$;

-- Craft two invisible visibility edges: one brushing against bridge_one, another far away, with type labels from tower sources.
with tower_pair as (
    select
        src.tower_id as source_id,
        dst.tower_id as target_id,
        src.h3 as source_h3,
        dst.h3 as target_h3,
        src.source as source_source,
        dst.source as target_source
    from mesh_towers src
    join mesh_towers dst on dst.tower_id > src.tower_id
    order by src.tower_id, dst.tower_id
    limit 1
)
insert into mesh_visibility_edges (
    source_id,
    target_id,
    source_h3,
    target_h3,
    type,
    distance_m,
    is_visible,
    is_between_clusters,
    cluster_hops,
    geom
)
select
    tp.source_id,
    tp.target_id,
    tp.source_h3,
    tp.target_h3,
    tp.source_source || '-' || tp.target_source,
    ST_Distance(close_start::geography, close_end::geography),
    false,
    true,
    null::integer,
    ST_MakeLine(close_start, close_end)
from tower_pair tp,
     lateral (
         select
             ST_SetSRID(ST_MakePoint(0.4, 0.01), 4326) as close_start,
             ST_SetSRID(ST_MakePoint(0.8, 0.01), 4326) as close_end
     ) as close_geom
union all
select
    tp.source_id,
    tp.target_id,
    tp.source_h3,
    tp.target_h3,
    tp.source_source || '-' || tp.target_source,
    ST_Distance(far_start::geography, far_end::geography),
    false,
    true,
    null::integer,
    ST_MakeLine(far_start, far_end)
from tower_pair tp,
     lateral (
         select
             ST_SetSRID(ST_MakePoint(10.0, 10.0), 4326) as far_start,
             ST_SetSRID(ST_MakePoint(11.0, 11.0), 4326) as far_end
     ) as far_geom;

-- Mirror the candidate+priority staging tables from fill_mesh_los_cache for this miniature scene.
drop table if exists mesh_route_candidate_cells;
create table mesh_route_candidate_cells as
select
    s.h3,
    s.centroid_geog,
    coalesce(s.has_building, false) as has_building,
    coalesce(s.building_count, 0) as building_count
from mesh_surface_h3_r8 s
where s.has_tower
   or s.can_place_tower;

alter table mesh_route_candidate_cells
    add primary key (h3);

create index if not exists mesh_route_candidate_cells_geog_idx
    on mesh_route_candidate_cells
    using gist (centroid_geog);

-- Precompute nearest invisible edge distance per candidate.
drop table if exists mesh_route_candidate_invisible_dist;
create table mesh_route_candidate_invisible_dist as
select
    c.h3,
    coalesce(
        (
            select ST_Distance(c.centroid_geog, e.geom::geography)
            from mesh_visibility_edges e
            where not e.is_visible
              and e.is_between_clusters
            order by c.centroid_geog::geometry <-> e.geom
            limit 1
        ),
        80000
    ) as distance_m
from mesh_route_candidate_cells c;

alter table mesh_route_candidate_invisible_dist
    add primary key (h3);

-- Build missing pairs exactly like the procedure does.
drop table if exists mesh_route_pair_candidates;
create table mesh_route_pair_candidates as
select
    c1.h3 as src_h3,
    c2.h3 as dst_h3,
    ST_Distance(c1.centroid_geog, c2.centroid_geog) as distance_m
from mesh_route_candidate_cells c1
join mesh_route_candidate_cells c2
  on c2.h3 > c1.h3
 where ST_DWithin(c1.centroid_geog, c2.centroid_geog, 80000)
   and not ST_DWithin(c1.centroid_geog, c2.centroid_geog, 5000);

create index on mesh_route_pair_candidates (src_h3, dst_h3);

drop table if exists mesh_route_missing_pairs;
create table mesh_route_missing_pairs as
select
    pr.src_h3,
    pr.dst_h3,
    (src_candidate.has_building::integer + dst_candidate.has_building::integer) as building_endpoint_count,
    (src_candidate.building_count + dst_candidate.building_count) as building_count,
    src_priority.distance_m + dst_priority.distance_m as priority
from mesh_route_pair_candidates pr
left join mesh_los_cache mlc
    on mlc.src_h3 = pr.src_h3
   and mlc.dst_h3 = pr.dst_h3
   and mlc.mast_height_src = 28
   and mlc.mast_height_dst = 28
   and mlc.frequency_hz = 868e6
join mesh_route_candidate_cells src_candidate on src_candidate.h3 = pr.src_h3
join mesh_route_candidate_cells dst_candidate on dst_candidate.h3 = pr.dst_h3
join mesh_route_candidate_invisible_dist src_priority on src_priority.h3 = pr.src_h3
join mesh_route_candidate_invisible_dist dst_priority on dst_priority.h3 = pr.dst_h3
where mlc.src_h3 is null;

create index on mesh_route_missing_pairs using brin (priority);

-- Ensure candidate staging mirrors can_place_tower filters and excludes obvious invalid cells.
do
$$
declare
    unfit_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(-0.6, 0.0), 4326), 8);
    roadless_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, 0.6), 4326), 8);
    out_of_bounds_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.0, -0.6), 4326), 8);
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
        or coalesce(s.distance_to_closest_tower >= s.min_distance_to_closest_tower, false) is not true
    );

    if invalid_candidates > 0 then
        raise exception 'mesh_route_candidate_cells should only include towers or placeable cells, found % invalid rows',
            invalid_candidates;
    end if;

    if exists (
        select 1
        from mesh_route_candidate_cells c
        where c.h3 in (unfit_h3, roadless_h3, out_of_bounds_h3)
    ) then
        raise exception 'Candidate staging should drop roadless/unfit/out-of-bound cells (% / % / %)',
            roadless_h3::text,
            unfit_h3::text,
            out_of_bounds_h3::text;
    end if;
end;
$$;

-- Verify the raw invisible-edge priority still stays near the handmade close
-- edge rather than drifting toward the far-away edge.
do
$$
declare
    bridge_one_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.4, 0.0), 4326), 8);
    close_distance double precision;
    far_distance double precision;
    recorded_priority double precision;
begin
    select min(priority) / 2.0
    into recorded_priority
    from mesh_route_missing_pairs;

    select ST_Distance(
        s.centroid_geog,
        ST_MakeLine(
            ST_SetSRID(ST_MakePoint(0.4, 0.01), 4326),
            ST_SetSRID(ST_MakePoint(0.8, 0.01), 4326)
        )::geography
    )
    into close_distance
    from mesh_route_candidate_cells s
    where s.h3 = bridge_one_h3;

    select ST_Distance(
        s.centroid_geog,
        ST_MakeLine(
            ST_SetSRID(ST_MakePoint(10.0, 10.0), 4326),
            ST_SetSRID(ST_MakePoint(11.0, 11.0), 4326)
        )::geography
    )
    into far_distance
    from mesh_route_candidate_cells s
    where s.h3 = bridge_one_h3;

    if recorded_priority > close_distance + 200 then
        raise exception 'Priority % should stay close to the nearby invisible edge band around %, expected at most %',
            recorded_priority,
            bridge_one_h3::text,
            close_distance + 200;
    end if;

    if recorded_priority >= far_distance then
        raise exception 'Priority % should remain below far-edge distance % for %, otherwise far invisible edges are dominating unexpectedly',
            recorded_priority,
            far_distance,
            bridge_one_h3::text;
    end if;
end;
$$;

-- Ensure prioritized selection prefers the building-backed pair even if the
-- non-building candidate is the same geometric distance from the invisible edge.
do
$$
declare
    bridge_two_h3 h3index := h3_latlng_to_cell(ST_SetSRID(ST_MakePoint(0.8, 0.0), 4326), 8);
    prioritized_pair record;
begin
    select src_h3, dst_h3, building_endpoint_count, building_count, priority
    into prioritized_pair
    from (
        select
            mp.src_h3,
            mp.dst_h3,
            mp.building_endpoint_count,
            mp.building_count,
            mp.priority
        from mesh_route_missing_pairs mp
        order by
            mp.building_endpoint_count desc,
            mp.priority,
            mp.building_count desc,
            mp.src_h3,
            mp.dst_h3
        limit 1
    ) sub;

    if prioritized_pair.src_h3 <> bridge_two_h3 and prioritized_pair.dst_h3 <> bridge_two_h3 then
        raise exception 'Prioritized missing pair % <> % does not include building-backed bridge_two %, building-first cache ordering failed',
            prioritized_pair.src_h3::text,
            prioritized_pair.dst_h3::text,
            bridge_two_h3::text;
    end if;

    if prioritized_pair.building_endpoint_count <= 0 then
        raise exception 'Prioritized pair should have at least one building-backed endpoint, got building_endpoint_count=% for % <> %',
            prioritized_pair.building_endpoint_count,
            prioritized_pair.src_h3::text,
            prioritized_pair.dst_h3::text;
    end if;
end;
$$;

rollback;
