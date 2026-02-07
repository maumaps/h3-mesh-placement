set client_min_messages = notice;

-- Cluster eligible hexes by population and install one tower per cluster when absent.
\set max_distance 70000
\set target_clusters 2

-- Reset prior population towers so reruns stay idempotent and unlock their cells.
delete from mesh_towers where source = 'population';

-- Clear surface flags for cells that just lost a population tower so eligibility stays accurate.
update mesh_surface_h3_r8 s
set has_tower = false,
    distance_to_closest_tower = null
where has_tower
  and not exists (
        select 1
        from mesh_towers t
        where t.h3 = s.h3
    );

-- Recompute spacing after cleanup to refresh can_place_tower before clustering.
update mesh_surface_h3_r8 s
set distance_to_closest_tower = sub.dist_m
from (
    select
        s2.h3,
        min(ST_Distance(s2.centroid_geog, t.centroid_geog)) as dist_m
    from mesh_surface_h3_r8 s2
    join mesh_towers t on true
    group by s2.h3
) sub
where s.h3 = sub.h3;

-- Pick cluster winners and insert population towers.
with
-- Eligible candidates that can host towers (excluding cells already visible from existing towers).
candidate_cells as (
    select
        s.h3,
        s.centroid_geog,
        coalesce(s.population_70km, s.population, 0) as population
    from mesh_surface_h3_r8 s
    where s.can_place_tower
      and coalesce(s.population_70km, s.population, 0) > 0
      and not exists (
            select 1
            from mesh_towers t
            where ST_DWithin(s.centroid_geog, t.centroid_geog, :max_distance)
              and h3_los_between_cells(s.h3, t.h3)
        )
),
-- Build weighted geocentric points (XYZM) to feed KMeans radius in meters and population as M weight.
candidate_points as (
    select
        c.h3,
        c.population,
        c.centroid_geog,
        ST_SetSRID(
            ST_MakePoint(
                ST_X(gc.geom_3d),
                ST_Y(gc.geom_3d),
                ST_Z(gc.geom_3d),
                c.population
            ),
            4978
        ) as cluster_geom
    from candidate_cells c
    cross join lateral (
        select ST_Transform(ST_Force3D(c.centroid_geog::geometry), 4978) as geom_3d
    ) as gc
),
-- Assign clusters weighted by nearby population using the 3D trick to cap radius at 70 km.
clustered as (
    select
        cp.h3,
        cp.population,
        cp.centroid_geog,
        ST_ClusterKMeans(
            cp.cluster_geom,
            greatest(:target_clusters, case when (select count(*) from candidate_points) > 1 then 2 else 1 end),
            :max_distance
        ) over () as cluster_id
    from candidate_points cp
),
-- Skip clusters that already have any tower in range so we do not duplicate them.
clusters_with_towers as (
    select distinct cl.cluster_id
    from clustered cl
    join mesh_towers t
      on ST_DWithin(cl.centroid_geog, t.centroid_geog, :max_distance)
),
-- Pick the highest-population hex per remaining cluster.
cluster_winners as (
    select distinct on (cl.cluster_id)
        cl.cluster_id,
        cl.h3,
        cl.population,
        cl.centroid_geog
    from clustered cl
    where not exists (
        select 1
        from clusters_with_towers cwt
        where cwt.cluster_id = cl.cluster_id
    )
    order by cl.cluster_id, cl.population desc, cl.h3
),
-- Insert towers for winners and return their H3s.
inserted as (
    insert into mesh_towers (h3, source)
    select cw.h3, 'population'
    from cluster_winners cw
    on conflict (h3) do nothing
    returning h3
)
select coalesce(count(*), 0) as inserted_population_towers from inserted;

-- Keep surface tower flags in sync with the refreshed mesh_towers content.
update mesh_surface_h3_r8 s
set has_tower = true,
    distance_to_closest_tower = 0
where exists (
    select 1
    from mesh_towers t
    where t.h3 = s.h3
);

-- Recompute spacing after tower set changed so can_place_tower stays accurate downstream.
update mesh_surface_h3_r8 s
set distance_to_closest_tower = sub.dist_m
from (
    select
        s2.h3,
        min(ST_Distance(s2.centroid_geog, t.centroid_geog)) as dist_m
    from mesh_surface_h3_r8 s2
    join mesh_towers t on true
    group by s2.h3
) sub
where s.h3 = sub.h3;
