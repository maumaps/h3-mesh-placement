set client_min_messages = warning;
-- This surface build includes 100 km geography neighborhoods and cached LOS checks, which can legitimately exceed the server's interactive statement timeout.
set statement_timeout = 0;

drop table if exists mesh_surface_h3_r8;
-- Create core mesh surface table with indicators per H3 cell
create table mesh_surface_h3_r8 (
    h3 h3index primary key,
    geom geometry generated always as (h3_cell_to_boundary_geometry(h3)) stored,
    centroid_geog public.geography generated always as (h3_cell_to_geometry(h3)::public.geography) stored,
    ele double precision,
    has_road boolean default false,
    building_count integer,
    population numeric,
    has_tower boolean default false,
    clearance double precision,
    path_loss double precision,
    -- Treat a cell as covered once any cached visibility metrics confirm fresnel clearance and loss
    has_reception boolean generated always as (
        has_tower
        or (clearance > 0 and path_loss is not null)
    ) stored,
    is_in_boundaries boolean default false,
    is_in_unfit_area boolean default false,
    min_distance_to_closest_tower double precision default 0,
    has_building boolean generated always as (coalesce(building_count, 0) > 0) stored,
    visible_population numeric,
    population_70km numeric,
    visible_uncovered_population numeric,
    -- Track how many existing towers a cell can see with LOS
    visible_tower_count integer default 0,
    distance_to_closest_tower double precision,
    can_place_tower boolean generated always as (
        has_road
        and is_in_boundaries
        and not has_tower
        and not is_in_unfit_area
        and coalesce(distance_to_closest_tower >= min_distance_to_closest_tower, false)
    ) stored
);

insert into mesh_surface_h3_r8 (h3)
select h3
from mesh_surface_domain_h3_r8;

update mesh_surface_h3_r8 s
set ele = g.ele
from gebco_elevation_h3_r8 g
where s.h3 = g.h3::h3index;

update mesh_surface_h3_r8 s
set has_road = true
where exists (
    select 1 from roads_h3_r8 r where r.h3::h3index = s.h3
);

update mesh_surface_h3_r8 s
set building_count = b.building_count
from buildings_h3_r8 b
where s.h3 = b.h3::h3index;

update mesh_surface_h3_r8 s
set is_in_boundaries = exists (
    select 1
    from georgia_boundary b
    where ST_Intersects(b.geom, s.geom)
);

update mesh_surface_h3_r8 s
set is_in_unfit_area = exists (
    select 1
    from georgia_unfit_areas u
    where ST_Intersects(u.geom, s.geom)
);

update mesh_surface_h3_r8 s
set population = p.population
from population_h3_r8 p
where s.h3 = p.h3::h3index;

-- Spatial indexes to accelerate neighborhood lookups (population rings, distance filters).
create index if not exists mesh_surface_h3_r8_geom_idx on mesh_surface_h3_r8 using gist (geom);
create index if not exists mesh_surface_h3_r8_geog_idx on mesh_surface_h3_r8 using gist (centroid_geog);
create index if not exists mesh_surface_h3_r8_geog_population_idx on mesh_surface_h3_r8 using gist (centroid_geog) where (population > 0);

update mesh_surface_h3_r8 s
set has_tower = true
where exists (
    select 1 from mesh_towers t where t.h3 = s.h3
);

with tower_points as (
    select h3, centroid_geog as geog
    from mesh_towers
)
update mesh_surface_h3_r8 s
set distance_to_closest_tower = sub.dist_m
from (
    select
        s2.h3,
        min(ST_Distance(s2.centroid_geog, t.geog)) as dist_m
    from mesh_surface_h3_r8 s2
    join tower_points t on true
    group by s2.h3
) sub
where s.h3 = sub.h3;

drop table if exists pg_temp.mesh_surface_population_points;
-- Create temporary projected r6 population buckets so the 100 km clustering weight avoids a huge r8-to-r8 neighborhood matrix.
create temporary table mesh_surface_population_points as
with parent_population as (
    -- Roll r8 population cells up to r6 buckets; this preserves the regional demand signal used for ranking while keeping the join bounded.
    select
        h3_cell_to_parent(h3, 6) as h3,
        sum(population) as population
    from mesh_surface_h3_r8
    where population > 0
    group by h3_cell_to_parent(h3, 6)
)
select
    h3,
    population,
    ST_Transform(h3_cell_to_geometry(h3), 32638) as geom
from parent_population;

-- Create a temporary GiST index for the population neighborhood lookup below.
create index mesh_surface_population_points_geom_idx on mesh_surface_population_points using gist (geom);

analyze mesh_surface_population_points;

drop table if exists pg_temp.mesh_surface_tower_candidates;
-- Create temporary projected candidate points; this keeps the heavy neighborhood aggregation copyable and easy to inspect.
create temporary table mesh_surface_tower_candidates as
select
    h3,
    ST_Transform(centroid_geog::geometry, 32638) as geom
from mesh_surface_h3_r8
where can_place_tower;

analyze mesh_surface_tower_candidates;

with candidate_population as (
    -- Sum population within 100 km (no LOS) only for tower-eligible cells to speed up clustering weights.
    select
        c.h3,
        coalesce(sum(pop.population), 0) as population_100km
    from mesh_surface_tower_candidates c
    left join mesh_surface_population_points pop
        on ST_DWithin(pop.geom, c.geom, 100000)
    group by c.h3
)
update mesh_surface_h3_r8 s
set population_70km = cp.population_100km
from candidate_population cp
where s.h3 = cp.h3;

with tower_points as (
    select h3, centroid_geog
    from mesh_towers
),
visible_pairs as (
    select
        s.h3 as cell_h3,
        tp.h3 as tower_h3
    from mesh_surface_h3_r8 s
    join tower_points tp
        on tp.h3 <> s.h3
       and ST_DWithin(s.centroid_geog, tp.centroid_geog, 100000)
),
visible_counts as (
    select
        vp.cell_h3,
        count(distinct vp.tower_h3) as visible_count
    from visible_pairs vp
    where exists (
        -- Use already computed LOS metrics during the full surface build; cache warming happens in later resumable batch targets.
        select 1
        from mesh_los_cache lc
        where lc.src_h3 = least(vp.cell_h3, vp.tower_h3)
          and lc.dst_h3 = greatest(vp.cell_h3, vp.tower_h3)
          and lc.clearance > 0
          and lc.path_loss_db is not null
    )
    group by vp.cell_h3
)
update mesh_surface_h3_r8 s
set visible_tower_count = vc.visible_count
from visible_counts vc
where s.h3 = vc.cell_h3;

update mesh_surface_h3_r8
set visible_tower_count = 0
where visible_tower_count is null;

create index if not exists mesh_surface_h3_r8_brin_all on mesh_surface_h3_r8 using brin (
    ele,
    building_count,
    population,
    population_70km,
    visible_population,
    visible_uncovered_population,
    visible_tower_count,
    distance_to_closest_tower,
    min_distance_to_closest_tower,
    clearance,
    path_loss
);
-- Create btree index to speed up distance-based eligibility filtering
create index if not exists mesh_surface_h3_r8_distance_idx on mesh_surface_h3_r8 (distance_to_closest_tower);
