set client_min_messages = warning;

drop table if exists mesh_surface_h3_r8;
-- Create core mesh surface table with indicators per H3 cell
create table mesh_surface_h3_r8 (
    h3 h3index primary key,
    geom geometry generated always as (h3_cell_to_boundary_geometry(h3)) stored,
    centroid_geog public.geography generated always as (h3_cell_to_geometry(h3)::public.geography) stored,
    ele double precision,
    has_road boolean default false,
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
    min_distance_to_closest_tower double precision default 5000,
    visible_population numeric,
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
       and ST_DWithin(s.centroid_geog, tp.centroid_geog, 70000)
),
visible_counts as (
    select
        vp.cell_h3,
        count(*) as visible_count
    from visible_pairs vp
    where h3_los_between_cells(vp.cell_h3, vp.tower_h3)
    group by vp.cell_h3
)
update mesh_surface_h3_r8 s
set visible_tower_count = vc.visible_count
from visible_counts vc
where s.h3 = vc.cell_h3;

update mesh_surface_h3_r8
set visible_tower_count = 0
where visible_tower_count is null;

create index if not exists mesh_surface_h3_r8_geom_idx on mesh_surface_h3_r8 using gist (geom);
create index if not exists mesh_surface_h3_r8_geog_idx on mesh_surface_h3_r8 using gist (centroid_geog);
create index if not exists mesh_surface_h3_r8_geog_population_idx on mesh_surface_h3_r8 using gist (centroid_geog) where (population > 0);
create index if not exists mesh_surface_h3_r8_brin_all on mesh_surface_h3_r8 using brin (
    ele,
    population,
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
