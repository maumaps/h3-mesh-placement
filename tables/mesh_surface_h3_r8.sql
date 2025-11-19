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
    has_reception boolean,
    can_place_tower boolean,
    visible_uncovered_population numeric,
    distance_to_closest_tower double precision
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
set population = p.population
from population_h3_r8 p
where s.h3 = p.h3::h3index;

update mesh_surface_h3_r8 s
set has_tower = true,
    has_reception = true,
    can_place_tower = false
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

update mesh_surface_h3_r8 s
set can_place_tower = false
where can_place_tower is distinct from false
  and (
        not exists (
            select 1
            from georgia_boundary b
            where ST_Intersects(b.geom, s.geom)
        )
        or has_road is not true
        or has_tower
        or distance_to_closest_tower < 5000
    );

update mesh_surface_h3_r8
set can_place_tower = true
where can_place_tower is null
  and has_road
  and distance_to_closest_tower >= 5000;

create index if not exists mesh_surface_h3_r8_geom_idx on mesh_surface_h3_r8 using gist (geom);
create index if not exists mesh_surface_h3_r8_geog_idx on mesh_surface_h3_r8 using gist (centroid_geog);
create index if not exists mesh_surface_h3_r8_brin_all on mesh_surface_h3_r8 using brin (ele, population, visible_uncovered_population, distance_to_closest_tower);
-- Create btree index to speed up distance-based eligibility filtering
create index if not exists mesh_surface_h3_r8_distance_idx on mesh_surface_h3_r8 (distance_to_closest_tower);
