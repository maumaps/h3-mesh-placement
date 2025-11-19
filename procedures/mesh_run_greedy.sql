-- Constants for this single greedy iteration
\set max_distance 60000
\set separation 5000
\set recalc_reception 60000
\set recalc_population 60000

set client_min_messages = notice;

begin;

do $$ begin
    raise notice 'Refreshing has_reception cache';
end $$;

update mesh_surface_h3_r8 s
set has_reception = q.has_reception
from (
    select
        s1.h3,
        exists (
            select
            from mesh_towers t
            where ST_DWithin(s1.centroid_geog, t.centroid_geog, 60000)
              and h3_los_between_cells(s1.h3, t.h3)
        ) as has_reception
    from mesh_surface_h3_r8 s1
    where s1.has_reception is null
      and s1.distance_to_closest_tower < 60000
) q
where s.h3 = q.h3;

do $$ begin
    raise notice 'Enabling new tower candidates';
end $$;

update mesh_surface_h3_r8
set can_place_tower = true
where can_place_tower is null
  and has_road
  and not has_tower
  and distance_to_closest_tower between 5000 and 60000;

do $$ begin
    raise notice 'Recomputing visible_uncovered_population for pending candidates';
end $$;

update mesh_surface_h3_r8 s
set visible_uncovered_population = coalesce(q.visible_pop, 0)
from (
    select
        c.h3,
        (
            select sum(population)
            from mesh_surface_h3_r8 t
            where t.population > 0
              and t.has_reception is not true
              and ST_DWithin(c.centroid_geog, t.centroid_geog, 60000)
              and h3_los_between_cells(c.h3, t.h3)
        ) as visible_pop
    from mesh_surface_h3_r8 c
    where c.can_place_tower
      and c.visible_uncovered_population is null
      and c.distance_to_closest_tower < 60000
      and has_reception
) q
where s.h3 = q.h3;

do
$$
declare
    separation constant double precision := 5000;
    recalc_radius constant double precision := 60000;
    previous_iteration integer := coalesce((select max(iteration) from mesh_greedy_iterations), 0);
    next_iteration integer := previous_iteration + 1;
    candidate record;
begin
    select s.h3,
           s.centroid_geog,
           s.visible_uncovered_population
    into candidate
    from mesh_surface_h3_r8 s
    where s.can_place_tower
      and coalesce(s.distance_to_closest_tower, separation) >= separation
      and coalesce(s.visible_uncovered_population, 0) > 0
    order by s.visible_uncovered_population desc
    limit 1;

    raise notice 'Installing tower #% at % (visible population %)',
        next_iteration,
        candidate.h3::text,
        candidate.visible_uncovered_population;

    insert into mesh_towers (h3, source)
    values (candidate.h3, 'greedy')
    on conflict (h3) do update set source = excluded.source;

    insert into mesh_greedy_iterations (iteration, chosen_h3, visible_population)
    values (next_iteration, candidate.h3, candidate.visible_uncovered_population);

    update mesh_surface_h3_r8
    set has_tower = true,
        has_reception = true,
        can_place_tower = false,
        visible_uncovered_population = 0,
        distance_to_closest_tower = 0
    where h3 = candidate.h3;

    update mesh_surface_h3_r8
    set has_reception = null,
        visible_uncovered_population = null,
        distance_to_closest_tower = coalesce(
            least(
                distance_to_closest_tower,
                ST_Distance(centroid_geog, candidate.centroid_geog)
            ),
            ST_Distance(centroid_geog, candidate.centroid_geog)
        ),
        can_place_tower = case
            when ST_DWithin(centroid_geog, candidate.centroid_geog, separation) then false
            else can_place_tower
        end
    where h3 <> candidate.h3
      and ST_DWithin(centroid_geog, candidate.centroid_geog, recalc_radius);
end;
$$;

commit;
