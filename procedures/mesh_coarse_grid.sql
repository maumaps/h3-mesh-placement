set client_min_messages = notice;

drop procedure if exists mesh_coarse_grid();
-- Seed one backbone tower per coarse H3 cell that lacks an installed tower already.
create or replace procedure mesh_coarse_grid()
language plpgsql
as
$$
declare
    enabled boolean := true;
    max_distance double precision := 80000;
    coarse_resolution integer := 4;
    inserted_count integer := 0;
begin
    -- Read the operator-maintained pipeline config once at the start so reruns
    -- can disable this placement stage without editing procedure code.
    select coalesce((
        select value::boolean
        from mesh_pipeline_settings
        where setting = 'enable_coarse'
    ), true)
    into enabled;

    select coalesce((
        select value::double precision
        from mesh_pipeline_settings
        where setting = 'max_los_distance_m'
    ), 80000)
    into max_distance;

    select coalesce((
        select value::integer
        from mesh_pipeline_settings
        where setting = 'coarse_resolution'
    ), 4)
    into coarse_resolution;

    -- Reset prior coarse towers so reruns stay idempotent and disabling the
    -- stage removes stale coarse anchors from previous runs.
    delete from mesh_towers where source = 'coarse';

    -- Clear surface flags for cells that just lost a coarse tower so spacing stays accurate.
    update mesh_surface_h3_r8 s
    set has_tower = false,
        distance_to_closest_tower = null
    where has_tower
      and not exists (
            select 1
            from mesh_towers t
            where t.h3 = s.h3
        );

    if not enabled then
        raise notice 'Coarse placement disabled by mesh_pipeline_settings.enable_coarse';
        return;
    end if;

    -- Recompute spacing after cleanup to refresh can_place_tower before seeding.
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

    with coarse_cells_with_towers as (
        -- Skip every coarse cell that already contains any installed tower.
        select distinct
            h3_cell_to_parent(t.h3, coarse_resolution) as coarse_h3
        from mesh_towers t
        where h3_get_resolution(t.h3) >= coarse_resolution
    ),
    candidate_cells as (
        -- Candidate cells must stay placeable, uncovered, and population-relevant.
        select
            s.h3,
            h3_cell_to_parent(s.h3, coarse_resolution) as coarse_h3,
            s.centroid_geog,
            s.has_building,
            coalesce(s.building_count, 0) as building_count,
            coalesce(s.population_70km, s.population, 0) as stage_population,
            coalesce(s.distance_to_closest_tower, 1e12) as spacing_score
        from mesh_surface_h3_r8 s
        where s.can_place_tower
          and coalesce(s.population_70km, s.population, 0) > 0
          and not exists (
                select 1
                from coarse_cells_with_towers occupied
                where occupied.coarse_h3 = h3_cell_to_parent(s.h3, coarse_resolution)
            )
          and not exists (
                select 1
                from mesh_towers t
                where ST_DWithin(s.centroid_geog, t.centroid_geog, max_distance)
                  and h3_los_between_cells(s.h3, t.h3)
            )
    ),
    coarse_winners as (
        -- Pick exactly one fine-resolution candidate per coarse cell.
        select distinct on (c.coarse_h3)
            c.coarse_h3,
            c.h3,
            c.has_building,
            c.stage_population,
            c.spacing_score,
            c.building_count
        from candidate_cells c
        order by
            c.coarse_h3,
            c.has_building desc,
            c.stage_population desc,
            c.spacing_score desc,
            c.building_count desc,
            c.h3
    ),
    inserted as (
        insert into mesh_towers (h3, source)
        select cw.h3, 'coarse'
        from coarse_winners cw
        on conflict (h3) do nothing
        returning h3
    )
    select coalesce(count(*), 0)
    into inserted_count
    from inserted;

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
end;
$$;
