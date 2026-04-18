set client_min_messages = warning;

drop table if exists population_h3_r8;
-- Create aggregated H3 population table using Kontur-provided resolution 8 indexes plus road fallback.
create table population_h3_r8 (
    h3 h3index primary key,
    population numeric
);

do
$$
begin
    if not exists (
        select 1
        from information_schema.columns
        where table_schema = 'public'
          and table_name = 'roads_h3_r8'
          and column_name = 'h3'
    ) then
        raise exception
            'roads_h3_r8 is missing the h3 column required for road-backed population fallback; rebuild it with psql -f tables/roads_h3_r8.sql or make -B db/table/roads_h3_r8';
    end if;
end;
$$;

-- Repair missing or empty population rows without rebuilding existing values.
with boundary as (
    -- Load the combined Georgia + Armenia boundary for Kontur clipping.
    select geom as boundary_geom
    from georgia_boundary
),
kontur_aggregated as (
    -- Aggregate Kontur population by H3 cell inside the boundary.
    select
        k.h3::h3index as h3,
        sum(k.population)::numeric as population
    from kontur_population k
    join boundary b
        on ST_Intersects(k.geom, b.boundary_geom)
    where k.h3 is not null
    group by k.h3
),
road_backfill as (
    -- Treat every road-served cell as populated so sparse settlements still seed the planner.
    select
        r.h3::h3index as h3,
        1::numeric as population
    from roads_h3_r8 r
),
aggregated as (
    -- Combine real Kontur totals with the road-backed fallback population floor.
    select
        combined.h3,
        max(combined.population)::numeric as population
    from (
        select h3, population from kontur_aggregated
        union all
        select h3, population from road_backfill
    ) combined
    group by combined.h3
),
missing as (
    -- Identify H3 rows that are missing or zero so we can repair only those.
    select
        a.h3,
        a.population
    from aggregated a
    left join population_h3_r8 p on p.h3 = a.h3
    where p.h3 is null
       or p.population is null
       or p.population = 0
)
insert into population_h3_r8 (h3, population)
select h3, population
from missing
on conflict (h3) do nothing;

-- Refresh mesh_surface_h3_r8 population values when the surface table already exists.
do
$$
begin
    if to_regclass('mesh_surface_h3_r8') is not null
       and exists (
            select 1
            from information_schema.columns
            where table_schema = 'public'
              and table_name = 'mesh_surface_h3_r8'
              and column_name = 'h3'
       ) then
        -- Repair surface population values that were left empty during a partial run.
        update mesh_surface_h3_r8 s
        set population = p.population
        from population_h3_r8 p
        where s.h3 = p.h3::h3index
          and (s.population is null or s.population = 0);
    end if;
end;
$$;
