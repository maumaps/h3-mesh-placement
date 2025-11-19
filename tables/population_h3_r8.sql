set client_min_messages = warning;

drop table if exists population_h3_r8;
-- Create aggregated H3 population table using Kontur-provided resolution 8 indexes
create table population_h3_r8 as
with boundary as (
    select geom as boundary_geom from georgia_boundary
)
select
    k.h3::h3index as h3,
    sum(k.population)::numeric as population
from kontur_population k
join boundary b
    on ST_Intersects(k.geom, b.boundary_geom)
where k.h3 is not null
group by k.h3;
alter table population_h3_r8 add primary key (h3);
