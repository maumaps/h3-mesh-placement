set client_min_messages = warning;

drop table if exists georgia_unfit_areas;
-- Create dissolved polygons for regions where node placement is forbidden
create table georgia_unfit_areas as
with admin_regions as (
    -- Load administrative polygons from the merged OSM extract to find forbidden regions.
    select
        lower(
            coalesce(
                nullif(tags ->> 'name:en', ''),
                nullif(tags ->> 'int_name', ''),
                nullif(tags ->> 'short_name', ''),
                nullif(tags ->> 'name', '')
            )
        ) as normalized_name,
        geog::geometry as geom
    from osm_for_mesh_placement
    where tags ? 'boundary'
      and tags ->> 'boundary' = 'administrative'
      and ST_GeometryType(geog::geometry) in ('ST_Polygon', 'ST_MultiPolygon')
),
matched_regions as (
    -- Match administrative regions we want to exclude from candidate placement.
    select
        case
            when normalized_name like '%abkhazia%' then 'abkhazia'
            when normalized_name like '%south ossetia%' then 'south ossetia'
            when normalized_name like '%tskhinvali%' then 'south ossetia'
        end as region,
        geom
    from admin_regions
    where normalized_name like '%abkhazia%'
       or normalized_name like '%south ossetia%'
       or normalized_name like '%tskhinvali%'
)
select
    region,
    ST_Union(geom) as geom
from matched_regions
where region is not null
group by region;
alter table georgia_unfit_areas add primary key (region);
create index if not exists georgia_unfit_areas_geom_idx on georgia_unfit_areas using gist (geom);
