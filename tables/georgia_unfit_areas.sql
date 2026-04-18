set client_min_messages = warning;

drop table if exists georgia_unfit_areas;
-- Create dissolved polygons for regions where node placement is forbidden.
create table georgia_unfit_areas as
with admin_regions as (
    -- Load country/region polygons from the merged OSM extract to find forbidden regions.
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
),
georgia as (
    -- Load the Georgia country polygon to remove the shared Armenia-Georgia border from the exclusion band.
    select ST_Union(geom) as geom
    from admin_regions
    where normalized_name in ('georgia', 'sakartvelo', 'republic of georgia')
),
armenia as (
    -- Load the Armenia country polygon so we can construct an interior border band.
    select ST_Union(geom) as geom
    from admin_regions
    where normalized_name = 'armenia'
),
armenia_border_band as (
    -- Keep the Armenian 10 km border band, excluding the shared Georgia border segment.
    select
        'armenia_non_georgia_border'::text as region,
        ST_Difference(
            ST_Difference(
                a.geom,
                ST_Buffer(a.geom::geography, -10000)::geometry
            ),
            ST_Buffer(
                ST_Intersection(ST_Boundary(a.geom), ST_Boundary(g.geom))::geography,
                10000
            )::geometry
        ) as geom
    from armenia a
    cross join georgia g
),
military_buffer as (
    -- Buffer all mapped military features by 2 km regardless of source geometry type.
    select
        'military'::text as region,
        ST_Union(ST_Buffer(geog, 2000)::geometry) as geom
    from osm_for_mesh_placement
    where tags ? 'military'
       or (tags ? 'landuse' and tags ->> 'landuse' = 'military')
),
all_regions as (
    -- Stack every forbidden geometry so the final table can retain human-readable region reasons.
    select region, geom from matched_regions where region is not null
    union all
    select region, geom from armenia_border_band where geom is not null and not ST_IsEmpty(geom)
    union all
    select region, geom from military_buffer where geom is not null and not ST_IsEmpty(geom)
)
select
    region,
    ST_Union(geom) as geom
from all_regions
group by region;
alter table georgia_unfit_areas add primary key (region);
create index if not exists georgia_unfit_areas_geom_idx on georgia_unfit_areas using gist (geom);
