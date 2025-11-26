set client_min_messages = warning;

drop table if exists georgia_boundary;
-- Create dissolved boundary of Georgia and Armenia from OSM polygons
create table georgia_boundary as
with georgia_polygons as (
    select
        lower(
            coalesce(
                nullif(tags ->> 'name:en', ''),
                nullif(tags ->> 'short_name', ''),
                nullif(tags ->> 'int_name', ''),
                nullif(tags ->> 'name', '')
            )
        ) as normalized_name,
        ST_Multi(geog::geometry) as geom
    from osm_caucasus
    where tags ? 'boundary'
      and tags ->> 'boundary' = 'administrative'
      and tags ->> 'admin_level' = '2'
      and ST_GeometryType(geog::geometry) in ('ST_Polygon', 'ST_MultiPolygon')
),
georgia as (
    select geom
    from georgia_polygons
    where normalized_name in (
        'georgia',
        'sakartvelo',
        'republic of georgia'
    )
),
armenia as (
    select ST_Multi(geog::geometry) as geom
    from osm_caucasus
    where tags ? 'boundary'
      and tags ->> 'boundary' = 'administrative'
      and tags ->> 'admin_level' = '2'
      and ST_GeometryType(geog::geometry) in ('ST_Polygon', 'ST_MultiPolygon')
      and (
            lower(coalesce(tags ->> 'ISO3166-1:alpha2', '')) = 'am'
            or lower(coalesce(tags ->> 'int_name', '')) = 'armenia'
            or lower(coalesce(tags ->> 'name:en', '')) = 'armenia'
            or tags ->> 'wikidata' = 'Q399'
        )
),
country_polygons as (
    select geom from georgia
    union all
    select geom from armenia
)
select ST_Union(geom) as geom
from country_polygons;
