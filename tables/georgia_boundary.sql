set client_min_messages = warning;

drop table if exists georgia_boundary;
-- Create dissolved boundary of Georgia and Armenia from merged OSM polygons
create table georgia_boundary as
with admin_polygons as (
    -- Collect admin-level-2 polygons from the merged OSM extract so mesh_surface_h3_r8 can cover both countries.
    select
        lower(
            coalesce(
                nullif(tags ->> 'name:en', ''),
                nullif(tags ->> 'short_name', ''),
                nullif(tags ->> 'int_name', ''),
                nullif(tags ->> 'name', '')
            )
        ) as normalized_name,
        tags,
        ST_Multi(geog::geometry) as geom
    from osm_for_mesh_placement
    where tags ? 'boundary'
      and tags ->> 'boundary' = 'administrative'
      and tags ->> 'admin_level' = '2'
      and ST_GeometryType(geog::geometry) in ('ST_Polygon', 'ST_MultiPolygon')
),
georgia as (
    -- Select Georgia admin boundary polygons to anchor the combined coverage area.
    select geom
    from admin_polygons
    where normalized_name in (
        'georgia',
        'sakartvelo',
        'republic of georgia'
    )
),
armenia as (
    -- Select Armenia admin boundary polygons so the combined domain includes Armenia coverage.
    select geom
    from admin_polygons
    where lower(coalesce(tags ->> 'ISO3166-1:alpha2', '')) = 'am'
       or lower(coalesce(tags ->> 'int_name', '')) = 'armenia'
       or lower(coalesce(tags ->> 'name:en', '')) = 'armenia'
       or tags ->> 'wikidata' = 'Q399'
),
country_polygons as (
    -- Combine Georgia and Armenia polygons for a single dissolved boundary used by mesh_surface_domain_h3_r8.
    select geom from georgia
    union all
    select geom from armenia
)
-- Dissolve the combined polygons into one geometry for consistent boundary checks.
select ST_Union(geom) as geom
from country_polygons;
