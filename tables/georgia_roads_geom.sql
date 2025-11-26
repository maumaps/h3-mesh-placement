set client_min_messages = warning;

drop table if exists georgia_roads_geom;
create table georgia_roads_geom as
select ST_Multi(
           geog::geometry
       ) as geom,
       tags ->> 'highway' as highway
from osm_caucasus
where tags ? 'highway'
  and (tags ->> 'highway') = any (
        array[
            'motorway', 'motorway_link',
            'trunk', 'trunk_link',
            'primary', 'primary_link',
            'secondary', 'secondary_link',
            'tertiary', 'tertiary_link',
            'unclassified', 'residential',
            'living_street', 'service',
            'road'
        ]
    )
  and coalesce(
        lower(tags ->> 'motor_vehicle'),
        lower(tags ->> 'motorcar'),
        lower(tags ->> 'vehicle'),
        lower(tags ->> 'access'),
        'yes'
    ) not in ('no', 'private')
  and ST_GeometryType(geog::geometry) in ('ST_LineString', 'ST_MultiLineString');
