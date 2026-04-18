set client_min_messages = warning;

drop table if exists buildings_h3_r8;
-- Create H3 table storing building counts per planning cell.
create table buildings_h3_r8 as
with building_features as (
    -- Reduce every building feature to one representative point so counting stays stable.
    select
        osm_id,
        h3_latlng_to_cell(ST_PointOnSurface(geog::geometry), 8) as h3
    from osm_for_mesh_placement
    where tags ? 'building'
      and coalesce(tags ->> 'building', '') not in ('', 'no')
      and ST_GeometryType(geog::geometry) in ('ST_Point', 'ST_MultiPoint', 'ST_LineString', 'ST_MultiLineString', 'ST_Polygon', 'ST_MultiPolygon')
)
select
    h3,
    count(*)::integer as building_count
from building_features
group by h3;

alter table buildings_h3_r8 add primary key (h3);
