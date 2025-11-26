set client_min_messages = warning;

drop table if exists georgia_convex_hull;
-- Create convex hull covering the Georgia + Armenia boundary union
create table georgia_convex_hull as
select ST_ConvexHull(ST_UnaryUnion(geom)) as geom
from georgia_boundary;
