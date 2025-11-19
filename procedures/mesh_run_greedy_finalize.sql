set client_min_messages = notice;

vacuum analyze mesh_surface_h3_r8;

truncate mesh_visibility_edges_active;
insert into mesh_visibility_edges_active (source_id, target_id, distance_m, is_visible, geom)
select
    t1.tower_id as source_id,
    t2.tower_id as target_id,
    ST_Distance(t1.centroid_geog, t2.centroid_geog) as distance_m,
    h3_los_between_cells(t1.h3, t2.h3) as is_visible,
    ST_MakeLine(t1.centroid_geog::geometry, t2.centroid_geog::geometry) as geom
from mesh_towers t1
join mesh_towers t2
    on t1.tower_id < t2.tower_id;
