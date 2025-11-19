set client_min_messages = notice;

update mesh_surface_h3_r8
set has_reception = null,
    visible_uncovered_population = null
where has_tower is not true;

update mesh_surface_h3_r8
set has_reception = true,
    visible_uncovered_population = 0
where has_tower;
