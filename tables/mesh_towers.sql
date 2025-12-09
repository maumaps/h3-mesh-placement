set client_min_messages = warning;

drop table if exists mesh_towers;
-- Create register table for existing and proposed towers
create table mesh_towers (
    tower_id serial primary key,
    h3 h3index not null unique,
    source text not null default 'seed',
    -- Count how many automated wiggle/recenter passes touched this tower
    recalculation_count integer not null default 0,
    created_at timestamptz not null default now(),
    centroid_geog public.geography generated always as (h3_cell_to_geometry(h3)::public.geography) stored
);

create index if not exists mesh_towers_geog_idx on mesh_towers using gist (centroid_geog);
