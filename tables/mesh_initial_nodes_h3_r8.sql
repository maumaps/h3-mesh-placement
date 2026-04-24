drop table if exists mesh_initial_nodes_h3_r8;
-- Create seed towers table projected to H3 resolution 8
create table mesh_initial_nodes_h3_r8 as
with allowed_initial_nodes as (
    -- Keep curated seed points unchanged, but clip MQTT-imported nodes to the
    -- Georgia+Armenia planning outline plus an 100 km border buffer in PostGIS.
    select
        mesh_initial_nodes.geom,
        mesh_initial_nodes.name,
        case
            when coalesce(mesh_initial_nodes.source, 'curated') = 'mqtt' then 'mqtt'
            else 'seed'
        end as source
    from mesh_initial_nodes
    cross join georgia_boundary
    where coalesce(mesh_initial_nodes.source, 'curated') <> 'mqtt'
       or ST_DWithin(
            mesh_initial_nodes.geom::geography,
            georgia_boundary.geom::geography,
            100000
        )
), grouped_initial_nodes as (
    -- Prefer curated/seed names whenever a cell contains both seed and MQTT points.
    select
        h3_latlng_to_cell(geom, 8) as h3,
        bool_or(source = 'seed') as has_seed,
        string_agg(
            coalesce(name, 'seed'),
            ', '
            order by coalesce(name, 'seed')
        ) filter (
            where source = 'seed'
        ) as seed_names,
        string_agg(
            coalesce(name, 'seed'),
            ', '
            order by coalesce(name, 'seed')
        ) as all_names,
        ST_Collect(geom) as geom
    from allowed_initial_nodes
    group by 1
)
select
    h3,
    coalesce(seed_names, all_names) as name,
    case
        when has_seed then 'seed'
        else 'mqtt'
    end as source,
    geom
from grouped_initial_nodes;
alter table mesh_initial_nodes_h3_r8 add primary key (h3);

delete from mesh_towers t
where t.source in ('seed', 'mqtt')
  and (
        t.source = 'mqtt'
        or not exists (
            select 1
            from mesh_initial_nodes_h3_r8 seeds
            where seeds.h3 = t.h3
              and seeds.source = 'seed'
        )
    );

insert into mesh_towers (h3, source)
with recursive params as (
    -- Match the LOS-cache radio settings used by routing so installed MQTT roots
    -- cannot silently split the tower graph into disconnected components.
    select
        coalesce((
            select value::double precision
            from mesh_pipeline_settings
            where setting = 'max_los_distance_m'
        ), 100000) as max_distance_m,
        coalesce((
            select value::double precision
            from mesh_pipeline_settings
            where setting = 'mast_height_m'
        ), 28) as mast_height_m,
        coalesce((
            select value::double precision
            from mesh_pipeline_settings
            where setting = 'frequency_hz'
        ), 868000000) as frequency_hz
), initial_node_edges as (
    -- Use cached LOS between imported seed/MQTT nodes to keep only MQTT nodes
    -- reachable from curated seeds before the placement restart removes old
    -- generated route towers.
    select distinct
        least(mesh_los_cache.src_h3, mesh_los_cache.dst_h3) as source_h3,
        greatest(mesh_los_cache.src_h3, mesh_los_cache.dst_h3) as target_h3
    from mesh_los_cache
    join params on true
    join mesh_initial_nodes_h3_r8 src_node
      on src_node.h3 = mesh_los_cache.src_h3
    join mesh_initial_nodes_h3_r8 dst_node
      on dst_node.h3 = mesh_los_cache.dst_h3
    where mesh_los_cache.clearance > 0
      and mesh_los_cache.distance_m <= params.max_distance_m
      and mesh_los_cache.mast_height_src = params.mast_height_m
      and mesh_los_cache.mast_height_dst = params.mast_height_m
      and mesh_los_cache.frequency_hz = params.frequency_hz
), reachable_initial_nodes(h3) as (
    -- Start from curated seeds and walk the seed/MQTT LOS graph.
    select h3
    from mesh_initial_nodes_h3_r8
    where source = 'seed'

    union

    select
        case
            when initial_node_edges.source_h3 = reachable_initial_nodes.h3 then initial_node_edges.target_h3
            else initial_node_edges.source_h3
        end as h3
    from reachable_initial_nodes
    join initial_node_edges
      on reachable_initial_nodes.h3 in (
            initial_node_edges.source_h3,
            initial_node_edges.target_h3
        )
)
select
    mesh_initial_nodes_h3_r8.h3,
    mesh_initial_nodes_h3_r8.source
from mesh_initial_nodes_h3_r8
where mesh_initial_nodes_h3_r8.source = 'seed'
   or (
        mesh_initial_nodes_h3_r8.source = 'mqtt'
        and mesh_initial_nodes_h3_r8.h3 in (
            select h3
            from reachable_initial_nodes
        )
    )
on conflict (h3) do update
set source = excluded.source;
