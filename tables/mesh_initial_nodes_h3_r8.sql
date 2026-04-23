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
  and not exists (
        select 1
        from mesh_initial_nodes_h3_r8 seeds
        where seeds.h3 = t.h3
    );

insert into mesh_towers (h3, source)
select h3, source
from mesh_initial_nodes_h3_r8
on conflict (h3) do update
set source = excluded.source;
