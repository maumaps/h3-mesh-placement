drop table if exists mesh_initial_nodes_h3_r8;
-- Create seed towers table projected to H3 resolution 8
create table mesh_initial_nodes_h3_r8 as
with allowed_initial_nodes as (
    -- Keep curated seed points unchanged, but clip MQTT-imported nodes to the
    -- Georgia+Armenia planning outline plus an 100 km border buffer in PostGIS.
    select
        mesh_initial_nodes.geom,
        mesh_initial_nodes.name,
        mesh_initial_nodes.source
    from mesh_initial_nodes
    cross join georgia_boundary
    where coalesce(mesh_initial_nodes.source, 'curated') <> 'mqtt'
       or ST_DWithin(
            mesh_initial_nodes.geom::geography,
            georgia_boundary.geom::geography,
            100000
        )
)
select
    h3_latlng_to_cell(geom, 8) as h3,
    string_agg(
        coalesce(name, 'seed'),
        ', '
        order by
            case when coalesce(source, 'curated') <> 'mqtt' then 0 else 1 end,
            coalesce(name, 'seed')
    ) as name,
    case
        when bool_or(coalesce(source, 'curated') <> 'mqtt') then 'seed'
        when bool_or(source = 'mqtt') then 'mqtt'
        else 'seed'
    end as source,
    ST_Collect(geom) as geom
from allowed_initial_nodes
group by 1;
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
