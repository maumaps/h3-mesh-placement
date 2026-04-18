set client_min_messages = warning;

drop function if exists mesh_route_corridor_between_towers(h3index, h3index);
drop function if exists mesh_route_corridor_between_towers(h3index, h3index, integer[]);
-- Recover intermediate routing nodes between two towers using the cached pgRouting graph.
create or replace function mesh_route_corridor_between_towers(
        source_h3 h3index,
        target_h3 h3index,
        blocked_nodes integer[] default null
    )
    returns table (seq integer, h3 h3index)
    language plpgsql
    volatile
as
$$
declare
    start_node integer;
    end_node integer;
    separation constant double precision := 0;
begin
    select node_id
    into start_node
    from mesh_route_nodes
    where mesh_route_nodes.h3 = source_h3;

    select node_id
    into end_node
    from mesh_route_nodes
    where mesh_route_nodes.h3 = target_h3;

    if start_node is null or end_node is null then
        return;
    end if;

    if to_regclass('pg_temp.mesh_route_blocked_nodes') is null then
        create temporary table mesh_route_blocked_nodes (
            node_id integer primary key
        ) on commit drop;
    else
        truncate mesh_route_blocked_nodes;
    end if;

    -- Zero-separation blocking only needs exact tower H3 matches, so avoid scanning the
    -- whole surface and tower set through ST_DWithin().
    insert into mesh_route_blocked_nodes (node_id)
    select mrn.node_id
    from mesh_route_nodes mrn
    join mesh_towers mt on mt.h3 = mrn.h3
    where mrn.node_id not in (start_node, end_node)
      and mt.h3 not in (source_h3, target_h3);

    if blocked_nodes is not null then
        insert into mesh_route_blocked_nodes (node_id)
        select unnest(blocked_nodes)
        on conflict (node_id) do nothing;
    end if;

    return query
    with path_vertices as (
        -- Run pgRouting across the cached LOS graph to recover the minimum-cost corridor.
        select *
        from pgr_dijkstra(
            'select e.edge_id as id, e.source, e.target, e.cost, e.reverse_cost
             from mesh_route_edges e
             left join mesh_route_blocked_nodes blocked_source on blocked_source.node_id = e.source
             left join mesh_route_blocked_nodes blocked_target on blocked_target.node_id = e.target
             where blocked_source.node_id is null
               and blocked_target.node_id is null',
            start_node,
            end_node,
            false
        )
        where node <> -1
        order by seq
    ),
    ordered_nodes as (
        -- Attach H3 cells for each traversed vertex, skipping the endpoints because they already host towers.
        select
            row_number() over (order by pv.seq)::integer as seq,
            mrn.h3
        from path_vertices pv
        join mesh_route_nodes mrn on mrn.node_id = pv.node
        where mrn.h3 not in (source_h3, target_h3)
        order by pv.seq
    )
    select ordered_nodes.seq, ordered_nodes.h3 from ordered_nodes;
end;
$$;
