set client_min_messages = warning;

drop function if exists mesh_visibility_invisible_route_geom(h3index, h3index);
-- Generate an elevation-weighted H3 corridor between two cells using pgRouting
create or replace function mesh_visibility_invisible_route_geom(
        source_h3 h3index,
        target_h3 h3index
    )
    returns geometry
    language plpgsql
    volatile
as
$$
declare
    start_node integer;
    end_node integer;
    routed_geom geometry;
    cached_geom geometry;
    canonical_source h3index;
    canonical_target h3index;
    stored_geom geometry;
    separation constant double precision := 0;
begin
    -- Normalize the pair ordering so cache lookups stay consistent regardless of call direction.
    if source_h3::text <= target_h3::text then
        canonical_source := source_h3;
        canonical_target := target_h3;
    else
        canonical_source := target_h3;
        canonical_target := source_h3;
    end if;

    select cache.geom
    into cached_geom
    from mesh_route_graph_cache cache
    where cache.source_h3 = canonical_source
      and cache.target_h3 = canonical_target;

    if found then
        if source_h3 = canonical_source then
            return cached_geom;
        else
            return ST_Reverse(cached_geom);
        end if;
    end if;

    -- Lookup pgRouting node ids for both endpoints.
    select node_id into start_node from mesh_route_graph_nodes where h3 = source_h3;
    select node_id into end_node from mesh_route_graph_nodes where h3 = target_h3;

    if start_node is null or end_node is null then
        return null;
    end if;

    if to_regclass('pg_temp.mesh_route_graph_blocked_nodes') is null then
        create temporary table mesh_route_graph_blocked_nodes (
            node_id integer primary key
        ) on commit drop;
    else
        truncate mesh_route_graph_blocked_nodes;
    end if;

    -- With zero separation we only need to block exact tower cells, so join by H3 instead of
    -- scanning every route node through ST_DWithin().
    insert into mesh_route_graph_blocked_nodes (node_id)
    select mrgn.node_id
    from mesh_route_graph_nodes mrgn
    join mesh_towers mt on mt.h3 = mrgn.h3
    where mrgn.node_id not in (start_node, end_node)
      and mt.h3 not in (canonical_source, canonical_target);

    with path_vertices as (
        -- Run pgRouting across the global routing graph to recover the minimum-cost corridor.
        select *
        from pgr_dijkstra(
            'select e.edge_id as id,
                    e.source_node_id as source,
                    e.target_node_id as target,
                    e.cost,
                    e.cost as reverse_cost
             from mesh_route_graph_edges e
             left join mesh_route_graph_blocked_nodes blocked_source on blocked_source.node_id = e.source_node_id
             left join mesh_route_graph_blocked_nodes blocked_target on blocked_target.node_id = e.target_node_id
             where blocked_source.node_id is null
               and blocked_target.node_id is null',
            start_node,
            end_node,
            true
        )
        where node <> -1
        order by seq
    ),
    vertex_points as (
        -- Attach back the centroid geometry for each visited node in order.
        select
            pv.seq,
            mrgn.geom
        from path_vertices pv
        join mesh_route_graph_nodes mrgn on mrgn.node_id = pv.node
        order by pv.seq
    )
    select
        case
            when count(*) = 0 then null
            else ST_MakeLine(vp.geom order by vp.seq)
        end
    into routed_geom
    from vertex_points vp;

    if routed_geom is null then
        return null;
    end if;

    stored_geom := case
        when source_h3 = canonical_source then routed_geom
        else ST_Reverse(routed_geom)
    end;

    insert into mesh_route_graph_cache (source_h3, target_h3, geom, created_at)
    values (canonical_source, canonical_target, stored_geom, now())
    on conflict on constraint mesh_route_graph_cache_pkey do update
        set geom = excluded.geom,
            created_at = excluded.created_at;

    if source_h3 = canonical_source then
        return routed_geom;
    else
        return ST_Reverse(routed_geom);
    end if;
end;
$$;
