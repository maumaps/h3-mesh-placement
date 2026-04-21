set client_min_messages = warning;

-- Rebuild mesh_visibility_edges along with visibility diagnostics and hop counts.
create or replace procedure mesh_visibility_edges_refresh()
    language plpgsql
as
$$
declare
    missing_list text;
    missing_count integer;
    v_tower_count integer;
    v_edge_count integer;
    v_visible_count integer;
begin
    select count(*) into v_tower_count from mesh_towers;
    raise notice 'Visibility edges refresh: rebuilding from % towers', v_tower_count;

    truncate mesh_visibility_edges;

if to_regclass('tmp_visibility_missing_elevation') is not null then
    execute 'drop table tmp_visibility_missing_elevation';
end if;
    -- Temporary table capturing towers lacking GEBCO elevation coverage
    create temporary table tmp_visibility_missing_elevation (
        tower_id integer primary key,
        h3 h3index not null
    ) on commit preserve rows;

    insert into tmp_visibility_missing_elevation (tower_id, h3)
    select
        t.tower_id,
        t.h3
    from mesh_towers t
    left join gebco_elevation_h3_r8 ge
      on ge.h3 = t.h3
    where ge.h3 is null;

    with eligible_towers as (
        -- Filter towers that have elevation samples so LOS math stays reliable and assign source ranks for type labels.
        select
            t.*,
            case t.source
                when 'seed' then 1
                when 'mqtt' then 1
                when 'coarse' then 2
                when 'population' then 3
                when 'route' then 4
                when 'bridge' then 5
                when 'cluster_slim' then 6
                when 'greedy' then 7
                else 99
            end as source_rank
        from mesh_towers t
        where not exists (
            select 1
            from tmp_visibility_missing_elevation missing
            where missing.h3 = t.h3
        )
    ),
    edge_pairs as (
        -- Build every tower-to-tower diagnostic edge so long invisible gaps remain available
        -- to routing heuristics and QGIS inspection.
        -- Only pairs within the shared 80 km planning radius pay the expensive LOS computation.
        select
            t1.tower_id as source_id,
            t2.tower_id as target_id,
            t1.h3 as source_h3,
            t2.h3 as target_h3,
            case
                when t1.source_rank < t2.source_rank then t1.source || '-' || t2.source
                when t1.source_rank > t2.source_rank then t2.source || '-' || t1.source
                when t1.source <= t2.source then t1.source || '-' || t2.source
                else t2.source || '-' || t1.source
            end as type,
            pair.distance_m,
            case
                when pair.distance_m <= 80000 then h3_los_between_cells(t1.h3, t2.h3)
                else false
            end as is_visible,
            ST_MakeLine(t1.centroid_geog::geometry, t2.centroid_geog::geometry) as geom
        from eligible_towers t1
        join eligible_towers t2
          on t1.tower_id < t2.tower_id
        cross join lateral (
            select ST_Distance(t1.centroid_geog, t2.centroid_geog) as distance_m
        ) pair
    )
    insert into mesh_visibility_edges (
        source_id,
        target_id,
        source_h3,
        target_h3,
        type,
        distance_m,
        is_visible,
        geom
    )
    select
        source_id,
        target_id,
        source_h3,
        target_h3,
        type,
        distance_m,
        is_visible,
        geom
    from edge_pairs;

if to_regclass('tmp_visibility_cluster_edges') is not null then
    execute 'drop table tmp_visibility_cluster_edges';
end if;
    -- Temporary graph holding LOS-adjacent tower pairs (<=80 km) so pgRouting can recover hop counts.
    create temporary table tmp_visibility_cluster_edges (
        edge_id bigserial primary key,
        source_id integer not null,
        target_id integer not null,
        cost double precision not null default 1
    )
    on commit preserve rows;

    insert into tmp_visibility_cluster_edges (source_id, target_id)
    select
        e.source_id,
        e.target_id
    from mesh_visibility_edges e
    where e.is_visible
      and e.distance_m <= 80000;

    with vertex_ids as (
        -- Limit pgRouting sources/targets to towers that participate in any LOS edge.
        select vid
        from (
            select source_id as vid from tmp_visibility_cluster_edges
            union
            select target_id as vid from tmp_visibility_cluster_edges
        ) v
    ),
    tower_components as (
        -- Recover connected components directly from the visible-edge graph we just built,
        -- instead of recomputing LOS a second time through mesh_tower_clusters().
        select
            et.tower_id,
            coalesce(cc.component, et.tower_id) as cluster_id
        from (
            select tower_id
            from mesh_towers t
            where not exists (
                select 1
                from tmp_visibility_missing_elevation missing
                where missing.h3 = t.h3
            )
        ) et
        left join pgr_connectedComponents(
            'select edge_id as id, source_id as source, target_id as target, cost, cost as reverse_cost from tmp_visibility_cluster_edges'
        ) cc on cc.node = et.tower_id
    ),
    cluster_hops as (
        -- Measure minimum hop count between every reachable tower pair inside a cluster.
        select
            least(result.start_vid, result.end_vid) as source_id,
            greatest(result.start_vid, result.end_vid) as target_id,
            result.agg_cost::integer as hops
        from pgr_dijkstra(
            'select edge_id as id, source_id as source, target_id as target, cost, cost as reverse_cost from tmp_visibility_cluster_edges',
            coalesce(array(select vid::bigint from vertex_ids order by vid), array[]::bigint[]),
            coalesce(array(select vid::bigint from vertex_ids order by vid), array[]::bigint[]),
            false
        ) as result
        where result.start_vid < result.end_vid
          and result.agg_cost < 'Infinity'::double precision
          and result.node = result.end_vid
    )
    update mesh_visibility_edges e
    set is_between_clusters = (src.cluster_id <> dst.cluster_id),
        cluster_hops = (
            select ch.hops
            from cluster_hops ch
            where ch.source_id = e.source_id
              and ch.target_id = e.target_id
        )
    from tower_components src,
         tower_components dst
    where src.tower_id = e.source_id
      and dst.tower_id = e.target_id;

if to_regclass('tmp_visibility_cluster_edges') is not null then
    execute 'drop table tmp_visibility_cluster_edges';
end if;

    select count(*), count(*) filter (where is_visible)
    into v_edge_count, v_visible_count
    from mesh_visibility_edges;
    raise notice 'Visibility edges refresh complete: % edges, % visible', v_edge_count, v_visible_count;

    select
        string_agg(h3::text, ', ' order by h3),
        count(*)
    into missing_list, missing_count
    from tmp_visibility_missing_elevation;

    if missing_count > 0 then
        raise warning 'Skipped % tower(s) without GEBCO elevation samples: %',
            missing_count,
            missing_list;
    end if;

if to_regclass('tmp_visibility_missing_elevation') is not null then
    execute 'drop table tmp_visibility_missing_elevation';
end if;
end;
$$;
