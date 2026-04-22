set client_min_messages = notice;

\set max_distance 100000
\set refresh_radius 100000

-- Return ordered intermediate H3 cells for cheapest bridge between two clusters.
drop function if exists mesh_route_intermediate_hexes(integer, integer);
create or replace function mesh_route_intermediate_hexes(
        start_cluster integer,
        end_cluster integer
    )
    returns table (seq integer, h3 h3index)
    language plpgsql
    volatile
as
$$
declare
    start_vids integer[];
    end_vids integer[];
    separation double precision := 0;
    start_h3s h3index[];
    end_h3s h3index[];
begin
    select greatest(coalesce((
        select value::double precision
        from mesh_pipeline_settings
        where setting = 'min_tower_separation_m'
    ), 0), 0)
    into separation;

    -- Pick the nearest tower-node pair between the two clusters and route
    -- between those anchors. Sending every tower node in both clusters into
    -- pgr_dijkstra makes each bridge attempt explode combinatorially.
    with start_towers as (
        select
            mrn.node_id,
            mrn.h3,
            mt.centroid_geog
        from mesh_route_nodes mrn
        join mesh_towers mt on mt.h3 = mrn.h3
        join mesh_tower_clusters() tc on tc.tower_id = mt.tower_id
        where tc.cluster_id = start_cluster
    ),
    end_towers as (
        select
            mrn.node_id,
            mrn.h3,
            mt.centroid_geog
        from mesh_route_nodes mrn
        join mesh_towers mt on mt.h3 = mrn.h3
        join mesh_tower_clusters() tc on tc.tower_id = mt.tower_id
        where tc.cluster_id = end_cluster
    ),
    nearest_pair as (
        select
            array[start_towers.node_id] as start_vids,
            array[end_towers.node_id] as end_vids,
            array[start_towers.h3] as start_h3s,
            array[end_towers.h3] as end_h3s
        from start_towers
        cross join end_towers
        order by start_towers.centroid_geog <-> end_towers.centroid_geog
        limit 1
    )
    select
        nearest_pair.start_vids,
        nearest_pair.end_vids,
        nearest_pair.start_h3s,
        nearest_pair.end_h3s
    into start_vids, end_vids, start_h3s, end_h3s
    from nearest_pair;

    if start_vids is null or array_length(start_vids, 1) = 0 then
        return;
    end if;

    if end_vids is null or array_length(end_vids, 1) = 0 then
        return;
    end if;

    if to_regclass('pg_temp.mesh_route_cluster_blocked_nodes') is null then
        create temporary table mesh_route_cluster_blocked_nodes (
            node_id integer primary key
        ) on commit drop;
    else
        truncate mesh_route_cluster_blocked_nodes;
    end if;

    insert into mesh_route_cluster_blocked_nodes (node_id)
    select mrn.node_id
    from mesh_route_nodes mrn
    join mesh_surface_h3_r8 surface on surface.h3 = mrn.h3
    where (start_vids is null or mrn.node_id <> all(start_vids))
      and (end_vids is null or mrn.node_id <> all(end_vids))
      and surface.centroid_geog is not null
      and exists (
            select 1
            from mesh_towers mt
            where (start_h3s is null or mt.h3 <> all(start_h3s))
              and (end_h3s is null or mt.h3 <> all(end_h3s))
              and ST_DWithin(surface.centroid_geog, mt.centroid_geog, separation)
        );

    -- Use pgRouting to find the minimum path-loss corridor, then keep only cells that do not already host towers.
    return query
    with raw_paths as (
        select
            pdr.*,
            sum(case when pdr.path_seq = 1 then 1 else 0 end) over (order by pdr.seq) as path_id
        from pgr_dijkstra(
            'select edge_id as id,
                    source,
                    target,
                    cost,
                    reverse_cost
             from mesh_route_edges
             where source not in (select node_id from mesh_route_cluster_blocked_nodes)
               and target not in (select node_id from mesh_route_cluster_blocked_nodes)',
            start_vids,
            end_vids,
            false
        ) as pdr
    ),
    best_path as (
        select path_id
        from raw_paths
        where node <> -1
        group by path_id
        order by max(agg_cost) asc
        limit 1
    ),
    ordered_nodes as (
        select
            rp.seq,
            rn.h3
        from raw_paths rp
        join best_path bp on bp.path_id = rp.path_id
        join mesh_route_nodes rn on rn.node_id = rp.node
        join mesh_surface_h3_r8 s on s.h3 = rn.h3
        where rp.node <> -1
          and s.has_tower is not true
        order by rp.seq
    )
    select ordered_nodes.seq, ordered_nodes.h3 from ordered_nodes;
end;
$$;

do
$$
declare
    max_distance constant double precision := 100000;
    refresh_radius constant double precision := 100000;
    total_new integer := 0;
    cluster_total integer;
    candidate_pair_limit constant integer := 256;
    max_pair_attempts_per_run constant integer := 256;
    attempted_pairs integer := 0;
    start_cluster integer;
    end_cluster integer;
    pair_distance double precision;
    path_node_count integer;
    route_added integer;
    new_h3 h3index;
    new_centroid public.geography;
begin
    -- Route insertion triggers expensive local LOS-based surface refreshes, so
    -- disable statement timeout for this transaction just like cluster slim.
    perform set_config('statement_timeout', '0', true);

    -- Ensure the seed step populated the routing graph before trying to bridge anything.
    if to_regclass('mesh_route_nodes') is null then
        raise notice 'mesh_route_nodes table missing, skipping routing';
        return;
    end if;

    if not exists (select 1 from mesh_route_nodes) then
        raise notice 'mesh_route_nodes not prepared, skipping routing';
        return;
    end if;

    if to_regclass('mesh_route_edges') is null then
        raise notice 'mesh_route_edges table missing, skipping routing';
        return;
    end if;

    if not exists (select 1 from mesh_route_edges) then
        raise notice 'mesh_route_edges not prepared, skipping routing';
        return;
    end if;

    -- Short-circuit if everything already belongs to one connected component.
    select count(distinct cluster_id)
    into cluster_total
    from mesh_tower_clusters();

    if cluster_total is null or cluster_total <= 1 then
        raise notice 'Tower graph already forms a single cluster, skipping routing';
        return;
    end if;

    -- Temporary table stores the path for the current cluster pair so we can reuse it across statements.
    drop table if exists mesh_route_path_nodes_work;
    create temporary table mesh_route_path_nodes_work (
        seq integer,
        h3 h3index
    ) on commit drop;

    if to_regclass('mesh_route_edge_components') is null then
        raise notice 'mesh_route_edge_components table missing, skipping routing';
        return;
    end if;

    if not exists (select 1 from mesh_route_edge_components) then
        raise notice 'mesh_route_edge_components not prepared, skipping routing';
        return;
    end if;

    -- Materialize country polygons once so pair ranking can prefer local bridges
    -- before cross-border links without changing pgRouting edge costs.
    drop table if exists mesh_route_country_polygons;
    create temporary table mesh_route_country_polygons as
    with admin_polygons as (
        select
            case
                when lower(coalesce(tags ->> 'ISO3166-1:alpha2', '')) = 'am' then 'am'
                when lower(
                    coalesce(
                        nullif(tags ->> 'name:en', ''),
                        nullif(tags ->> 'int_name', ''),
                        nullif(tags ->> 'name', '')
                    )
                ) = any (array['georgia', 'sakartvelo', 'republic of georgia']) then 'ge'
                else null
            end as country_code,
            ST_Multi(geog::geometry) as geom
        from osm_for_mesh_placement
        where tags ? 'boundary'
          and tags ->> 'boundary' = 'administrative'
          and tags ->> 'admin_level' = '2'
          and ST_GeometryType(geog::geometry) in ('ST_Polygon', 'ST_MultiPolygon')
    )
    select
        country_code,
        ST_Union(geom) as geom
    from admin_polygons
    where country_code is not null
    group by country_code;

    create index if not exists mesh_route_country_polygons_geom_idx
        on mesh_route_country_polygons using gist (geom);
    analyze mesh_route_country_polygons;

    -- Track cluster pairs that have already been attempted so we can try the rest even if one fails.
    drop table if exists mesh_route_failed_pairs;
    create temporary table mesh_route_failed_pairs (
        cluster_a integer,
        cluster_b integer,
        failure_reason text,
        unique (cluster_a, cluster_b)
    ) on commit drop;

    loop
        if attempted_pairs >= max_pair_attempts_per_run then
            raise notice 'Reached % bridge pair attempts without finishing cluster bridging in this run',
                max_pair_attempts_per_run;
            exit;
        end if;

        -- Refresh cluster counts each iteration because new towers change connectivity.
        select count(distinct cluster_id)
        into cluster_total
        from mesh_tower_clusters();

        exit when cluster_total is null or cluster_total <= 1;

        with cluster_points as (
            -- Collect tower centroids per cluster so we can measure inter-cluster distance.
            select
                tc.cluster_id,
                mt.centroid_geog,
                country.country_code,
                rec.component
            from mesh_tower_clusters() tc
            join mesh_towers mt on mt.tower_id = tc.tower_id
            join mesh_route_nodes mrn on mrn.h3 = mt.h3
            join mesh_route_edge_components rec on rec.node = mrn.node_id
            left join mesh_route_country_polygons country
              on ST_Intersects(mt.h3::geometry, country.geom)
        ),
        cluster_pairs as (
            -- Rank cluster pairs by the closest tower-to-tower gap instead of the
            -- widest centroid gap. The partial route graph is far more likely to
            -- connect neighboring clusters than pairs hundreds of kilometers apart.
            select
                cp1.cluster_id as cluster_a,
                cp2.cluster_id as cluster_b,
                min(
                    case
                        when cp1.country_code is not null
                         and cp1.country_code = cp2.country_code then 0
                        when cp1.country_code is null
                          or cp2.country_code is null then 1
                        else 2
                    end
                ) as country_priority,
                min(ST_Distance(cp1.centroid_geog, cp2.centroid_geog)) as cluster_distance
            from cluster_points cp1
            join cluster_points cp2 on cp1.cluster_id < cp2.cluster_id
            where not exists (
                select 1
                from mesh_route_failed_pairs fp
                where fp.cluster_a = cp1.cluster_id
                  and fp.cluster_b = cp2.cluster_id
            )
              and cp1.component = cp2.component
            group by cp1.cluster_id, cp2.cluster_id
        )
        -- Lock in the next cluster pair to bridge. Searching a bounded window of
        -- the closest gaps keeps the stage moving instead of spending minutes on
        -- extreme long-shot pairs that the current graph cannot possibly bridge.
        select cluster_a, cluster_b, cluster_distance
        into start_cluster, end_cluster, pair_distance
        from (
            select
                cluster_a,
                cluster_b,
                country_priority,
                cluster_distance
            from cluster_pairs
            order by country_priority asc, cluster_distance asc
            limit candidate_pair_limit
        ) ranked_cluster_pairs
        order by country_priority asc, cluster_distance asc
        limit 1;

        exit when start_cluster is null or end_cluster is null;

        attempted_pairs := attempted_pairs + 1;

        raise notice 'Bridge attempt %: clusters % -> % (nearest gap % m)',
            attempted_pairs,
            start_cluster,
            end_cluster,
            pair_distance;

        -- Reset the path buffer before computing the next route.
        truncate mesh_route_path_nodes_work;

        -- Materialize the proposed bridge nodes for the selected clusters.
        insert into mesh_route_path_nodes_work (seq, h3)
        select *
        from mesh_route_intermediate_hexes(start_cluster, end_cluster);

        -- Count how many new cells we would install along this corridor.
        select count(*)
        into path_node_count
        from mesh_route_path_nodes_work;

        raise notice 'Bridge attempt % path candidate count: %',
            attempted_pairs,
            path_node_count;

        if path_node_count = 0 then
            insert into mesh_route_failed_pairs (cluster_a, cluster_b, failure_reason)
            values (
                least(start_cluster, end_cluster),
                greatest(start_cluster, end_cluster),
                'no los corridor'
            )
            on conflict do nothing;

            raise notice 'No feasible LOS route between clusters % and % (distance % m), trying next pair',
                start_cluster,
                end_cluster,
                pair_distance;
            continue;
        end if;

        route_added := 0;

        -- Promote every intermediate H3 from the chosen corridor.
        -- Do not distance-dedup here: close route cells can have different
        -- cached LOS neighbor sets and may be required for cluster connectivity.
        -- Redundant generated towers are pruned later by mesh_tower_wiggle only
        -- when a nearby tower preserves the same visible-neighbor set.
        for new_h3 in
            select pnw.h3
            from mesh_route_path_nodes_work pnw
            join mesh_surface_h3_r8 s on s.h3 = pnw.h3
            order by pnw.seq
        loop
            -- Grab the candidate centroid before deciding whether to promote it.
            select centroid_geog
            into new_centroid
            from mesh_surface_h3_r8
            where h3 = new_h3;

            insert into mesh_towers (h3, source)
            values (new_h3, 'route')
            on conflict (h3) do nothing;

            if not found then
                continue;
            end if;

            route_added := route_added + 1;

            -- Mark the promoted cell as a tower and reset cached metrics.
            update mesh_surface_h3_r8
            set has_tower = true,
                clearance = null,
                path_loss = null,
                visible_uncovered_population = 0,
                distance_to_closest_tower = 0
            where h3 = new_h3;

            -- Invalidate/adjust nearby cells so refresh functions can recompute accurate stats.
            update mesh_surface_h3_r8
            set clearance = null,
                path_loss = null,
                visible_uncovered_population = null,
                visible_tower_count = null,
                distance_to_closest_tower = coalesce(
                    least(
                        distance_to_closest_tower,
                        ST_Distance(centroid_geog, new_centroid)
                    ),
                    ST_Distance(centroid_geog, new_centroid)
                )
            where h3 <> new_h3
              and ST_DWithin(centroid_geog, new_centroid, refresh_radius);

        end loop;

        if route_added = 0 then
            insert into mesh_route_failed_pairs (cluster_a, cluster_b, failure_reason)
            values (
                least(start_cluster, end_cluster),
                greatest(start_cluster, end_cluster),
                'existing towers cover path'
            )
            on conflict do nothing;

            raise notice 'Clusters % and % already linked by existing towers (distance % m), trying next pair',
                start_cluster,
                end_cluster,
                pair_distance;
            continue;
        end if;

        total_new := total_new + route_added;

        raise notice 'Bridge attempt % inserted % route towers; deferred local surface refresh to later route_refresh_visibility stage',
            attempted_pairs,
            route_added;
    end loop;

    -- Drop helper artifacts so future runs start from a clean slate.
    drop table if exists mesh_route_path_nodes_work;
    drop table if exists mesh_route_failed_pairs;

    raise notice 'Inserted % routing towers prior to greedy placement', coalesce(total_new, 0);
end;
$$;

drop function if exists mesh_route_intermediate_hexes(integer, integer);

vacuum analyze mesh_surface_h3_r8;
