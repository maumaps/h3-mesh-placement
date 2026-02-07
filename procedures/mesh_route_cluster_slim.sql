set client_min_messages = notice;

drop procedure if exists mesh_route_cluster_slim();
drop procedure if exists mesh_route_cluster_slim(integer);
drop procedure if exists mesh_route_cluster_slim(integer, integer);

-- Each invocation processes at most one corridor; loop externally for continuous runs.
create or replace procedure mesh_route_cluster_slim(iteration_label integer, inout promoted integer)
    language plpgsql
as
$$
declare
    max_distance constant double precision := 70000;
    refresh_radius constant double precision := 70000;
    separation constant double precision := 5000;
    hop_limit constant integer := 7;
    -- Keep this batch high by default: evaluating more candidates in one pass helps
    -- pick a stronger corridor early, which usually shrinks the next-iteration search
    -- space much faster than many tiny batches.
    default_candidate_batch constant integer := 512;
    candidate_batch integer;
    candidate_count integer;
    blocked_node_count integer;
    path_row_count integer;
    analysis_row_count integer;
    expected_hops integer;
    remaining_pairs integer;
    iteration_started_at timestamptz;
    stage_started_at timestamptz;
    best_pair record;
    chain_candidate record;
    los_chain h3index[];
    los_chain_existing boolean[];
    los_chain_length integer;
    los_chain_complete boolean;
    chain_index integer;
    anchor_h3 h3index;
    new_h3 h3index;
    new_centroid public.geography;
    promoted_count integer;
    existing_reused_count integer;
    has_existing boolean;
    iteration_number integer;
begin
    perform set_config('statement_timeout', '0', true);

    if promoted is null then
        promoted := 0;
    end if;
    iteration_number := coalesce(iteration_label, 1);
    candidate_batch := coalesce(
        nullif(current_setting('mesh.cluster_slim_candidate_batch', true), '')::integer,
        default_candidate_batch
    );
    if to_regclass('mesh_route_nodes') is null then
        raise notice 'mesh_route_nodes table missing, skipping cluster slimming';
        return;
    end if;

    if not exists (select 1 from mesh_route_nodes) then
        raise notice 'mesh_route_nodes not prepared, skipping cluster slimming';
        return;
    end if;

    if to_regclass('mesh_route_edges') is null then
        raise notice 'mesh_route_edges table missing, skipping cluster slimming';
        return;
    end if;

    if not exists (select 1 from mesh_route_edges) then
        raise notice 'mesh_route_edges not prepared, skipping cluster slimming';
        return;
    end if;

    if to_regclass('mesh_route_cluster_slim_failures') is null then
        raise exception 'mesh_route_cluster_slim_failures table missing; run db/table/mesh_route_cluster_slim_failures first';
    end if;

    -- Full visibility refresh is needed before the first slim pass.
    -- Later iterations already run this refresh at the end when towers are promoted.
    if iteration_number = 1 then
        call mesh_visibility_edges_refresh();
    end if;

    if to_regclass('pg_temp.mesh_route_cluster_slim_candidates') is not null then
        drop table mesh_route_cluster_slim_candidates;
    end if;
    -- Candidate tower pairs pending evaluation.
    create temporary table mesh_route_cluster_slim_candidates (
        pair_id integer primary key,
        source_id integer,
        target_id integer,
        source_h3 h3index,
        target_h3 h3index,
        cluster_hops integer,
        distance_m double precision,
        average_hop_length double precision,
        source_node_id integer,
        target_node_id integer,
        seed_endpoint_count integer
    ) on commit preserve rows;

    if to_regclass('pg_temp.mesh_route_cluster_slim_batch_path') is not null then
        drop table mesh_route_cluster_slim_batch_path;
    end if;
    -- Store every routed corridor cell for the current candidate batch.
    create temporary table mesh_route_cluster_slim_batch_path (
        pair_id integer,
        seq integer,
        h3 h3index,
        centroid_geog public.geography,
        has_tower boolean
    ) on commit preserve rows;

    if to_regclass('pg_temp.mesh_route_cluster_slim_analysis') is not null then
        drop table mesh_route_cluster_slim_analysis;
    end if;
    -- Summaries per pair (hop counts, sharing score, failure reason).
    create temporary table mesh_route_cluster_slim_analysis (
        pair_id integer primary key,
        source_id integer,
        target_id integer,
        source_h3 h3index,
        target_h3 h3index,
        cluster_hops integer,
        distance_m double precision,
        average_hop_length double precision,
        path_nodes integer,
        expected_hops integer,
        new_node_count integer,
        existing_node_count integer,
        shared_score integer,
        seed_endpoint_count integer,
        fail_reason text
    ) on commit preserve rows;

    if to_regclass('pg_temp.mesh_route_cluster_slim_endpoints') is not null then
        drop table mesh_route_cluster_slim_endpoints;
    end if;
    -- Keeps pgRouting endpoints so we never block them.
    create temporary table mesh_route_cluster_slim_endpoints (
        node_id integer primary key
    ) on commit preserve rows;

    if to_regclass('pg_temp.mesh_route_unblocked_edges') is not null then
        drop table mesh_route_unblocked_edges;
    end if;
    -- Cache currently-routable edges once per iteration so pgr_dijkstra does not
    -- rescan and filter mesh_route_edges for every candidate pair.
    create temporary table mesh_route_unblocked_edges (
        id bigint primary key,
        source integer not null,
        target integer not null,
        cost double precision not null,
        reverse_cost double precision not null
    ) on commit preserve rows;

    iteration_started_at := clock_timestamp();

    truncate mesh_route_cluster_slim_candidates;
    truncate mesh_route_cluster_slim_batch_path;
    truncate mesh_route_cluster_slim_analysis;
    truncate mesh_route_cluster_slim_endpoints;

    stage_started_at := clock_timestamp();
    select count(*)
    into remaining_pairs
    from mesh_visibility_edges e
    where e.cluster_hops > hop_limit
      and not exists (
            select 1
            from mesh_route_cluster_slim_failures f
            where f.source_id = e.source_id
              and f.target_id = e.target_id
        );

    if remaining_pairs = 0 then
        raise notice 'Cluster slim iteration % idle: no over-limit pairs remain (%.1f s elapsed)',
            iteration_number,
            extract(epoch from clock_timestamp() - iteration_started_at);
        promoted := 0;
        return;
    end if;

    raise notice 'Cluster slim iteration % scanning % over-limit pair(s)',
        iteration_number,
        remaining_pairs;

    stage_started_at := clock_timestamp();

        with ranked_candidates as (
            -- Rank candidate visibility edges with seed endpoints first to reduce exploration time.
            select
                row_number() over (
                    order by
                        ((case when src.source = 'seed' then 1 else 0 end)
                      + (case when dst.source = 'seed' then 1 else 0 end)) desc,
                        e.distance_m / nullif(e.cluster_hops, 0) asc,
                        e.cluster_hops desc,
                        e.distance_m desc,
                        e.source_id,
                        e.target_id
                ) as pair_id,
                e.*,
                ((case when src.source = 'seed' then 1 else 0 end)
               + (case when dst.source = 'seed' then 1 else 0 end)) as seed_endpoint_count
            from mesh_visibility_edges e
            join mesh_towers src on src.tower_id = e.source_id
            join mesh_towers dst on dst.tower_id = e.target_id
            where e.cluster_hops > hop_limit
              and not exists (
                    select 1
                    from mesh_route_cluster_slim_failures f
                    where f.source_id = e.source_id
                      and f.target_id = e.target_id
                )
        )
        insert into mesh_route_cluster_slim_candidates (
            pair_id,
            source_id,
            target_id,
            source_h3,
            target_h3,
            cluster_hops,
            distance_m,
            average_hop_length,
            source_node_id,
            target_node_id,
            seed_endpoint_count
        )
        select
            rc.pair_id,
            rc.source_id,
            rc.target_id,
            rc.source_h3,
            rc.target_h3,
            rc.cluster_hops,
            rc.distance_m,
            rc.distance_m / nullif(rc.cluster_hops, 0),
            sn.node_id,
            tn.node_id,
            rc.seed_endpoint_count
        from ranked_candidates rc
        join mesh_route_nodes sn on sn.h3 = rc.source_h3
        join mesh_route_nodes tn on tn.h3 = rc.target_h3
        where rc.pair_id <= candidate_batch;

        get diagnostics candidate_count = ROW_COUNT;

        raise notice 'Cluster slim iteration % queued % candidate pair(s) in %.1f s',
            iteration_number,
            candidate_count,
            extract(epoch from clock_timestamp() - stage_started_at);

        insert into mesh_route_cluster_slim_endpoints (node_id)
        select distinct source_node_id from mesh_route_cluster_slim_candidates
        union
        select distinct target_node_id from mesh_route_cluster_slim_candidates;

        if to_regclass('pg_temp.mesh_route_blocked_nodes') is null then
            create temporary table mesh_route_blocked_nodes (
                node_id integer primary key
            ) on commit drop;
        else
            truncate mesh_route_blocked_nodes;
        end if;

        stage_started_at := clock_timestamp();

        -- Lock out nodes that already host towers (besides the endpoints) so the router avoids
        -- planting duplicates right next to existing infrastructure.
        insert into mesh_route_blocked_nodes (node_id)
        select mrn.node_id
        from mesh_route_nodes mrn
        join mesh_surface_h3_r8 surface on surface.h3 = mrn.h3
        where surface.has_tower is not true
          and surface.distance_to_closest_tower is not null
          and surface.distance_to_closest_tower < separation
          and not exists (
                select 1
                from mesh_route_cluster_slim_endpoints ep
                where ep.node_id = mrn.node_id
            );

        get diagnostics blocked_node_count = ROW_COUNT;

        raise notice 'Cluster slim iteration % blocked % nearby node(s) in %.1f s',
            iteration_number,
            blocked_node_count,
            extract(epoch from clock_timestamp() - stage_started_at);

        stage_started_at := clock_timestamp();

        truncate mesh_route_unblocked_edges;

        -- Materialize currently allowed routing edges once; each pgr_dijkstra call
        -- then runs on this reduced graph instead of re-evaluating anti-joins.
        insert into mesh_route_unblocked_edges (id, source, target, cost, reverse_cost)
        select
            e.edge_id as id,
            e.source,
            e.target,
            e.cost,
            e.reverse_cost
        from mesh_route_edges e
        where not exists (
                select 1
                from mesh_route_blocked_nodes blocked
                where blocked.node_id = e.source
            )
          and not exists (
                select 1
                from mesh_route_blocked_nodes blocked
                where blocked.node_id = e.target
            );

        raise notice 'Cluster slim iteration % prepared % unblocked edge(s) in %.1f s',
            iteration_number,
            (select count(*) from mesh_route_unblocked_edges),
            extract(epoch from clock_timestamp() - stage_started_at);

        stage_started_at := clock_timestamp();

        -- Materialize corridor nodes for this candidate batch so later steps can compute sharing
        -- scores and promotion counts without re-running pgRouting multiple times.
        insert into mesh_route_cluster_slim_batch_path (pair_id, seq, h3, centroid_geog, has_tower)
        select
            cp.pair_id,
            path.seq,
            rn.h3,
            surface.centroid_geog,
            surface.has_tower
        from mesh_route_cluster_slim_candidates cp
        cross join lateral (
            select *
            from pgr_dijkstra(
                'select id,
                        source,
                        target,
                        cost,
                        reverse_cost
                 from mesh_route_unblocked_edges',
                cp.source_node_id,
                cp.target_node_id,
                false
            )
        ) path
        join mesh_route_nodes rn on rn.node_id = path.node
        join mesh_surface_h3_r8 surface on surface.h3 = rn.h3
        where path.node <> -1
          and rn.h3 not in (cp.source_h3, cp.target_h3)
        order by cp.pair_id, path.seq;

        get diagnostics path_row_count = ROW_COUNT;

        raise notice 'Cluster slim iteration % routed % intermediate node(s) in %.1f s',
            iteration_number,
            path_row_count,
            extract(epoch from clock_timestamp() - stage_started_at);

        stage_started_at := clock_timestamp();

        -- Summarize each candidate corridor to understand hop count impact and sharing potential.
        insert into mesh_route_cluster_slim_analysis (
            pair_id,
            source_id,
            target_id,
            source_h3,
            target_h3,
            cluster_hops,
            distance_m,
            average_hop_length,
            path_nodes,
            expected_hops,
            new_node_count,
            existing_node_count,
            shared_score,
            seed_endpoint_count
        )
        with path_counts as (
            select
                pair_id,
                count(*) as path_nodes,
                count(*) filter (where has_tower is false) as new_node_count,
                count(*) filter (where has_tower is true) as existing_node_count
            from mesh_route_cluster_slim_batch_path
            group by pair_id
        )
        select
            cp.pair_id,
            cp.source_id,
            cp.target_id,
            cp.source_h3,
            cp.target_h3,
            cp.cluster_hops,
            cp.distance_m,
            cp.average_hop_length,
            pc.path_nodes,
            case when pc.path_nodes is not null then pc.path_nodes + 1 end as expected_hops,
            pc.new_node_count,
            pc.existing_node_count,
            0,
            cp.seed_endpoint_count
        from mesh_route_cluster_slim_candidates cp
        left join path_counts pc on pc.pair_id = cp.pair_id
        ;

        get diagnostics analysis_row_count = ROW_COUNT;

        raise notice 'Cluster slim iteration % analyzed % corridor candidate(s) in %.1f s',
            iteration_number,
            analysis_row_count,
            extract(epoch from clock_timestamp() - stage_started_at);

        update mesh_route_cluster_slim_analysis
        set fail_reason = 'no routing corridor available'
        where coalesce(path_nodes, 0) = 0;

        update mesh_route_cluster_slim_analysis msa
        set fail_reason = format('corridor still needs %s hops (current %s)', msa.expected_hops, msa.cluster_hops)
        where msa.fail_reason is null
          and msa.expected_hops >= msa.cluster_hops;

        update mesh_route_cluster_slim_analysis msa
        set fail_reason = format('corridor still needs %s hops which exceeds hop limit %s', msa.expected_hops, hop_limit)
        where msa.fail_reason is null
          and msa.expected_hops > hop_limit;

        update mesh_route_cluster_slim_analysis msa
        set fail_reason = 'corridor already satisfied by existing towers'
        where msa.fail_reason is null
          and coalesce(msa.new_node_count, 0) = 0;

        insert into mesh_route_cluster_slim_failures (source_id, target_id, status, reason, last_attempt_at)
        select source_id, target_id, 'failed', fail_reason, clock_timestamp()
        from mesh_route_cluster_slim_analysis
        where fail_reason is not null
        on conflict (source_id, target_id) do update
        set status = excluded.status,
            reason = excluded.reason,
            last_attempt_at = excluded.last_attempt_at,
            attempt_count = mesh_route_cluster_slim_failures.attempt_count + 1;

        -- Recompute sharing scores while ignoring corridors that already failed validation and discarding peers of equal length.
        with viable_pairs as (
            -- Only corridors that produced at least one new node and passed validation can contribute to sharing preference.
            select
                pair_id,
                path_nodes
            from mesh_route_cluster_slim_analysis
            where fail_reason is null
              and coalesce(path_nodes, 0) > 0
        ),
        shared as (
            -- Count how many longer viable corridors reuse each candidate node so we prioritize paths that unlock bigger queues.
            select
                shared_paths.base_pair_id as pair_id,
                count(*) as shared_score
            from (
                select distinct
                    base_path.pair_id as base_pair_id,
                    other_path.pair_id as other_pair_id,
                    base_path.h3
                from mesh_route_cluster_slim_batch_path base_path
                join mesh_route_cluster_slim_batch_path other_path
                    on other_path.h3 = base_path.h3
                   and base_path.has_tower is false
                   and other_path.has_tower is false
                join viable_pairs base_stats on base_stats.pair_id = base_path.pair_id
                join viable_pairs other_stats on other_stats.pair_id = other_path.pair_id
                where other_path.pair_id <> base_path.pair_id
                  and other_stats.path_nodes > base_stats.path_nodes
            ) shared_paths
            group by shared_paths.base_pair_id
        )
        update mesh_route_cluster_slim_analysis msa
        set shared_score = shared.shared_score
        from shared
        where msa.pair_id = shared.pair_id;

        update mesh_route_cluster_slim_analysis
        set shared_score = 0
        where shared_score is null;

        select *
        into best_pair
        from mesh_route_cluster_slim_analysis msa
        where msa.fail_reason is null
        order by
            msa.seed_endpoint_count desc,
            msa.shared_score desc,
            (msa.cluster_hops - msa.expected_hops) desc,
            msa.average_hop_length asc
        limit 1;

        if best_pair.pair_id is null then
            raise notice 'Cluster slim ran out of viable corridors after %.1f s',
                extract(epoch from clock_timestamp() - iteration_started_at);
            promoted := 0;
            return;
        end if;

        los_chain := array[]::h3index[];
        los_chain_existing := array[]::boolean[];
        los_chain_length := 0;
        los_chain_complete := false;
        anchor_h3 := best_pair.source_h3;

        for chain_candidate in
            select h3, has_tower
            from mesh_route_cluster_slim_batch_path
            where pair_id = best_pair.pair_id
            order by seq
        loop
            if not h3_los_between_cells(anchor_h3, chain_candidate.h3) then
                insert into mesh_route_cluster_slim_failures (source_id, target_id, status, reason, last_attempt_at)
                values (
                    best_pair.source_id,
                    best_pair.target_id,
                    'failed',
                    format('no los between %s and %s', anchor_h3::text, chain_candidate.h3::text),
                    clock_timestamp()
                )
                on conflict (source_id, target_id) do update
                set status = excluded.status,
                    reason = excluded.reason,
                    last_attempt_at = excluded.last_attempt_at,
                    attempt_count = mesh_route_cluster_slim_failures.attempt_count + 1;

            raise notice 'Cluster slim skipping edge % -> % because hop % lacks LOS to previous node',
                best_pair.source_id,
                best_pair.target_id,
                chain_candidate.h3;

            promoted := 0;
            return;
            end if;

            los_chain := array_append(los_chain, chain_candidate.h3);
            los_chain_existing := array_append(los_chain_existing, chain_candidate.has_tower);
            los_chain_length := los_chain_length + 1;
            anchor_h3 := chain_candidate.h3;

            if h3_los_between_cells(anchor_h3, best_pair.target_h3) then
                los_chain_complete := true;
                exit;
            end if;
        end loop;

        if not los_chain_complete then
            insert into mesh_route_cluster_slim_failures (source_id, target_id, status, reason, last_attempt_at)
            values (best_pair.source_id, best_pair.target_id, 'failed', 'no los corridor along routed path', clock_timestamp())
            on conflict (source_id, target_id) do update
            set status = excluded.status,
                reason = excluded.reason,
                last_attempt_at = excluded.last_attempt_at,
                attempt_count = mesh_route_cluster_slim_failures.attempt_count + 1;

            raise notice 'Cluster slim skipping edge % -> % because routed path lacks continuous LOS to target',
                best_pair.source_id,
                best_pair.target_id;

            promoted := 0;
            return;
        end if;

        expected_hops := best_pair.expected_hops;
        los_chain_length := coalesce(array_length(los_chain, 1), 0);
        promoted_count := 0;
        existing_reused_count := 0;

        raise notice 'Cluster slim iteration % picked edge % -> % (%.1f km) % -> % hops, % seed endpoint(s), shared score %',
            iteration_number,
            best_pair.source_id,
            best_pair.target_id,
            best_pair.distance_m / 1000.0,
            best_pair.cluster_hops,
            expected_hops,
            best_pair.seed_endpoint_count,
            best_pair.shared_score;

        for chain_index in 1..los_chain_length loop
            new_h3 := los_chain[chain_index];
            has_existing := los_chain_existing[chain_index];

            if has_existing then
                existing_reused_count := existing_reused_count + 1;
                continue;
            end if;

            insert into mesh_towers (h3, source)
            values (new_h3, 'cluster_slim')
            on conflict (h3) do nothing;

            if not found then
                continue;
            end if;

            promoted_count := promoted_count + 1;

            select centroid_geog
            into new_centroid
            from mesh_surface_h3_r8
            where h3 = new_h3;

            update mesh_surface_h3_r8
            set has_tower = true,
                clearance = null,
                path_loss = null,
                visible_uncovered_population = 0,
                distance_to_closest_tower = 0
            where h3 = new_h3;

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

            perform mesh_surface_refresh_visible_tower_counts(
                new_h3,
                refresh_radius,
                max_distance
            );

            perform mesh_surface_refresh_reception_metrics(
                new_h3,
                refresh_radius,
                max_distance
            );
        end loop;

        if promoted_count > 0 then
            stage_started_at := clock_timestamp();
            call mesh_visibility_edges_refresh();
            raise notice 'Cluster slim iteration % refreshed visibility diagnostics in %.1f s after installing % new tower(s)',
                iteration_number,
                extract(epoch from clock_timestamp() - stage_started_at),
                promoted_count;
        else
            raise notice 'Cluster slim iteration % skipped visibility refresh because corridor reused existing towers',
                iteration_number;
        end if;

        insert into mesh_route_cluster_slim_failures (source_id, target_id, status, reason, last_attempt_at)
        values (best_pair.source_id, best_pair.target_id, 'completed', 'completed', clock_timestamp())
        on conflict (source_id, target_id) do update
        set status = excluded.status,
            reason = excluded.reason,
            last_attempt_at = excluded.last_attempt_at,
            attempt_count = mesh_route_cluster_slim_failures.attempt_count + 1;

        raise notice 'Slimmed cluster edge % -> %: % -> % hops using % new / % existing tower(s) (%.1f km) in %.1f s total',
            best_pair.source_id,
            best_pair.target_id,
            best_pair.cluster_hops,
            expected_hops,
            promoted_count,
            existing_reused_count,
            best_pair.distance_m / 1000.0,
            extract(epoch from clock_timestamp() - iteration_started_at);
        promoted := promoted_count;
        return;

end;
$$;
