set client_min_messages = notice;

drop procedure if exists mesh_route_cluster_slim();
drop procedure if exists mesh_route_cluster_slim(integer);
drop procedure if exists mesh_route_cluster_slim(integer, integer);
drop procedure if exists mesh_route_cluster_slim_prepare_iteration(integer);

-- Prepare the shared ranked candidate queue for one cluster-slim iteration.
create or replace procedure mesh_route_cluster_slim_prepare_iteration(p_iteration_label integer)
    language plpgsql
as
$$
declare
    hop_limit constant integer := 7;
    iteration_number integer;
    queued_count integer;
    remaining_pairs integer;
begin
    iteration_number := coalesce(p_iteration_label, 1);

    if to_regclass('mesh_route_cluster_slim_candidate_queue') is null then
        raise exception 'mesh_route_cluster_slim_candidate_queue table missing; run db/table/mesh_route_cluster_slim_candidate_queue first';
    end if;

    if to_regclass('mesh_route_cluster_slim_claims') is null then
        raise exception 'mesh_route_cluster_slim_claims table missing; run db/table/mesh_route_cluster_slim_claims first';
    end if;

    if to_regclass('mesh_route_cluster_slim_failures') is null then
        raise exception 'mesh_route_cluster_slim_failures table missing; run db/table/mesh_route_cluster_slim_failures first';
    end if;

    delete from mesh_route_cluster_slim_claims
    where mesh_route_cluster_slim_claims.iteration_label = iteration_number;

    delete from mesh_route_cluster_slim_candidate_queue
    where mesh_route_cluster_slim_candidate_queue.iteration_label = iteration_number;

    drop table if exists pg_temp.mesh_route_country_polygons;
    -- Build local country polygons so same-country repairs stay ahead of fallback cross-border repairs.
    create temporary table mesh_route_country_polygons as
    with admin_polygons as (
        -- Normalize only the countries this rollout currently handles.
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

    create index mesh_route_country_polygons_geom_idx
        on mesh_route_country_polygons using gist (geom);
    analyze mesh_route_country_polygons;

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

    with ranked_candidates as (
        -- Rank candidate visibility edges once, prefer same-country hop shortening before cross-border hop shortening,
        -- then let workers drain this shared queue.
        select
            row_number() over (
                order by
                    case
                        when src_country.country_code is not null
                         and src_country.country_code = dst_country.country_code then 0
                        when src_country.country_code is null
                          or dst_country.country_code is null then 1
                        else 2
                    end asc,
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
        left join mesh_route_country_polygons src_country
          on ST_Intersects(src.h3::geometry, src_country.geom)
        left join mesh_route_country_polygons dst_country
          on ST_Intersects(dst.h3::geometry, dst_country.geom)
        where e.cluster_hops > hop_limit
          and not exists (
                select 1
                from mesh_route_cluster_slim_failures f
                where f.source_id = e.source_id
                  and f.target_id = e.target_id
            )
    )
    insert into mesh_route_cluster_slim_candidate_queue (
        iteration_label,
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
        iteration_number,
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
    on conflict (iteration_label, source_id, target_id) do nothing;

    get diagnostics queued_count = ROW_COUNT;

    raise notice 'Cluster slim iteration % queued % of % over-limit pair(s)',
        iteration_number,
        queued_count,
        remaining_pairs;
end;
$$;

-- Each invocation drains the shared queue until it promotes one corridor or runs out of safe candidates.
create or replace procedure mesh_route_cluster_slim(p_iteration_label integer, inout promoted integer)
    language plpgsql
as
$$
declare
    refresh_radius constant double precision := 100000;
    separation constant double precision := 0;
    claim_resolution integer := 4;
    claim_disk integer := 1;
    p_worker_count integer;
    p_worker_index integer;
    blocked_node_count integer;
    path_row_count integer;
    expected_hops integer;
    iteration_started_at timestamptz;
    stage_started_at timestamptz;
    candidate_pair record;
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
    needed_claim_count integer;
    inserted_claim_count integer;
begin
    perform set_config('statement_timeout', '0', true);

    if promoted is null then
        promoted := 0;
    end if;

    iteration_number := coalesce(p_iteration_label, 1);
    p_worker_count := greatest(
        coalesce(
            nullif(current_setting('mesh.cluster_slim_worker_count', true), '')::integer,
            1
        ),
        1
    );
    p_worker_index := coalesce(
        nullif(current_setting('mesh.cluster_slim_worker_index', true), '')::integer,
        0
    );

    if p_worker_index < 0 or p_worker_index >= p_worker_count then
        raise exception 'Cluster slim worker index % is outside worker count %',
            p_worker_index,
            p_worker_count;
    end if;

    if to_regclass('mesh_route_nodes') is null or not exists (select 1 from mesh_route_nodes) then
        raise notice 'mesh_route_nodes not prepared, skipping cluster slimming';
        return;
    end if;

    if to_regclass('mesh_route_edges') is null or not exists (select 1 from mesh_route_edges) then
        raise notice 'mesh_route_edges not prepared, skipping cluster slimming';
        return;
    end if;

    if to_regclass('mesh_route_cluster_slim_candidate_queue') is null then
        raise exception 'mesh_route_cluster_slim_candidate_queue table missing; run db/table/mesh_route_cluster_slim_candidate_queue first';
    end if;

    if to_regclass('mesh_route_cluster_slim_claims') is null then
        raise exception 'mesh_route_cluster_slim_claims table missing; run db/table/mesh_route_cluster_slim_claims first';
    end if;

    if to_regclass('mesh_route_cluster_slim_failures') is null then
        raise exception 'mesh_route_cluster_slim_failures table missing; run db/table/mesh_route_cluster_slim_failures first';
    end if;

    select coalesce(
        (
            select value::integer
            from mesh_pipeline_settings
            where setting = 'cluster_slim_claim_resolution'
        ),
        claim_resolution
    )
    into claim_resolution;

    select coalesce(
        (
            select value::integer
            from mesh_pipeline_settings
            where setting = 'cluster_slim_claim_disk'
        ),
        claim_disk
    )
    into claim_disk;

    drop table if exists pg_temp.mesh_route_cluster_slim_batch_path;
    -- Store every routed corridor cell for the currently claimed candidate.
    create temporary table mesh_route_cluster_slim_batch_path (
        pair_id integer,
        seq integer,
        h3 h3index,
        centroid_geog public.geography,
        has_tower boolean
    ) on commit preserve rows;

    drop table if exists pg_temp.mesh_route_cluster_slim_analysis;
    -- Store the current candidate summary so validation and promotion stay copyable.
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
        building_node_count integer,
        shared_score integer,
        seed_endpoint_count integer,
        fail_reason text
    ) on commit preserve rows;

    drop table if exists pg_temp.mesh_route_cluster_slim_endpoints;
    -- Keep all queued pgRouting endpoints open while building the reduced graph.
    create temporary table mesh_route_cluster_slim_endpoints (
        node_id integer primary key
    ) on commit preserve rows;

    drop table if exists pg_temp.mesh_route_unblocked_edges;
    -- Cache currently routable edges once per worker; every candidate reuses this pgRouting edge SQL.
    create temporary table mesh_route_unblocked_edges (
        id bigint primary key,
        source integer not null,
        target integer not null,
        cost double precision not null,
        reverse_cost double precision not null
    ) on commit preserve rows;

    drop table if exists pg_temp.mesh_route_cluster_slim_claim_needed;
    -- Hold the approximate or exact claim set for one candidate before inserting shared locks.
    create temporary table mesh_route_cluster_slim_claim_needed (
        claim_h3 h3index primary key
    ) on commit preserve rows;

    iteration_started_at := clock_timestamp();

    insert into mesh_route_cluster_slim_endpoints (node_id)
    select distinct source_node_id
    from mesh_route_cluster_slim_candidate_queue
    where iteration_label = iteration_number
    union
    select distinct target_node_id
    from mesh_route_cluster_slim_candidate_queue
    where iteration_label = iteration_number;

    drop table if exists pg_temp.mesh_route_blocked_nodes;
    -- Block only non-endpoint route nodes that violate the configured tower spacing.
    create temporary table mesh_route_blocked_nodes (
        node_id integer primary key
    ) on commit drop;

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

    raise notice 'Cluster slim iteration % worker %/% blocked % node(s) and prepared % unblocked edge(s)',
        iteration_number,
        p_worker_index + 1,
        p_worker_count,
        blocked_node_count,
        (select count(*) from mesh_route_unblocked_edges);

    <<candidate_loop>>
    loop
        with next_candidate as (
            -- Claim one queued pair transactionally so other workers skip it.
            select q.iteration_label, q.source_id, q.target_id
            from mesh_route_cluster_slim_candidate_queue q
            where q.iteration_label = iteration_number
              and q.status = 'queued'
            order by q.pair_id
            for update skip locked
            limit 1
        ),
        claimed as (
            update mesh_route_cluster_slim_candidate_queue q
            set status = 'routing',
                worker_index = p_worker_index,
                claimed_at = clock_timestamp(),
                attempted_at = clock_timestamp(),
                reason = null
            from next_candidate nc
            where q.iteration_label = nc.iteration_label
              and q.source_id = nc.source_id
              and q.target_id = nc.target_id
            returning q.*
        )
        select *
        into candidate_pair
        from claimed;

        if not found then
            promoted := 0;
            raise notice 'Cluster slim iteration % worker %/% has no more non-conflicting queued candidate(s) after %.1f s',
                iteration_number,
                p_worker_index + 1,
                p_worker_count,
                extract(epoch from clock_timestamp() - iteration_started_at);
            return;
        end if;

        truncate mesh_route_cluster_slim_claim_needed;

        insert into mesh_route_cluster_slim_claim_needed (claim_h3)
        select distinct disk.claim_h3
        from h3_grid_path_cells(
            h3_cell_to_parent(candidate_pair.source_h3, claim_resolution),
            h3_cell_to_parent(candidate_pair.target_h3, claim_resolution)
        ) as path(path_h3)
        cross join lateral h3_grid_disk(path.path_h3, claim_disk) as disk(claim_h3);

        select count(*)
        into needed_claim_count
        from mesh_route_cluster_slim_claim_needed;

        begin
            -- Keep only the short shared-claim mutation serialized.
            -- Expensive pgRouting stays parallel after this block releases.
            perform pg_advisory_lock(hashtext('mesh_route_cluster_slim_claims_write'));

            with inserted as (
                insert into mesh_route_cluster_slim_claims (
                    iteration_label,
                    claim_h3,
                    source_id,
                    target_id,
                    worker_index,
                    claim_stage
                )
                select
                    iteration_number,
                    needed.claim_h3,
                    candidate_pair.source_id,
                    candidate_pair.target_id,
                    p_worker_index,
                    'approx'
                from mesh_route_cluster_slim_claim_needed needed
                -- Parallel workers can overlap on many coarse buckets.
                -- Insert claims in deterministic index order before ON CONFLICT.
                order by needed.claim_h3
                on conflict (iteration_label, claim_h3) do nothing
                returning 1
            )
            select count(*)
            into inserted_claim_count
            from inserted;

            if inserted_claim_count <> needed_claim_count then
                delete from mesh_route_cluster_slim_claims claims
                where claims.iteration_label = iteration_number
                  and claims.source_id = candidate_pair.source_id
                  and claims.target_id = candidate_pair.target_id;
            end if;

            perform pg_advisory_unlock(hashtext('mesh_route_cluster_slim_claims_write'));
        exception when others then
            perform pg_advisory_unlock(hashtext('mesh_route_cluster_slim_claims_write'));
            raise;
        end;

        if inserted_claim_count <> needed_claim_count then
            update mesh_route_cluster_slim_candidate_queue q
            set status = 'claim_conflict',
                reason = format('approx claim conflict: inserted %s of %s r%s bucket(s)', inserted_claim_count, needed_claim_count, claim_resolution)
            where q.iteration_label = iteration_number
              and q.source_id = candidate_pair.source_id
              and q.target_id = candidate_pair.target_id;

            continue;
        end if;

        truncate mesh_route_cluster_slim_batch_path;
        truncate mesh_route_cluster_slim_analysis;

        stage_started_at := clock_timestamp();

        insert into mesh_route_cluster_slim_batch_path (pair_id, seq, h3, centroid_geog, has_tower)
        select
            candidate_pair.pair_id,
            path.seq,
            rn.h3,
            surface.centroid_geog,
            surface.has_tower
        from pgr_dijkstra(
            'select id,
                    source,
                    target,
                    cost,
                    reverse_cost
             from mesh_route_unblocked_edges',
            candidate_pair.source_node_id,
            candidate_pair.target_node_id,
            false
        ) path
        join mesh_route_nodes rn on rn.node_id = path.node
        join mesh_surface_h3_r8 surface on surface.h3 = rn.h3
        where path.node <> -1
          and rn.h3 not in (candidate_pair.source_h3, candidate_pair.target_h3)
        order by path.seq;

        get diagnostics path_row_count = ROW_COUNT;

        raise notice 'Cluster slim iteration % worker %/% routed pair % -> % through % intermediate node(s) in %.1f s',
            iteration_number,
            p_worker_index + 1,
            p_worker_count,
            candidate_pair.source_id,
            candidate_pair.target_id,
            path_row_count,
            extract(epoch from clock_timestamp() - stage_started_at);

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
            building_node_count,
            shared_score,
            seed_endpoint_count
        )
        with path_counts as (
            -- Summarize the routed corridor once; later validation only reads this row and path table.
            select
                pair_id,
                count(*) as path_nodes,
                count(*) filter (where mesh_route_cluster_slim_batch_path.has_tower is false) as new_node_count,
                count(*) filter (where mesh_route_cluster_slim_batch_path.has_tower is true) as existing_node_count,
                count(*) filter (where mesh_route_cluster_slim_batch_path.has_tower is false and surface.has_building) as building_node_count
            from mesh_route_cluster_slim_batch_path
            join mesh_surface_h3_r8 surface using (h3)
            group by pair_id
        )
        select
            candidate_pair.pair_id,
            candidate_pair.source_id,
            candidate_pair.target_id,
            candidate_pair.source_h3,
            candidate_pair.target_h3,
            candidate_pair.cluster_hops,
            candidate_pair.distance_m,
            candidate_pair.average_hop_length,
            pc.path_nodes,
            case when pc.path_nodes is not null then pc.path_nodes + 1 end as expected_hops,
            pc.new_node_count,
            pc.existing_node_count,
            pc.building_node_count,
            0,
            candidate_pair.seed_endpoint_count
        from path_counts pc
        right join (select 1) keep_row on true;

        update mesh_route_cluster_slim_analysis
        set fail_reason = 'no routing corridor available'
        where coalesce(path_nodes, 0) = 0;

        update mesh_route_cluster_slim_analysis msa
        set fail_reason = format('corridor still needs %s hops (current %s)', msa.expected_hops, msa.cluster_hops)
        where msa.fail_reason is null
          and msa.expected_hops >= msa.cluster_hops;

        update mesh_route_cluster_slim_analysis msa
        set fail_reason = format('corridor still needs %s hops which exceeds hop limit %s', msa.expected_hops, 7)
        where msa.fail_reason is null
          and msa.expected_hops > 7;

        update mesh_route_cluster_slim_analysis msa
        set fail_reason = 'corridor already satisfied by existing towers'
        where msa.fail_reason is null
          and coalesce(msa.new_node_count, 0) = 0;

        select *
        into best_pair
        from mesh_route_cluster_slim_analysis
        where pair_id = candidate_pair.pair_id;

        if best_pair.fail_reason is not null then
            insert into mesh_route_cluster_slim_failures (source_id, target_id, status, reason, last_attempt_at)
            values (best_pair.source_id, best_pair.target_id, 'failed', best_pair.fail_reason, clock_timestamp())
            on conflict (source_id, target_id) do update
            set status = excluded.status,
                reason = excluded.reason,
                last_attempt_at = excluded.last_attempt_at,
                attempt_count = mesh_route_cluster_slim_failures.attempt_count + 1;

            update mesh_route_cluster_slim_candidate_queue q
            set status = 'failed',
                reason = best_pair.fail_reason
            where q.iteration_label = iteration_number
              and q.source_id = best_pair.source_id
              and q.target_id = best_pair.target_id;

            delete from mesh_route_cluster_slim_claims claims
            where claims.iteration_label = iteration_number
              and claims.source_id = best_pair.source_id
              and claims.target_id = best_pair.target_id;

            continue;
        end if;

        truncate mesh_route_cluster_slim_claim_needed;

        insert into mesh_route_cluster_slim_claim_needed (claim_h3)
        select distinct disk.claim_h3
        from mesh_route_cluster_slim_batch_path path
        cross join lateral h3_grid_disk(h3_cell_to_parent(path.h3, claim_resolution), claim_disk) as disk(claim_h3)
        where path.pair_id = best_pair.pair_id;

        select count(*)
        into needed_claim_count
        from mesh_route_cluster_slim_claim_needed needed
        where not exists (
            select 1
            from mesh_route_cluster_slim_claims claims
            where claims.iteration_label = iteration_number
              and claims.claim_h3 = needed.claim_h3
              and claims.source_id = best_pair.source_id
              and claims.target_id = best_pair.target_id
        );

        begin
            -- Exact path claims touch the same shared unique H3 bucket key as
            -- approximate claims, so this short write section is serialized too.
            perform pg_advisory_lock(hashtext('mesh_route_cluster_slim_claims_write'));

            with inserted as (
                insert into mesh_route_cluster_slim_claims (
                    iteration_label,
                    claim_h3,
                    source_id,
                    target_id,
                    worker_index,
                    claim_stage
                )
                select
                    iteration_number,
                    needed.claim_h3,
                    best_pair.source_id,
                    best_pair.target_id,
                    p_worker_index,
                    'exact'
                from mesh_route_cluster_slim_claim_needed needed
                where not exists (
                    select 1
                    from mesh_route_cluster_slim_claims own_claim
                    where own_claim.iteration_label = iteration_number
                      and own_claim.claim_h3 = needed.claim_h3
                      and own_claim.source_id = best_pair.source_id
                      and own_claim.target_id = best_pair.target_id
                )
                -- Keep exact claims in the same unique-index order as approximate claims.
                order by needed.claim_h3
                on conflict (iteration_label, claim_h3) do nothing
                returning 1
            )
            select count(*)
            into inserted_claim_count
            from inserted;

            if inserted_claim_count <> needed_claim_count then
                delete from mesh_route_cluster_slim_claims claims
                where claims.iteration_label = iteration_number
                  and claims.source_id = best_pair.source_id
                  and claims.target_id = best_pair.target_id;
            end if;

            perform pg_advisory_unlock(hashtext('mesh_route_cluster_slim_claims_write'));
        exception when others then
            perform pg_advisory_unlock(hashtext('mesh_route_cluster_slim_claims_write'));
            raise;
        end;

        if inserted_claim_count <> needed_claim_count then
            update mesh_route_cluster_slim_candidate_queue q
            set status = 'exact_claim_conflict',
                reason = format('exact claim conflict: inserted %s of %s extra r%s bucket(s)', inserted_claim_count, needed_claim_count, claim_resolution)
            where q.iteration_label = iteration_number
              and q.source_id = best_pair.source_id
              and q.target_id = best_pair.target_id;

            continue;
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

                update mesh_route_cluster_slim_candidate_queue q
                set status = 'failed',
                    reason = format('no los between %s and %s', anchor_h3::text, chain_candidate.h3::text)
                where q.iteration_label = iteration_number
                  and q.source_id = best_pair.source_id
                  and q.target_id = best_pair.target_id;

                delete from mesh_route_cluster_slim_claims claims
                where claims.iteration_label = iteration_number
                  and claims.source_id = best_pair.source_id
                  and claims.target_id = best_pair.target_id;

                continue candidate_loop;
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

            update mesh_route_cluster_slim_candidate_queue q
            set status = 'failed',
                reason = 'no los corridor along routed path'
            where q.iteration_label = iteration_number
              and q.source_id = best_pair.source_id
              and q.target_id = best_pair.target_id;

            delete from mesh_route_cluster_slim_claims claims
            where claims.iteration_label = iteration_number
              and claims.source_id = best_pair.source_id
              and claims.target_id = best_pair.target_id;

            continue;
        end if;

        expected_hops := best_pair.expected_hops;
        los_chain_length := coalesce(array_length(los_chain, 1), 0);
        promoted_count := 0;
        existing_reused_count := 0;

        raise notice 'Cluster slim iteration % picked edge % -> % (%.1f km) % -> % hops, % seed endpoint(s)',
            iteration_number,
            best_pair.source_id,
            best_pair.target_id,
            best_pair.distance_m / 1000.0,
            best_pair.cluster_hops,
            expected_hops,
            best_pair.seed_endpoint_count;

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

            if p_worker_count = 1 then
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
            end if;
        end loop;

        insert into mesh_route_cluster_slim_failures (source_id, target_id, status, reason, last_attempt_at)
        values (best_pair.source_id, best_pair.target_id, 'completed', 'completed', clock_timestamp())
        on conflict (source_id, target_id) do update
        set status = excluded.status,
            reason = excluded.reason,
            last_attempt_at = excluded.last_attempt_at,
            attempt_count = mesh_route_cluster_slim_failures.attempt_count + 1;

        update mesh_route_cluster_slim_candidate_queue q
        set status = 'completed',
            reason = 'completed'
        where q.iteration_label = iteration_number
          and q.source_id = best_pair.source_id
          and q.target_id = best_pair.target_id;

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
    end loop candidate_loop;
end;
$$;
