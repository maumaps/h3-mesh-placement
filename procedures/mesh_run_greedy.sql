-- Greedy iteration terminology:
-- * bridge mode: prioritize connecting tower clusters to each other
-- * greedy mode: fall back to maximizing uncovered population
-- * visible_tower_count: count of individual towers with LOS within 70 km
-- * visible_cluster_count: count of unique tower clusters reachable from a candidate
-- * max_distance: any LOS/(re)calculation stays within 70 km by design
-- * separation: minimum 5 km spacing between towers
-- * recalc_reception: radius for recomputing clearance/path loss around placements
-- * recalc_population: radius for recomputing uncovered population (same as talk)
-- All distances are meters and rely on geography SRIDs with ST_DWithin.
-- Constants for this single greedy iteration
\set max_distance 70000
\set separation 5000
\set recalc_reception 70000
\set recalc_population 70000

set client_min_messages = notice;

--begin;

do $$ begin
    raise notice 'Refreshing reception metrics';
end $$;
with reception_targets as (
    select
        s1.h3,
        s1.centroid_geog
    from mesh_surface_h3_r8 s1
    where s1.has_tower is not true
      and s1.clearance is null
      and s1.distance_to_closest_tower < 70000
),
-- Pair each reception target with nearby towers
reception_candidate_towers as (
    select
        rt.h3 as cell_h3,
        t.h3 as tower_h3,
        t.centroid_geog
    from reception_targets rt
    join lateral (
        select t.h3, t.centroid_geog
        from mesh_towers t
        where t.h3 <> rt.h3
          and ST_DWithin(rt.centroid_geog, t.centroid_geog, 70000)
    ) t on true
),
-- Evaluate visibility exactly once for every (cell, tower) pair
candidate_visibility as (
    select
        rct.cell_h3,
        rct.tower_h3,
        m.clearance,
        m.path_loss_db
    from reception_candidate_towers rct
    cross join lateral h3_visibility_metrics(
        rct.cell_h3,
        rct.tower_h3,
        28,
        28,
        868e6
    ) as m(clearance, path_loss_db)
),
-- Count unique visible towers per cell (a tower counts at most once)
visible_counts as (
    select
        cell_h3,
        count(*) as visible_tower_count
    from (
        select distinct cell_h3, tower_h3
        from candidate_visibility
        where clearance > 0
    ) d
    group by cell_h3
),
-- pick the best tower per cell (lowest path_loss), then attach the unique-count
reception_metrics as (
    select
        b.cell_h3,
        b.clearance,
        b.path_loss_db,
        coalesce(vc.visible_tower_count, 0) as visible_tower_count
    from (
        select distinct on (cell_h3)
            cell_h3,
            clearance,
            path_loss_db
        from candidate_visibility
        order by cell_h3, path_loss_db asc
    ) b
    left join visible_counts vc using (cell_h3)
)
update mesh_surface_h3_r8 s
set clearance = q.clearance,
    path_loss = q.path_loss_db,
    visible_tower_count = q.visible_tower_count
from reception_metrics q
where s.h3 = q.cell_h3;


do
$$
declare
    separation constant double precision := 5000;
    recalc_radius constant double precision := 70000;
    max_distance constant double precision := 70000;
    previous_iteration integer := coalesce((select max(iteration) from mesh_greedy_iterations), 0);
    next_iteration integer := previous_iteration + 1;
    candidate record;
    recompute record;
    recompute_visible numeric;
    best_visible_uncovered numeric;
begin
    with
    -- Identify current tower clusters to know which components need bridging
    tower_clusters as (
        select * from mesh_tower_clusters()
    ),
    -- Count clusters so we only run bridge logic once multiple groups exist
    cluster_stats as (
        select count(distinct cluster_id) as cluster_count
        from tower_clusters
    ),
    -- List distinct cluster ids for lateral scans
    cluster_list as (
        select distinct cluster_id
        from tower_clusters
    ),
    -- Candidate surface cells that satisfy spacing/visibility constraints
    candidate_cells as (
        select
            s.h3,
            s.centroid_geog,
            s.visible_uncovered_population
        from mesh_surface_h3_r8 s
        join cluster_stats stats on stats.cluster_count > 1
        where s.can_place_tower
          and coalesce(s.distance_to_closest_tower >= s.min_distance_to_closest_tower, false)
          and s.distance_to_closest_tower < max_distance
          and s.visible_tower_count >= 2
    ),
    -- Collect all towers per cluster within 70 km, or the closest one if none are in range
    candidate_cluster_towers as (
        select
            c.h3 as candidate_h3,
            cl.cluster_id,
            t_in.h3 as tower_h3,
            t_in.centroid_geog as tower_geog,
            ST_Distance(c.centroid_geog, t_in.centroid_geog) as tower_distance
        from candidate_cells c
        join cluster_list cl on true
        join lateral (
            select t.*
            from mesh_towers t
            join tower_clusters tc on tc.tower_id = t.tower_id
            where tc.cluster_id = cl.cluster_id
              and t.h3 <> c.h3
              and ST_DWithin(c.centroid_geog, t.centroid_geog, max_distance)
        ) t_in on true

        union

        select
            c.h3 as candidate_h3,
            cl.cluster_id,
            fallback.tower_h3,
            fallback.tower_geog,
            fallback.tower_distance
        from candidate_cells c
        join cluster_list cl on true
        join lateral (
            select
                t.h3 as tower_h3,
                t.centroid_geog as tower_geog,
                ST_Distance(c.centroid_geog, t.centroid_geog) as tower_distance
            from mesh_towers t
            join tower_clusters tc on tc.tower_id = t.tower_id
            where tc.cluster_id = cl.cluster_id
              and t.h3 <> c.h3
            order by c.centroid_geog <-> t.centroid_geog
            limit 1
        ) fallback on true
    ),
    -- Score each candidate/cluster pair using LOS metrics from the collected towers
    candidate_cluster_metrics as (
        select
            cct.candidate_h3,
            cct.cluster_id,
            metrics.path_loss_db,
            (metrics.clearance > 0) as has_visible_link,
            cct.tower_distance as distance_to_cluster
        from candidate_cluster_towers cct
        cross join lateral h3_visibility_metrics(
            cct.candidate_h3,
            cct.tower_h3,
            28,
            28,
            868e6
        ) as metrics(clearance, path_loss_db)
    ),
    -- Keep only the best (lowest loss) tower per cluster per candidate
    candidate_cluster_scores as (
        select distinct on (ccm.candidate_h3, ccm.cluster_id)
            ccm.candidate_h3,
            ccm.cluster_id,
            ccm.path_loss_db as min_path_loss,
            ccm.has_visible_link,
            ccm.distance_to_cluster
        from candidate_cluster_metrics ccm
        order by ccm.candidate_h3, ccm.cluster_id, ccm.path_loss_db asc
    ),
    -- Aggregate per-candidate stats about blocked clusters (counting clusters, not towers) and penalties
    bridge_candidates as (
        select
            ccs.candidate_h3,
            floor(
                avg(
                    case
                    when ccs.has_visible_link then 0
                    else ccs.min_path_loss
                    end
                )::double precision / 5
            ) * 5 as avg_blocked_path_loss,
            count(*) filter (where ccs.has_visible_link) as visible_cluster_count,
            floor(               
                coalesce(
                    avg(
                        case
                            when ccs.has_visible_link then 0
                            else ccs.distance_to_cluster ^ -2.0
                        end
                    ),
                    0
                ) ^ (1/-2.0) / 5000
            ) * 5000 as min_blocked_distance
        from candidate_cluster_scores ccs
        group by ccs.candidate_h3
        having count(*) filter (where ccs.has_visible_link is not true) > 0
    ),
    -- Attach surface attributes to bridge candidates that passed blocking rules
    base_bridge_candidates as (
        select
            bc.candidate_h3 as h3,
            s.centroid_geog,
            s.visible_population,
            s.visible_uncovered_population,
            bc.visible_cluster_count::integer,
            bc.avg_blocked_path_loss,
            bc.min_blocked_distance
        from bridge_candidates bc
        join mesh_surface_h3_r8 s on s.h3 = bc.candidate_h3
        join cluster_stats stats on true
        where stats.cluster_count > 1
          and bc.visible_cluster_count >= 1
    ),
    -- Determine the global best bridge metrics for tie-breaking
    best_bridge_metrics as (
        select
            visible_cluster_count,
            avg_blocked_path_loss,
            min_blocked_distance
        from base_bridge_candidates
        order by
            visible_cluster_count desc,            
            min_blocked_distance asc,
            avg_blocked_path_loss asc
        limit 1
    ),
    -- Re-evaluate visible population for candidates tied on bridge criteria
    tie_bridge_candidates as (
        select
            bbc.*,
            coalesce(
                bbc.visible_population,
                mesh_surface_fill_visible_population(bbc.h3)
            ) as tie_visible_population
        from base_bridge_candidates bbc
        join best_bridge_metrics bbm
          on bbc.visible_cluster_count = bbm.visible_cluster_count
         and bbc.avg_blocked_path_loss = bbm.avg_blocked_path_loss
         and bbc.min_blocked_distance = bbm.min_blocked_distance
    ),
    -- Count how many new cells would become tower-ready if this candidate is installed
    tie_bridge_candidate_openings as (
        select
            tie.h3,
            count(*) as opened_candidate_cell_count
        from tie_bridge_candidates tie
        join mesh_surface_h3_r8 s
          on s.h3 <> tie.h3
         and s.has_road
         and s.can_place_tower is distinct from false
         and coalesce(s.visible_tower_count, 0) < 2
         and s.has_tower is not true
         and ST_DWithin(tie.centroid_geog, s.centroid_geog, max_distance)
         and h3_los_between_cells(tie.h3, s.h3)
        group by tie.h3
    )
    select
        tie.h3,
        tie.centroid_geog,
        tie.tie_visible_population as visible_population,
        tie.visible_uncovered_population,
        'bridge'::text as strategy,
        tie.visible_cluster_count,
        tie.avg_blocked_path_loss,
        tie.min_blocked_distance,
        coalesce(opened.opened_candidate_cell_count, 0) as new_candidate_cell_count
    into candidate
    from tie_bridge_candidates tie
    left join tie_bridge_candidate_openings opened on opened.h3 = tie.h3
    order by
        coalesce(opened.opened_candidate_cell_count, 0) desc,
        coalesce(tie.tie_visible_population, 0) desc,
        coalesce(tie.visible_uncovered_population, 0) desc
    limit 1;

    if not found then
        raise notice 'No bridge candidate available, recomputing visible_uncovered_population for greedy fallback';

        best_visible_uncovered := null;

        for recompute in
            select
                c.h3,
                c.centroid_geog,
                coalesce(
                    c.visible_population,
                    mesh_surface_fill_visible_population(c.h3),
                    0
                ) as visible_population
            from mesh_surface_h3_r8 c
            where c.can_place_tower
              and c.visible_uncovered_population is null
              and c.distance_to_closest_tower < max_distance
              and c.has_reception
              and c.visible_tower_count >= 2
            order by visible_population desc
        loop
            exit when best_visible_uncovered is not null
                   and best_visible_uncovered >= recompute.visible_population;

            select coalesce(sum(population), 0)
            into recompute_visible
            from mesh_surface_h3_r8 t
            where t.population > 0
              and t.has_reception is not true
              and ST_DWithin(recompute.centroid_geog, t.centroid_geog, max_distance)
              and h3_los_between_cells(recompute.h3, t.h3);

            update mesh_surface_h3_r8
            set visible_uncovered_population = recompute_visible
            where h3 = recompute.h3;

            if recompute_visible > coalesce(best_visible_uncovered, -1) then
                best_visible_uncovered := recompute_visible;
            end if;
        end loop;

        select g.h3,
               g.centroid_geog,
               coalesce(
                   g.visible_population,
                   mesh_surface_fill_visible_population(g.h3)
               ) as visible_population,
               g.visible_uncovered_population,
               'greedy'::text as strategy,
               null::integer as visible_cluster_count,
               null::double precision as avg_blocked_path_loss,
               null::double precision as min_blocked_distance,
               g.new_candidate_cell_count
        into candidate
        from (
            select
                s.*,
                coalesce(opened.opened_candidate_cell_count, 0) as new_candidate_cell_count
            from mesh_surface_h3_r8 s
            left join lateral (
                select
                    count(*) as opened_candidate_cell_count
                from mesh_surface_h3_r8 sc
                where sc.h3 <> s.h3
                  and sc.has_road
                  and sc.can_place_tower is distinct from false
                  and sc.has_tower is not true
                  and coalesce(sc.visible_tower_count, 0) < 2
                  and ST_DWithin(s.centroid_geog, sc.centroid_geog, max_distance)
                  and h3_los_between_cells(s.h3, sc.h3)
            ) opened on true
            where s.can_place_tower
              and coalesce(s.distance_to_closest_tower >= s.min_distance_to_closest_tower, false)
              and s.visible_tower_count >= 2
              and coalesce(s.visible_uncovered_population, 0) > 0
        ) g
        order by g.new_candidate_cell_count desc,
                 g.visible_uncovered_population desc
        limit 1;
    end if;

    if candidate.h3 is null then
        raise exception 'No eligible tower candidates remain for greedy iteration %', next_iteration;
    end if;

    if candidate.strategy = 'bridge' then
        raise notice 'Installing bridge tower #% at % (visible clusters %, avg blocked path loss % dB, min blocked distance % m, opens % tower candidates)',
            next_iteration,
            candidate.h3::text,
            candidate.visible_cluster_count,
            candidate.avg_blocked_path_loss,
            candidate.min_blocked_distance,
            candidate.new_candidate_cell_count;
    else
        raise notice 'Installing tower #% at % (visible population %, visible uncovered %, opens % tower candidates)',
            next_iteration,
            candidate.h3::text,
            coalesce(candidate.visible_population, 0),
            coalesce(candidate.visible_uncovered_population, 0),
            candidate.new_candidate_cell_count;
    end if;

    insert into mesh_towers (h3, source)
    values (
        candidate.h3,
        case candidate.strategy
            when 'bridge' then 'bridge'
            else 'greedy'
        end
    )
    on conflict (h3) do update set source = excluded.source;

    insert into mesh_greedy_iterations (iteration, chosen_h3, visible_population)
    values (next_iteration, candidate.h3, coalesce(candidate.visible_population, 0));

    update mesh_surface_h3_r8
    set has_tower = true,
        clearance = null,
        path_loss = null,
        visible_uncovered_population = 0,
        distance_to_closest_tower = 0
    where h3 = candidate.h3;

    update mesh_surface_h3_r8
    set clearance = null,
        path_loss = null,
        visible_uncovered_population = null,
        visible_tower_count = null,
        distance_to_closest_tower = coalesce(
            least(
                distance_to_closest_tower,
                ST_Distance(centroid_geog, candidate.centroid_geog)
            ),
            ST_Distance(centroid_geog, candidate.centroid_geog)
        )
    where h3 <> candidate.h3
      and ST_DWithin(centroid_geog, candidate.centroid_geog, recalc_radius);

    perform mesh_surface_refresh_visible_tower_counts(
        candidate.h3,
        recalc_radius,
        max_distance
    );

    perform mesh_surface_refresh_reception_metrics(
        candidate.h3,
        recalc_radius,
        max_distance
    );
end;
$$;

--commit;

vacuum mesh_surface_h3_r8;
