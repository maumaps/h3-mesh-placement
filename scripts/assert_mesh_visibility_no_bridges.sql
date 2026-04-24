set client_min_messages = notice;

-- Assert that the refreshed live visibility graph has no articulation towers
-- or bridge edges before exporting the installer handout.
do
$$
declare
    finding_total integer;
    finding_summary text;
begin
    if to_regclass('mesh_towers') is null then
        raise exception 'mesh_towers is missing; cannot verify bridge/cut-node visibility invariant';
    end if;

    if to_regclass('mesh_visibility_edges') is null then
        raise exception 'mesh_visibility_edges is missing; refresh visibility before verifying bridge/cut-node invariant';
    end if;

    with recursive
    nodes as (
        -- Only live rollout towers participate in the installer graph.
        select
            tower_id,
            source
        from mesh_towers
        where source <> 'mqtt'
    ),
    visible_edges as (
        -- Treat direct LOS links as undirected graph edges.
        select
            e.source_id,
            source_tower.source as source_type,
            e.target_id,
            target_tower.source as target_type,
            e.distance_m
        from mesh_visibility_edges e
        join nodes source_tower on source_tower.tower_id = e.source_id
        join nodes target_tower on target_tower.tower_id = e.target_id
        where e.is_visible
    ),
    undirected_edges as (
        select source_id as a, target_id as b
        from visible_edges

        union all

        select target_id as a, source_id as b
        from visible_edges
    ),
    removed_nodes as (
        select tower_id as removed_id
        from nodes
    ),
    node_walk_starts as (
        -- For each removed node, start from the lowest surviving tower.
        select
            removed_nodes.removed_id,
            min(nodes.tower_id) as start_id
        from removed_nodes
        join nodes on nodes.tower_id <> removed_nodes.removed_id
        group by removed_nodes.removed_id
    ),
    node_walk(removed_id, tower_id) as (
        select
            removed_id,
            start_id
        from node_walk_starts

        union

        select
            node_walk.removed_id,
            undirected_edges.b
        from node_walk
        join undirected_edges on undirected_edges.a = node_walk.tower_id
        where undirected_edges.b <> node_walk.removed_id
    ),
    node_reach as (
        select
            removed_id,
            count(distinct tower_id) as reachable_count
        from node_walk
        group by removed_id
    ),
    node_total as (
        select count(*) - 1 as remaining_count
        from nodes
    ),
    articulation_report as (
        select
            'cut_node'::text as finding_type,
            removed_nodes.removed_id as source_id,
            removed_tower.source as source_type,
            null::integer as target_id,
            null::text as target_type,
            null::double precision as distance_m,
            coalesce(node_reach.reachable_count, 0) as reachable_count,
            node_total.remaining_count,
            least(
                coalesce(node_reach.reachable_count, 0),
                node_total.remaining_count - coalesce(node_reach.reachable_count, 0)
            ) as smaller_side_count
        from removed_nodes
        cross join node_total
        left join node_reach on node_reach.removed_id = removed_nodes.removed_id
        join nodes removed_tower on removed_tower.tower_id = removed_nodes.removed_id
        where coalesce(node_reach.reachable_count, 0) <> node_total.remaining_count
    ),
    removed_edges as (
        select
            source_id,
            target_id
        from visible_edges
    ),
    edge_walk_edges as (
        -- For every removed edge, expose all other LOS edges in both directions.
        select
            visible_edges.source_id as a,
            visible_edges.target_id as b,
            removed_edges.source_id as removed_source,
            removed_edges.target_id as removed_target
        from visible_edges
        cross join removed_edges
        where not (
            visible_edges.source_id = removed_edges.source_id
            and visible_edges.target_id = removed_edges.target_id
        )

        union all

        select
            visible_edges.target_id as a,
            visible_edges.source_id as b,
            removed_edges.source_id as removed_source,
            removed_edges.target_id as removed_target
        from visible_edges
        cross join removed_edges
        where not (
            visible_edges.source_id = removed_edges.source_id
            and visible_edges.target_id = removed_edges.target_id
        )
    ),
    edge_walk_starts as (
        -- Always start from the same tower so reachability changes are deterministic.
        select
            removed_edges.source_id as removed_source,
            removed_edges.target_id as removed_target,
            min(nodes.tower_id) as start_id
        from removed_edges
        cross join nodes
        group by
            removed_edges.source_id,
            removed_edges.target_id
    ),
    edge_walk(removed_source, removed_target, tower_id) as (
        select
            removed_source,
            removed_target,
            start_id
        from edge_walk_starts

        union

        select
            edge_walk.removed_source,
            edge_walk.removed_target,
            edge_walk_edges.b
        from edge_walk
        join edge_walk_edges
          on edge_walk_edges.a = edge_walk.tower_id
         and edge_walk_edges.removed_source = edge_walk.removed_source
         and edge_walk_edges.removed_target = edge_walk.removed_target
    ),
    edge_reach as (
        select
            removed_source,
            removed_target,
            count(distinct tower_id) as reachable_count
        from edge_walk
        group by
            removed_source,
            removed_target
    ),
    edge_total as (
        select count(*) as remaining_count
        from nodes
    ),
    bridge_report as (
        select
            'bridge_edge'::text as finding_type,
            edge_reach.removed_source as source_id,
            visible_edges.source_type,
            edge_reach.removed_target as target_id,
            visible_edges.target_type,
            visible_edges.distance_m,
            edge_reach.reachable_count,
            edge_total.remaining_count,
            least(
                edge_reach.reachable_count,
                edge_total.remaining_count - edge_reach.reachable_count
            ) as smaller_side_count
        from edge_reach
        cross join edge_total
        join visible_edges
          on visible_edges.source_id = edge_reach.removed_source
         and visible_edges.target_id = edge_reach.removed_target
        where edge_reach.reachable_count <> edge_total.remaining_count
    ),
    findings as (
        select
            finding_type,
            source_id,
            source_type,
            target_id,
            target_type,
            round(distance_m)::integer as distance_m,
            reachable_count,
            remaining_count,
            smaller_side_count
        from articulation_report

        union all

        select
            finding_type,
            source_id,
            source_type,
            target_id,
            target_type,
            round(distance_m)::integer as distance_m,
            reachable_count,
            remaining_count,
            smaller_side_count
        from bridge_report
    )
    select
        count(*),
        string_agg(
            case
                when target_id is null then format(
                    '%s %s:%s smaller_side=%s',
                    finding_type,
                    source_id,
                    source_type,
                    smaller_side_count
                )
                else format(
                    '%s %s:%s -> %s:%s distance_m=%s smaller_side=%s',
                    finding_type,
                    source_id,
                    source_type,
                    target_id,
                    target_type,
                    distance_m,
                    smaller_side_count
                )
            end,
            '; '
            order by
                smaller_side_count desc,
                finding_type,
                source_id,
                target_id
        )
    into finding_total, finding_summary
    from findings;

    if finding_total > 0 then
        raise exception
            'Visibility redundancy invariant failed: % bridge/cut-node finding(s): %',
            finding_total,
            finding_summary;
    end if;

    raise notice 'Visibility redundancy invariant holds: no bridge edges or cut nodes';
end;
$$;
