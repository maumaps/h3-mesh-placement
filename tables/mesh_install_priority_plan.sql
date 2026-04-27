set client_min_messages = notice;

drop table if exists mesh_install_priority_plan;

-- Create derived installer ordering table used by the HTML/CSV exporter.
create table mesh_install_priority_plan (
    tower_id integer primary key,
    cluster_key text not null,
    cluster_label text not null,
    cluster_install_rank integer,
    install_phase text not null,
    is_next_for_cluster boolean not null default false,
    rollout_status text not null,
    installed boolean not null,
    source text not null,
    label text not null,
    lon double precision not null,
    lat double precision not null,
    impact_score integer not null default 0,
    impact_people_est integer not null default 0,
    impact_tower_count integer not null default 0,
    next_unlock_count integer not null default 0,
    backlink_count integer not null default 0,
    primary_previous_tower_id integer,
    previous_connection_ids integer[] not null default '{}',
    next_connection_ids integer[] not null default '{}',
    inter_cluster_neighbor_ids integer[] not null default '{}',
    inter_cluster_connections text not null default ''
);

comment on table mesh_install_priority_plan is
    'Derived installer plan: rank zero installed nodes, connection backbone first, coverage frontier after that.';

do
$$
declare
    enabled boolean;
    current_cluster_key text;
    cluster_active_ids integer[];
    current_rank integer;
    current_component_count integer;
    chosen_start_tower_id integer;
    chosen_end_tower_id integer;
    chosen_neighbor_cluster_key text;
    chosen_connector_distance_m double precision;
    new_nodes_added integer;
    path_row record;
begin
    select coalesce((
        select value::boolean
        from mesh_pipeline_settings
        where setting = 'enable_install_priority_plan'
    ), true)
    into enabled;

    if not enabled then
        raise notice 'Install-priority plan disabled by mesh_pipeline_settings.enable_install_priority_plan';
        return;
    end if;

    -- Materialize current visible tower graph with stable edge ids for pgRouting.
    create temporary table install_priority_visible_edges on commit drop as
    select
        row_number() over (order by source_id, target_id) as id,
        source_id as source,
        target_id as target,
        distance_m::double precision as cost,
        distance_m::double precision as reverse_cost
    from mesh_visibility_edges
    where is_visible;

    create index install_priority_visible_edges_source_idx
        on install_priority_visible_edges (source);
    create index install_priority_visible_edges_target_idx
        on install_priority_visible_edges (target);

    -- Materialize a tiny country polygon table so plan ownership can prefer a
    -- same-country seed queue before falling back to a cross-border queue.
    create temporary table install_priority_countries on commit drop as
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
            case
                when lower(coalesce(tags ->> 'ISO3166-1:alpha2', '')) = 'am' then 'Armenia'
                else 'Georgia'
            end as country_name,
            ST_Multi(geog::geometry) as geom
        from osm_for_mesh_placement
        where tags ? 'boundary'
          and tags ->> 'boundary' = 'administrative'
          and tags ->> 'admin_level' = '2'
          and ST_GeometryType(geog::geometry) in ('ST_Polygon', 'ST_MultiPolygon')
    )
    select
        country_code,
        country_name,
        ST_Union(geom) as geom
    from admin_polygons
    where country_code is not null
    group by country_code, country_name;

    create index install_priority_countries_geom_idx
        on install_priority_countries using gist (geom);
    analyze install_priority_countries;

    -- Resolve duplicate installed MQTT roots before routing.
    -- Field-imported MQTT points can land on top of curated seeds or another
    -- MQTT import; those should not become separate rank-zero roots.
    create temporary table install_priority_suppressed_mqtt on commit drop as
    with mqtt_towers as (
        select
            tower.tower_id,
            tower.h3
        from mesh_towers tower
        where tower.source = 'mqtt'
    ),
    seed_towers as (
        select
            tower.tower_id,
            tower.h3
        from mesh_towers tower
        where tower.source = 'seed'
    ),
    seed_duplicate_mqtt as (
        select distinct mqtt_towers.tower_id
        from mqtt_towers
        join seed_towers
          on ST_DWithin(
                mqtt_towers.h3::geography,
                seed_towers.h3::geography,
                1000
             )
    ),
    mqtt_duplicate_mqtt as (
        select distinct duplicate_mqtt.tower_id
        from mqtt_towers duplicate_mqtt
        join mqtt_towers keeper_mqtt
          on keeper_mqtt.tower_id < duplicate_mqtt.tower_id
         and ST_DWithin(
                duplicate_mqtt.h3::geography,
                keeper_mqtt.h3::geography,
                1000
             )
        where not exists (
            select 1
            from seed_duplicate_mqtt seed_duplicate
            where seed_duplicate.tower_id = keeper_mqtt.tower_id
        )
    )
    select tower_id
    from seed_duplicate_mqtt
    union
    select tower_id
    from mqtt_duplicate_mqtt;

    create index install_priority_suppressed_mqtt_tower_id_idx
        on install_priority_suppressed_mqtt (tower_id);

    -- Resolve seed display names directly in SQL so cluster names are stable before export.
    create temporary table install_priority_towers on commit drop as
    with seed_names as (
        select distinct on (mesh_towers.tower_id)
            mesh_towers.tower_id,
            mesh_initial_nodes_h3_r8.name as seed_name
        from mesh_towers
        join mesh_initial_nodes_h3_r8
          on coalesce(mesh_initial_nodes_h3_r8.source, 'seed') = 'seed'
         and ST_DWithin(
                mesh_towers.h3::geography,
                mesh_initial_nodes_h3_r8.h3::geography,
                1000
             )
        where mesh_towers.source = 'seed'
        order by
            mesh_towers.tower_id,
            mesh_towers.h3::geography <-> mesh_initial_nodes_h3_r8.h3::geography,
            mesh_initial_nodes_h3_r8.name
    ),
    population_context as (
        select
            mesh_towers.tower_id,
            nearby_place.place_key,
            nearby_place.population_est
        from mesh_towers
        left join lateral (
            select
                osm_for_mesh_placement.osm_type || ':' || osm_for_mesh_placement.osm_id::text as place_key,
                nullif(
                    regexp_replace(osm_for_mesh_placement.tags ->> 'population', '[^0-9.]', '', 'g'),
                    ''
                )::numeric as population_est
            from osm_for_mesh_placement
            where osm_for_mesh_placement.tags ? 'place'
              and osm_for_mesh_placement.tags ? 'population'
              and nullif(
                    regexp_replace(osm_for_mesh_placement.tags ->> 'population', '[^0-9.]', '', 'g'),
                    ''
                ) is not null
              and ST_DWithin(
                    mesh_towers.h3::geography,
                    osm_for_mesh_placement.geog,
                    35000
                )
            order by mesh_towers.h3::geography <-> osm_for_mesh_placement.geog
            limit 1
        ) as nearby_place on true
    )
    select
        mesh_towers.tower_id,
        mesh_towers.h3,
        mesh_towers.source,
        case
            when mesh_towers.source = 'seed' then coalesce(seed_names.seed_name, 'seed #' || mesh_towers.tower_id::text)
            else mesh_towers.source || ' #' || mesh_towers.tower_id::text
        end as label,
        mesh_towers.source in ('seed', 'mqtt') as installed,
        ST_X(mesh_towers.h3::geometry) as lon,
        ST_Y(mesh_towers.h3::geometry) as lat,
        country_context.country_code,
        country_context.country_name,
        population_context.place_key as population_place_id,
        coalesce(population_context.population_est, 0)::double precision as population_est
    from mesh_towers
    left join lateral (
        select
            install_priority_countries.country_code,
            install_priority_countries.country_name
        from install_priority_countries
        where ST_Covers(
            install_priority_countries.geom,
            mesh_towers.h3::geometry
        )
        order by install_priority_countries.geom <-> mesh_towers.h3::geometry
        limit 1
    ) as country_context on true
    left join seed_names on seed_names.tower_id = mesh_towers.tower_id
    left join population_context on population_context.tower_id = mesh_towers.tower_id
    where not exists (
        select 1
        from install_priority_suppressed_mqtt suppressed_mqtt
        where suppressed_mqtt.tower_id = mesh_towers.tower_id
    );

    create index install_priority_towers_tower_id_idx
        on install_priority_towers (tower_id);

    -- Drop duplicate-MQTT edges too, otherwise pgRouting could still traverse
    -- hidden installed points as free intermediate graph vertices.
    delete from install_priority_visible_edges edge
    where not exists (
            select 1
            from install_priority_towers tower
            where tower.tower_id = edge.source
        )
       or not exists (
            select 1
            from install_priority_towers tower
            where tower.tower_id = edge.target
        );

    -- Find connected components among installed seed towers for seed-rooted cluster labels.
    create temporary table install_priority_seed_components on commit drop as
    with recursive seed_edges as (
        select
            edge.source as source_id,
            edge.target as target_id
        from install_priority_visible_edges edge
        join install_priority_towers source_tower on source_tower.tower_id = edge.source
        join install_priority_towers target_tower on target_tower.tower_id = edge.target
        where source_tower.source = 'seed'
          and target_tower.source = 'seed'
        union all
        select
            edge.target as source_id,
            edge.source as target_id
        from install_priority_visible_edges edge
        join install_priority_towers source_tower on source_tower.tower_id = edge.source
        join install_priority_towers target_tower on target_tower.tower_id = edge.target
        where source_tower.source = 'seed'
          and target_tower.source = 'seed'
    ),
    seed_walk(root_id, tower_id, path_ids) as (
        select
            tower_id as root_id,
            tower_id,
            array[tower_id] as path_ids
        from install_priority_towers
        where source = 'seed'

        union all

        select
            seed_walk.root_id,
            seed_edges.target_id,
            seed_walk.path_ids || seed_edges.target_id
        from seed_walk
        join seed_edges on seed_edges.source_id = seed_walk.tower_id
        where not seed_edges.target_id = any(seed_walk.path_ids)
    ),
    seed_roots as (
        select
            tower_id,
            min(root_id) as root_id
        from seed_walk
        group by tower_id
    )
    select
        seed_roots.tower_id,
        seed_roots.root_id,
        'seed:' || string_agg(component_towers.tower_id::text, '+' order by component_towers.tower_id) as cluster_key,
        string_agg(component_towers.label, ', ' order by lower(component_towers.label), component_towers.tower_id) as cluster_label,
        coalesce(
            min(component_towers.country_code)
                filter (where component_towers.source = 'seed' and component_towers.country_code is not null),
            min(component_towers.country_code)
                filter (where component_towers.country_code is not null),
            ''
        ) as country_code,
        coalesce(
            min(component_towers.country_name)
                filter (where component_towers.source = 'seed' and component_towers.country_name is not null),
            min(component_towers.country_name)
                filter (where component_towers.country_name is not null),
            ''
        ) as country_name
    from seed_roots
    join seed_roots component_roots on component_roots.root_id = seed_roots.root_id
    join install_priority_towers component_towers on component_towers.tower_id = component_roots.tower_id
    group by seed_roots.tower_id, seed_roots.root_id;

    create index install_priority_seed_components_tower_id_idx
        on install_priority_seed_components (tower_id);

    -- Assign every tower to the nearest installed seed component by visible path cost.
    create temporary table install_priority_cluster_assignment on commit drop as
    with seed_roots as (
        select distinct root_id
        from install_priority_seed_components
    ),
    routed_costs as (
        select
            paths.start_vid::integer as root_id,
            paths.end_vid::integer as tower_id,
            max(paths.agg_cost)::double precision as total_distance_m,
            max(paths.path_seq)::integer as hop_count
        from pgr_dijkstra(
            'select id, source, target, cost, reverse_cost from install_priority_visible_edges',
            (select array_agg(root_id::bigint) from seed_roots),
            (select array_agg(tower_id::bigint) from install_priority_towers),
            false
        ) as paths
        group by paths.start_vid, paths.end_vid
    ),
    best_assignment as (
        select distinct on (tower_id)
            routed_costs.tower_id,
            routed_costs.root_id,
            routed_costs.total_distance_m,
            routed_costs.hop_count
        from routed_costs
        join install_priority_towers target_tower
          on target_tower.tower_id = routed_costs.tower_id
        join install_priority_seed_components seed_components
          on seed_components.tower_id = routed_costs.root_id
        left join install_priority_seed_components target_seed_component
          on target_seed_component.tower_id = routed_costs.tower_id
        order by
            tower_id,
            case
                when target_tower.source = 'seed'
                 and routed_costs.root_id = target_seed_component.root_id then 0
                when target_tower.source = 'seed' then 1
                when target_tower.country_code is not null
                 and seed_components.country_code is not null
                 and target_tower.country_code = seed_components.country_code then 0
                when target_tower.country_code is not null
                 and seed_components.country_code is not null then 1
                else 0
            end,
            total_distance_m,
            hop_count,
            root_id
    )
    select
        best_assignment.tower_id,
        case
            when target_tower.source = 'seed' then target_seed_component.cluster_key
            else seed_components.cluster_key
        end as cluster_key,
        case
            when target_tower.source = 'seed' then target_seed_component.cluster_label
            else seed_components.cluster_label
        end as cluster_label,
        case
            when target_tower.source = 'seed' then target_seed_component.root_id
            else best_assignment.root_id
        end as root_id,
        best_assignment.total_distance_m,
        best_assignment.hop_count
    from best_assignment
    join install_priority_seed_components seed_components
      on seed_components.tower_id = best_assignment.root_id
    join install_priority_towers target_tower
      on target_tower.tower_id = best_assignment.tower_id
    left join install_priority_seed_components target_seed_component
      on target_seed_component.tower_id = best_assignment.tower_id;

    create index install_priority_cluster_assignment_tower_id_idx
        on install_priority_cluster_assignment (tower_id);
    create index install_priority_cluster_assignment_cluster_key_idx
        on install_priority_cluster_assignment (cluster_key);

    -- Pick one visible connector per neighboring cluster pair, minimizing current path-to-join cost.
    create temporary table install_priority_inter_cluster_connectors on commit drop as
    with connector_candidates as (
        select
            least(left_assignment.cluster_key, right_assignment.cluster_key) as left_cluster_key,
            greatest(left_assignment.cluster_key, right_assignment.cluster_key) as right_cluster_key,
            case
                when left_assignment.cluster_key <= right_assignment.cluster_key then edge.source
                else edge.target
            end as left_tower_id,
            case
                when left_assignment.cluster_key <= right_assignment.cluster_key then edge.target
                else edge.source
            end as right_tower_id,
            edge.cost as connector_distance_m,
            left_assignment.total_distance_m
                + edge.cost
                + right_assignment.total_distance_m as join_distance_m
        from install_priority_visible_edges edge
        join install_priority_cluster_assignment left_assignment
          on left_assignment.tower_id = edge.source
        join install_priority_cluster_assignment right_assignment
          on right_assignment.tower_id = edge.target
        where left_assignment.cluster_key <> right_assignment.cluster_key
    )
    select distinct on (left_cluster_key, right_cluster_key)
        left_cluster_key,
        right_cluster_key,
        left_tower_id,
        right_tower_id,
        connector_distance_m,
        join_distance_m
    from connector_candidates
    order by
        left_cluster_key,
        right_cluster_key,
        join_distance_m,
        connector_distance_m,
        left_tower_id,
        right_tower_id;

    create index install_priority_inter_cluster_connectors_left_idx
        on install_priority_inter_cluster_connectors (left_tower_id);
    create index install_priority_inter_cluster_connectors_right_idx
        on install_priority_inter_cluster_connectors (right_tower_id);

    -- Keep intra-cluster routing inside the seed-owned component so connector paths cannot steal towers from another rollout queue.
    create temporary table install_priority_cluster_edges on commit drop as
    select
        source_assignment.cluster_key,
        edge.id,
        edge.source,
        edge.target,
        edge.cost,
        edge.reverse_cost
    from install_priority_visible_edges edge
    join install_priority_cluster_assignment source_assignment
      on source_assignment.tower_id = edge.source
    join install_priority_cluster_assignment target_assignment
      on target_assignment.tower_id = edge.target
     and target_assignment.cluster_key = source_assignment.cluster_key;

    create index install_priority_cluster_edges_source_idx
        on install_priority_cluster_edges (source);
    create index install_priority_cluster_edges_target_idx
        on install_priority_cluster_edges (target);

    -- Build phase one as an install prefix:
    -- first merge disconnected installed roots inside each cluster, then
    -- append the earliest reachable connector endpoints to neighboring
    -- rollout clusters.
    create temporary table install_priority_phase_one_order (
        cluster_key text not null,
        tower_id integer primary key,
        phase_rank integer not null,
        parent_tower_id integer,
        install_reason text not null
    ) on commit drop;

    insert into install_priority_phase_one_order (
        cluster_key,
        tower_id,
        phase_rank,
        parent_tower_id,
        install_reason
    )
    select
        assignment.cluster_key,
        tower.tower_id,
        0,
        null::integer,
        'installed'
    from install_priority_towers tower
    join install_priority_cluster_assignment assignment
      on assignment.tower_id = tower.tower_id
    where tower.installed;

    for current_cluster_key in
        select distinct cluster_key
        from install_priority_cluster_assignment
        order by cluster_key
    loop
        select array_agg(tower_id order by tower_id)
        into cluster_active_ids
        from install_priority_phase_one_order
        where cluster_key = current_cluster_key;

        current_rank := 0;

        loop
            execute format(
                $sql$
                with active_nodes as (
                    select unnest(%1$L::integer[]) as tower_id
                ),
                components as (
                    select
                        active_nodes.tower_id,
                        coalesce(component_map.component, active_nodes.tower_id) as component_id
                    from active_nodes
                    left join pgr_connectedComponents(
                        'select row_number() over (order by source, target) as id, source, target, cost, reverse_cost from install_priority_cluster_edges where cluster_key = ''%2$s'' and source = any(''%1$s''::integer[]) and target = any(''%1$s''::integer[])'
                    ) as component_map
                      on component_map.node = active_nodes.tower_id
                )
                select count(distinct components.component_id)::integer
                from components
                join install_priority_towers tower
                  on tower.tower_id = components.tower_id
                where tower.installed
                $sql$,
                cluster_active_ids,
                current_cluster_key
            )
            into current_component_count;

            chosen_start_tower_id := null;
            chosen_end_tower_id := null;
            chosen_neighbor_cluster_key := null;
            chosen_connector_distance_m := null;

            if current_component_count > 1 then
                execute format(
                    $sql$
                    with active_nodes as (
                        select unnest(%1$L::integer[]) as tower_id
                    ),
                    components as (
                        select
                            active_nodes.tower_id,
                            coalesce(component_map.component, active_nodes.tower_id) as component_id
                        from active_nodes
                        left join pgr_connectedComponents(
                            'select row_number() over (order by source, target) as id, source, target, cost, reverse_cost from install_priority_cluster_edges where cluster_key = ''%2$s'' and source = any(''%1$s''::integer[]) and target = any(''%1$s''::integer[])'
                        ) as component_map
                          on component_map.node = active_nodes.tower_id
                    ),
                    candidate_paths as (
                        select
                            paths.start_vid::integer as start_tower_id,
                            paths.end_vid::integer as end_tower_id,
                            max(paths.agg_cost)::double precision as total_cost,
                            max(paths.path_seq)::integer as hop_count
                        from pgr_dijkstra(
                            'select id, source, target, cost, reverse_cost from install_priority_cluster_edges where cluster_key = ''%2$s''',
                            (select array_agg(tower_id::bigint order by tower_id) from active_nodes),
                            (select array_agg(tower_id::bigint order by tower_id) from active_nodes),
                            false
                        ) as paths
                        join components start_component
                          on start_component.tower_id = paths.start_vid
                        join components end_component
                          on end_component.tower_id = paths.end_vid
                         and end_component.component_id <> start_component.component_id
                        where paths.start_vid < paths.end_vid
                        group by paths.start_vid, paths.end_vid
                    )
                    select
                        start_tower_id,
                        end_tower_id
                    from candidate_paths
                    order by total_cost, hop_count, start_tower_id, end_tower_id
                    limit 1
                    $sql$,
                    cluster_active_ids,
                    current_cluster_key
                )
                into chosen_start_tower_id, chosen_end_tower_id;
            else
                execute format(
                    $sql$
                    with local_connectors as (
                        select
                            connector.right_cluster_key as neighbor_cluster_key,
                            connector.left_tower_id as local_tower_id,
                            connector.connector_distance_m
                        from install_priority_inter_cluster_connectors connector
                        where connector.left_cluster_key = %2$L
                          and not connector.left_tower_id = any(%1$L::integer[])
                        union all
                        select
                            connector.left_cluster_key as neighbor_cluster_key,
                            connector.right_tower_id as local_tower_id,
                            connector.connector_distance_m
                        from install_priority_inter_cluster_connectors connector
                        where connector.right_cluster_key = %2$L
                          and not connector.right_tower_id = any(%1$L::integer[])
                    ),
                    pending_endpoints as (
                        select
                            neighbor_cluster_key,
                            local_tower_id,
                            min(connector_distance_m) as connector_distance_m
                        from local_connectors
                        group by neighbor_cluster_key, local_tower_id
                    ),
                    endpoint_paths as (
                        select
                            paths.start_vid::integer as start_tower_id,
                            paths.end_vid::integer as end_tower_id,
                            pending_endpoints.neighbor_cluster_key,
                            pending_endpoints.connector_distance_m,
                            max(paths.agg_cost)::double precision as total_cost,
                            max(paths.path_seq)::integer as hop_count
                        from pgr_dijkstra(
                            'select id, source, target, cost, reverse_cost from install_priority_cluster_edges where cluster_key = ''%2$s''',
                            %1$L::bigint[],
                            (select array_agg(local_tower_id::bigint order by local_tower_id) from pending_endpoints),
                            false
                        ) as paths
                        join pending_endpoints
                          on pending_endpoints.local_tower_id = paths.end_vid
                        group by
                            paths.start_vid,
                            paths.end_vid,
                            pending_endpoints.neighbor_cluster_key,
                            pending_endpoints.connector_distance_m
                    ),
                    chosen_endpoint as (
                        select distinct on (end_tower_id)
                            start_tower_id,
                            end_tower_id,
                            neighbor_cluster_key,
                            connector_distance_m,
                            total_cost,
                            hop_count
                        from endpoint_paths
                        order by
                            end_tower_id,
                            total_cost,
                            hop_count,
                            start_tower_id
                    )
                    select
                        start_tower_id,
                        end_tower_id,
                        neighbor_cluster_key,
                        connector_distance_m
                    from chosen_endpoint
                    order by
                        total_cost,
                        connector_distance_m,
                        end_tower_id,
                        start_tower_id
                    limit 1
                    $sql$,
                    cluster_active_ids,
                    current_cluster_key
                )
                into chosen_start_tower_id, chosen_end_tower_id, chosen_neighbor_cluster_key, chosen_connector_distance_m;
            end if;

            if chosen_end_tower_id is null then
                if current_component_count > 1 then
                    raise exception 'mesh_install_priority_plan could not connect installed components inside cluster %',
                        current_cluster_key;
                end if;
                exit;
            end if;

            new_nodes_added := 0;

            for path_row in
                execute format(
                    $sql$
                    with path_rows as (
                        select
                            paths.path_seq::integer as path_seq,
                            paths.node::integer as tower_id,
                            lag(paths.node::integer) over (order by paths.path_seq) as parent_tower_id
                        from pgr_dijkstra(
                            'select id, source, target, cost, reverse_cost from install_priority_cluster_edges where cluster_key = ''%1$s''',
                            %2$s,
                            %3$s,
                            false
                        ) as paths
                    )
                    select
                        tower_id,
                        parent_tower_id
                    from path_rows
                    order by path_seq
                    $sql$,
                    current_cluster_key,
                    chosen_start_tower_id,
                    chosen_end_tower_id
                )
            loop
                if path_row.tower_id = any(cluster_active_ids) then
                    continue;
                end if;

                current_rank := current_rank + 1;
                new_nodes_added := new_nodes_added + 1;

                insert into install_priority_phase_one_order (
                    cluster_key,
                    tower_id,
                    phase_rank,
                    parent_tower_id,
                    install_reason
                )
                values (
                    current_cluster_key,
                    path_row.tower_id,
                    current_rank,
                    path_row.parent_tower_id,
                    case
                        when chosen_neighbor_cluster_key is null then 'connect_installed_component'
                        else 'connect_neighbor_cluster'
                    end
                );

                cluster_active_ids := cluster_active_ids || path_row.tower_id;
            end loop;

            if new_nodes_added = 0 then
                raise exception 'mesh_install_priority_plan phase one stalled for cluster % toward tower %',
                    current_cluster_key,
                    chosen_end_tower_id;
            end if;
        end loop;
    end loop;

    create index install_priority_phase_one_order_tower_id_idx
        on install_priority_phase_one_order (tower_id);
    create index install_priority_phase_one_order_cluster_key_idx
        on install_priority_phase_one_order (cluster_key);

    -- Iteratively append coverage nodes, requiring each next node to see the active network.
    create temporary table install_priority_phase_two_order on commit drop as
    with recursive cluster_state as (
        select
            assignment.cluster_key,
            array_agg(assignment.tower_id order by assignment.tower_id)
                filter (where phase_one.tower_id is not null) as active_ids,
            array_agg(assignment.tower_id order by assignment.tower_id)
                filter (where phase_one.tower_id is null) as remaining_ids,
            array_agg(distinct tower.population_place_id)
                filter (
                    where phase_one.tower_id is not null
                      and tower.population_place_id is not null
                ) as covered_place_ids
        from install_priority_cluster_assignment assignment
        join install_priority_towers tower on tower.tower_id = assignment.tower_id
        left join install_priority_phase_one_order phase_one on phase_one.tower_id = assignment.tower_id
        group by assignment.cluster_key
    ),
    walk as (
        select
            cluster_key,
            coalesce(active_ids, '{}') as active_ids,
            coalesce(remaining_ids, '{}') as remaining_ids,
            coalesce(covered_place_ids, '{}') as covered_place_ids,
            0 as step,
            null::integer as tower_id,
            null::integer as primary_previous_tower_id,
            null::integer[] as previous_connection_ids,
            0::integer as impact_people_est,
            0::integer as impact_tower_count,
            0::integer as next_unlock_count,
            0::integer as backlink_count
        from cluster_state

        union all

        select
            walk.cluster_key,
            walk.active_ids || candidate.tower_id,
            array_remove(walk.remaining_ids, candidate.tower_id),
            case
                when candidate.population_place_id is null
                  or candidate.population_place_id = any(walk.covered_place_ids) then walk.covered_place_ids
                else walk.covered_place_ids || candidate.population_place_id
            end,
            walk.step + 1,
            candidate.tower_id,
            candidate.primary_previous_tower_id,
            candidate.previous_connection_ids,
            candidate.impact_people_est,
            candidate.impact_tower_count,
            candidate.next_unlock_count,
            candidate.backlink_count
        from walk
        join lateral (
            with frontier as (
                select
                    candidate_tower.tower_id,
                    candidate_tower.source,
                    candidate_tower.population_place_id,
                    case
                        when candidate_tower.population_place_id is not null
                         and not candidate_tower.population_place_id = any(walk.covered_place_ids)
                            then candidate_tower.population_est::integer
                        else 0
                    end as impact_people_est,
                    array_agg(active_edge.target order by active_edge.cost, active_edge.target)
                        filter (where active_edge.target is not null) as previous_connection_ids,
                    min(active_edge.cost) as nearest_active_distance_m,
                    count(active_edge.target)::integer as backlink_count
                from install_priority_towers candidate_tower
                join install_priority_cluster_assignment assignment
                  on assignment.tower_id = candidate_tower.tower_id
                 and assignment.cluster_key = walk.cluster_key
                join lateral (
                    select
                        case
                            when edge.source = candidate_tower.tower_id then edge.target
                            else edge.source
                        end as target,
                        edge.cost
                    from install_priority_visible_edges edge
                    where (
                            edge.source = candidate_tower.tower_id
                            and edge.target = any(walk.active_ids)
                        )
                       or (
                            edge.target = candidate_tower.tower_id
                            and edge.source = any(walk.active_ids)
                        )
                ) as active_edge on true
                where candidate_tower.tower_id = any(walk.remaining_ids)
                group by
                    candidate_tower.tower_id,
                    candidate_tower.source,
                    candidate_tower.population_place_id,
                    candidate_tower.population_est
            ),
            scored_frontier as (
                select
                    frontier.*,
                    (
                        select count(*)
                        from install_priority_visible_edges edge
                        where (
                                edge.source = frontier.tower_id
                                and edge.target = any(walk.remaining_ids)
                                and edge.target <> frontier.tower_id
                            )
                           or (
                                edge.target = frontier.tower_id
                                and edge.source = any(walk.remaining_ids)
                                and edge.source <> frontier.tower_id
                            )
                    )::integer as next_unlock_count,
                    (
                        select count(*)
                        from install_priority_towers remaining_tower
                        where remaining_tower.tower_id = any(walk.remaining_ids)
                    )::integer as impact_tower_count
                from frontier
            )
            select
                scored_frontier.tower_id,
                scored_frontier.population_place_id,
                scored_frontier.impact_people_est,
                scored_frontier.impact_tower_count,
                scored_frontier.next_unlock_count,
                scored_frontier.backlink_count,
                scored_frontier.previous_connection_ids,
                scored_frontier.previous_connection_ids[1] as primary_previous_tower_id
            from scored_frontier
            order by
                scored_frontier.impact_people_est desc,
                scored_frontier.impact_tower_count desc,
                scored_frontier.next_unlock_count desc,
                scored_frontier.backlink_count desc,
                scored_frontier.nearest_active_distance_m asc,
                case
                    when scored_frontier.source in ('route', 'bridge') then 0
                    when scored_frontier.source = 'cluster_slim' then 1
                    when scored_frontier.source in ('population', 'coarse') then 2
                    else 3
                end,
                scored_frontier.tower_id
            limit 1
        ) as candidate on cardinality(walk.remaining_ids) > 0
    )
    select *
    from walk
    where step > 0;

    create index install_priority_phase_two_order_tower_id_idx
        on install_priority_phase_two_order (tower_id);

    -- Write the final plan table with phase-one ranks first and frontier coverage ranks after them.
    insert into mesh_install_priority_plan (
        tower_id,
        cluster_key,
        cluster_label,
        cluster_install_rank,
        install_phase,
        is_next_for_cluster,
        rollout_status,
        installed,
        source,
        label,
        lon,
        lat,
        impact_score,
        impact_people_est,
        impact_tower_count,
        next_unlock_count,
        backlink_count,
        primary_previous_tower_id,
        previous_connection_ids,
        next_connection_ids,
        inter_cluster_neighbor_ids,
        inter_cluster_connections
    )
    with max_phase_one_rank as (
        select
            cluster_key,
            coalesce(max(phase_rank) filter (where phase_rank > 0), 0) as max_rank
        from install_priority_phase_one_order
        group by cluster_key
    ),
    phase_one_rows as (
        select
            tower.tower_id,
            assignment.cluster_key,
            assignment.cluster_label,
            phase_one.phase_rank as cluster_install_rank,
            case when tower.installed then 'installed' else 'connect' end as install_phase,
            false as is_next_for_cluster,
            case when tower.installed then 'installed' else 'planned' end as rollout_status,
            tower.installed,
            tower.source,
            tower.label,
            tower.lon,
            tower.lat,
            0 as impact_score,
            0 as impact_people_est,
            0 as impact_tower_count,
            0 as next_unlock_count,
            coalesce(cardinality(previous_edges.previous_connection_ids), 0) as backlink_count,
            case
                when tower.installed then null::integer
                else coalesce(phase_one.parent_tower_id, previous_edges.primary_previous_tower_id)
            end as primary_previous_tower_id,
            coalesce(previous_edges.previous_connection_ids, '{}') as previous_connection_ids,
            '{}'::integer[] as next_connection_ids
        from install_priority_phase_one_order phase_one
        join install_priority_towers tower on tower.tower_id = phase_one.tower_id
        join install_priority_cluster_assignment assignment on assignment.tower_id = tower.tower_id
        left join lateral (
            with previous_candidates as (
                select
                    case when edge.source = tower.tower_id then edge.target else edge.source end as previous_tower_id,
                    edge.cost,
                    previous_phase_one.phase_rank
                from install_priority_visible_edges edge
                join install_priority_phase_one_order previous_phase_one
                  on previous_phase_one.tower_id = case when edge.source = tower.tower_id then edge.target else edge.source end
                 and previous_phase_one.cluster_key = phase_one.cluster_key
                where (
                        edge.source = tower.tower_id
                        or edge.target = tower.tower_id
                    )
                  and (
                        tower.installed
                        or previous_phase_one.phase_rank < phase_one.phase_rank
                        or previous_phase_one.phase_rank = 0
                    )
            )
            select
                (array_agg(
                    previous_tower_id
                    order by phase_rank desc, cost, previous_tower_id
                ))[1] as primary_previous_tower_id,
                array_agg(
                    previous_tower_id
                    order by cost, previous_tower_id
                ) as previous_connection_ids
            from previous_candidates
        ) as previous_edges on true
    ),
    phase_two_rows as (
        select
            tower.tower_id,
            assignment.cluster_key,
            assignment.cluster_label,
            max_phase_one_rank.max_rank + phase_two.step as cluster_install_rank,
            'coverage'::text as install_phase,
            phase_two.step = 1 as is_next_for_cluster,
            case when phase_two.step = 1 then 'next' else 'planned' end as rollout_status,
            false as installed,
            tower.source,
            tower.label,
            tower.lon,
            tower.lat,
            phase_two.impact_people_est as impact_score,
            phase_two.impact_people_est,
            phase_two.impact_tower_count,
            phase_two.next_unlock_count,
            phase_two.backlink_count,
            phase_two.primary_previous_tower_id,
            phase_two.previous_connection_ids,
            '{}'::integer[] as next_connection_ids
        from install_priority_phase_two_order phase_two
        join install_priority_towers tower on tower.tower_id = phase_two.tower_id
        join install_priority_cluster_assignment assignment on assignment.tower_id = tower.tower_id
        join max_phase_one_rank on max_phase_one_rank.cluster_key = assignment.cluster_key
    ),
    all_rows as (
        select * from phase_one_rows
        union all
        select * from phase_two_rows
    ),
    connector_metadata as (
        select
            endpoint.tower_id,
            array_agg(endpoint.neighbor_id order by endpoint.neighbor_id) as neighbor_ids,
            string_agg(endpoint.summary, ' | ' order by endpoint.summary) as summaries
        from (
            select
                left_tower_id as tower_id,
                right_tower_id as neighbor_id,
                right_cluster.cluster_label || ' via ' || right_tower.label || ' (Tower ' || right_tower_id::text || ')' as summary
            from install_priority_inter_cluster_connectors connector
            join install_priority_cluster_assignment right_cluster on right_cluster.tower_id = connector.right_tower_id
            join install_priority_towers right_tower on right_tower.tower_id = connector.right_tower_id
            union all
            select
                right_tower_id as tower_id,
                left_tower_id as neighbor_id,
                left_cluster.cluster_label || ' via ' || left_tower.label || ' (Tower ' || left_tower_id::text || ')' as summary
            from install_priority_inter_cluster_connectors connector
            join install_priority_cluster_assignment left_cluster on left_cluster.tower_id = connector.left_tower_id
            join install_priority_towers left_tower on left_tower.tower_id = connector.left_tower_id
        ) as endpoint
        group by endpoint.tower_id
    )
    select
        all_rows.tower_id,
        all_rows.cluster_key,
        all_rows.cluster_label,
        all_rows.cluster_install_rank,
        all_rows.install_phase,
        all_rows.is_next_for_cluster,
        all_rows.rollout_status,
        all_rows.installed,
        all_rows.source,
        all_rows.label,
        all_rows.lon,
        all_rows.lat,
        all_rows.impact_score,
        all_rows.impact_people_est,
        all_rows.impact_tower_count,
        all_rows.next_unlock_count,
        all_rows.backlink_count,
        all_rows.primary_previous_tower_id,
        all_rows.previous_connection_ids,
        all_rows.next_connection_ids,
        coalesce(connector_metadata.neighbor_ids, '{}') as inter_cluster_neighbor_ids,
        coalesce(connector_metadata.summaries, '') as inter_cluster_connections
    from all_rows
    left join connector_metadata on connector_metadata.tower_id = all_rows.tower_id
    order by
        all_rows.cluster_label,
        all_rows.cluster_install_rank,
        all_rows.tower_id;

    -- Assert cluster-local install ranks have no gaps.
    if exists (
        with ranked as (
            select
                cluster_key,
                cluster_install_rank,
                row_number() over (
                    partition by cluster_key
                    order by cluster_install_rank
                ) - 1 as expected_rank
            from (
                select distinct cluster_key, cluster_install_rank
                from mesh_install_priority_plan
                where cluster_install_rank is not null
            ) ranks
        )
        select 1
        from ranked
        where cluster_install_rank <> expected_rank
    ) then
        raise exception 'mesh_install_priority_plan has cluster_install_rank gaps';
    end if;

    -- Assert directly visible installed seed/MQTT roots stay in one rollout cluster.
    if exists (
        select 1
        from mesh_visibility_edges edge
        join mesh_install_priority_plan source_plan
          on source_plan.tower_id = edge.source_id
         and source_plan.installed
        join mesh_install_priority_plan target_plan
          on target_plan.tower_id = edge.target_id
         and target_plan.installed
        where edge.is_visible
          and source_plan.cluster_key <> target_plan.cluster_key
    ) then
        raise exception 'mesh_install_priority_plan split directly visible installed roots across clusters';
    end if;

    -- Assert every planned node has a visible predecessor from the earlier active graph.
    if exists (
        select 1
        from mesh_install_priority_plan row
        left join mesh_install_priority_plan previous
          on previous.tower_id = row.primary_previous_tower_id
        left join mesh_visibility_edges edge
          on edge.is_visible
         and (
                (edge.source_id = row.tower_id and edge.target_id = row.primary_previous_tower_id)
             or (edge.target_id = row.tower_id and edge.source_id = row.primary_previous_tower_id)
         )
        where row.cluster_install_rank > 0
          and (
                row.primary_previous_tower_id is null
             or previous.tower_id is null
             or previous.cluster_key <> row.cluster_key
             or previous.cluster_install_rank >= row.cluster_install_rank
             or edge.source_id is null
          )
    ) then
        raise exception 'mesh_install_priority_plan has invalid predecessor order or visibility';
    end if;

    create index mesh_install_priority_plan_cluster_rank_idx
        on mesh_install_priority_plan (cluster_key, cluster_install_rank);
end;
$$;
