set client_min_messages = notice;

-- Contract soft population anchors after routing has used them as demand hints.
-- This stage never computes fresh LOS. It only trusts mesh_los_cache, because
-- close H3 cells can have different visibility roles even when they look like
-- duplicates on the map.
do
$$
declare
    enabled boolean := true;
    max_distance constant double precision := 100000;
    mast_height double precision := 28;
    frequency double precision := 868000000;
    merge_distance double precision := 0;
    generated_merge_distance double precision := 10000;
    generated_sources constant text[] := array['route', 'cluster_slim', 'bridge', 'coarse'];
    target_sources constant text[] := array['population', 'route', 'cluster_slim', 'bridge', 'coarse'];
    anchor record;
    replacement record;
    synthetic record;
    leaf record;
    preserves_single_component boolean;
    current_component_count integer;
    hypothetical_component_count integer;
    removed_population integer := 0;
    removed_generated integer := 0;
begin
    if to_regclass('mesh_pipeline_settings') is null then
        raise notice 'mesh_pipeline_settings missing, skipping population anchor contraction';
        return;
    end if;

    select coalesce((
        select value::boolean
        from mesh_pipeline_settings
        where setting = 'enable_population_anchor_contract'
    ), true)
    into enabled;

    if not enabled then
        raise notice 'Population anchor contraction disabled by mesh_pipeline_settings.enable_population_anchor_contract';
        return;
    end if;

    if to_regclass('mesh_towers') is null
       or to_regclass('mesh_surface_h3_r8') is null
       or to_regclass('mesh_los_cache') is null then
        raise notice 'Required placement tables missing, skipping population anchor contraction';
        return;
    end if;

    select coalesce((
        select value::double precision
        from mesh_pipeline_settings
        where setting = 'mast_height_m'
    ), 28)
    into mast_height;

    select coalesce((
        select value::double precision
        from mesh_pipeline_settings
        where setting = 'frequency_hz'
    ), 868000000)
    into frequency;

    select greatest(coalesce((
        select value::double precision
        from mesh_pipeline_settings
        where setting = 'population_anchor_contract_distance_m'
    ), 0), 0)
    into merge_distance;

    select greatest(coalesce((
        select value::double precision
        from mesh_pipeline_settings
        where setting = 'generated_tower_merge_distance_m'
    ), 10000), 0)
    into generated_merge_distance;

    create temporary table if not exists mesh_population_anchor_contract_deleted_h3 (
        h3 h3index primary key
    ) on commit drop;
    truncate mesh_population_anchor_contract_deleted_h3;

    create temporary table if not exists mesh_population_anchor_contract_affected_points (
        centroid_geog public.geography not null
    ) on commit drop;
    truncate mesh_population_anchor_contract_affected_points;

    for anchor in
        select
            t.tower_id,
            t.h3,
            t.centroid_geog
        from mesh_towers t
        where t.source = 'population'
        order by t.tower_id
    loop
        -- Pick a generated route-like neighbor that can preserve every
        -- non-population cached LOS neighbor of this soft population anchor.
        with anchor_neighbors as materialized (
            select
                nb.tower_id,
                nb.h3,
                nb.source,
                nb.centroid_geog
            from mesh_towers nb
            join mesh_los_cache mlc
              on mlc.src_h3 = least(anchor.h3, nb.h3)
             and mlc.dst_h3 = greatest(anchor.h3, nb.h3)
             and mlc.mast_height_src = mast_height
             and mlc.mast_height_dst = mast_height
             and mlc.frequency_hz = frequency
             and mlc.clearance > 0
            where nb.tower_id <> anchor.tower_id
              and nb.source <> 'population'
        ),
        candidate_replacements as materialized (
            select an.*
            from anchor_neighbors an
            where an.source = any(generated_sources)
              and (
                    merge_distance <= 0
                    or ST_DWithin(an.centroid_geog, anchor.centroid_geog, merge_distance)
                )
        ),
        replacement_scores as (
            select
                candidate.tower_id,
                candidate.h3,
                candidate.centroid_geog,
                count(required.tower_id) filter (where required.tower_id <> candidate.tower_id) as preserved_required_neighbors,
                (
                    select count(*)
                    from mesh_towers nb
                    join mesh_los_cache candidate_link
                      on candidate_link.src_h3 = least(candidate.h3, nb.h3)
                     and candidate_link.dst_h3 = greatest(candidate.h3, nb.h3)
                     and candidate_link.mast_height_src = mast_height
                     and candidate_link.mast_height_dst = mast_height
                     and candidate_link.frequency_hz = frequency
                     and candidate_link.clearance > 0
                    where nb.tower_id <> candidate.tower_id
                      and nb.source <> 'population'
                ) as candidate_visible_neighbors,
                ST_Distance(candidate.centroid_geog, anchor.centroid_geog) as distance_m
            from candidate_replacements candidate
            left join anchor_neighbors required
              on required.tower_id <> candidate.tower_id
            where not exists (
                select 1
                from anchor_neighbors required
                where required.tower_id <> candidate.tower_id
                  and not exists (
                        select 1
                        from mesh_los_cache replacement_link
                        where replacement_link.src_h3 = least(candidate.h3, required.h3)
                          and replacement_link.dst_h3 = greatest(candidate.h3, required.h3)
                          and replacement_link.mast_height_src = mast_height
                          and replacement_link.mast_height_dst = mast_height
                          and replacement_link.frequency_hz = frequency
                          and replacement_link.clearance > 0
                    )
            )
            group by candidate.tower_id, candidate.h3, candidate.centroid_geog
        )
        select *
        into replacement
        from replacement_scores
        order by
            preserved_required_neighbors desc,
            candidate_visible_neighbors desc,
            distance_m asc,
            tower_id asc
        limit 1;

        if replacement.tower_id is null then
            -- Sometimes no existing generated neighbor can replace the soft
            -- population anchor, but a nearby placeable H3 can carry the
            -- combined LOS role of one generated neighbor plus the anchor.
            -- In that case move the generated tower to the synthetic cell and
            -- then remove the population anchor. This handles two-node stars
            -- without introducing distance-only duplicate pruning.
            with generated_neighbors as materialized (
                select
                    nb.tower_id,
                    nb.h3,
                    nb.source,
                    nb.centroid_geog,
                    ST_Centroid(ST_Collect(anchor.h3::geometry, nb.h3::geometry))::public.geography as midpoint_geog
                from mesh_towers nb
                join mesh_los_cache anchor_link
                  on anchor_link.src_h3 = least(anchor.h3, nb.h3)
                 and anchor_link.dst_h3 = greatest(anchor.h3, nb.h3)
                 and anchor_link.mast_height_src = mast_height
                 and anchor_link.mast_height_dst = mast_height
                 and anchor_link.frequency_hz = frequency
                 and anchor_link.clearance > 0
                where nb.source = any(generated_sources)
                  and ST_DWithin(nb.centroid_geog, anchor.centroid_geog, generated_merge_distance)
            ),
            pair_required as materialized (
                select distinct
                    gn.tower_id as generated_tower_id,
                    required.tower_id,
                    required.h3
                from generated_neighbors gn
                join mesh_towers required
                  on required.tower_id not in (anchor.tower_id, gn.tower_id)
                 and required.source <> 'population'
                join mesh_los_cache anchor_required_link
                  on anchor_required_link.src_h3 = least(anchor.h3, required.h3)
                 and anchor_required_link.dst_h3 = greatest(anchor.h3, required.h3)
                 and anchor_required_link.mast_height_src = mast_height
                 and anchor_required_link.mast_height_dst = mast_height
                 and anchor_required_link.frequency_hz = frequency
                 and anchor_required_link.clearance > 0

                union

                select distinct
                    gn.tower_id as generated_tower_id,
                    required.tower_id,
                    required.h3
                from generated_neighbors gn
                join mesh_towers required
                  on required.tower_id not in (anchor.tower_id, gn.tower_id)
                 and required.source <> 'population'
                join mesh_los_cache generated_required_link
                  on generated_required_link.src_h3 = least(gn.h3, required.h3)
                 and generated_required_link.dst_h3 = greatest(gn.h3, required.h3)
                 and generated_required_link.mast_height_src = mast_height
                 and generated_required_link.mast_height_dst = mast_height
                 and generated_required_link.frequency_hz = frequency
                 and generated_required_link.clearance > 0
            ),
            synthetic_candidates as (
                select
                    gn.tower_id,
                    gn.h3 as old_h3,
                    gn.centroid_geog as old_centroid_geog,
                    s.h3 as new_h3,
                    s.centroid_geog as new_centroid_geog,
                    count(pr.tower_id) as preserved_required_neighbors,
                    ST_Distance(s.centroid_geog, gn.midpoint_geog) as midpoint_distance_m,
                    coalesce(nullif(s.visible_population, 0), s.population_70km, s.population, 0) as score,
                    coalesce(s.has_building, false) as has_building,
                    coalesce(s.building_count, 0) as building_count
                from generated_neighbors gn
                join mesh_surface_h3_r8 s
                  on s.is_in_boundaries
                 and s.has_road
                 and not s.is_in_unfit_area
                 and s.h3 not in (anchor.h3, gn.h3)
                 and ST_DWithin(
                        s.centroid_geog,
                        gn.midpoint_geog,
                        generated_merge_distance
                    )
                 and not exists (
                        select 1
                        from mesh_towers existing
                        where existing.h3 = s.h3
                          and existing.tower_id not in (anchor.tower_id, gn.tower_id)
                    )
                left join pair_required pr
                  on pr.generated_tower_id = gn.tower_id
                where not exists (
                        select 1
                        from pair_required required
                        where required.generated_tower_id = gn.tower_id
                          and not exists (
                                select 1
                                from mesh_los_cache candidate_link
                                where candidate_link.src_h3 = least(s.h3, required.h3)
                                  and candidate_link.dst_h3 = greatest(s.h3, required.h3)
                                  and candidate_link.mast_height_src = mast_height
                                  and candidate_link.mast_height_dst = mast_height
                                  and candidate_link.frequency_hz = frequency
                                  and candidate_link.clearance > 0
                            )
                    )
                group by
                    gn.tower_id,
                    gn.h3,
                    gn.centroid_geog,
                    gn.midpoint_geog,
                    s.h3,
                    s.centroid_geog,
                    s.visible_population,
                    s.population_70km,
                    s.population,
                    s.has_building,
                    s.building_count
            )
            select *
            into synthetic
            from synthetic_candidates
            order by
                preserved_required_neighbors desc,
                has_building desc,
                score desc,
                building_count desc,
                midpoint_distance_m asc,
                tower_id asc,
                new_h3 asc
            limit 1;

            if synthetic.tower_id is null then
                continue;
            end if;

            insert into mesh_population_anchor_contract_deleted_h3 (h3)
            values (synthetic.old_h3)
            on conflict do nothing;

            insert into mesh_population_anchor_contract_affected_points (centroid_geog)
            values (synthetic.old_centroid_geog), (synthetic.new_centroid_geog);

            update mesh_towers
            set h3 = synthetic.new_h3
            where tower_id = synthetic.tower_id;

            update mesh_surface_h3_r8 s
            set has_tower = false,
                clearance = null,
                path_loss = null,
                visible_population = null,
                visible_uncovered_population = null
            where s.h3 = synthetic.old_h3
              and not exists (
                    select 1
                    from mesh_towers mt
                    where mt.h3 = s.h3
                );

            update mesh_surface_h3_r8
            set has_tower = true,
                clearance = null,
                path_loss = null,
                visible_population = null,
                visible_uncovered_population = 0,
                distance_to_closest_tower = 0
            where h3 = synthetic.new_h3;

            if to_regclass('mesh_tower_wiggle_queue') is not null then
                update mesh_tower_wiggle_queue
                set is_dirty = true
                where tower_id = synthetic.tower_id;
            end if;

            select
                synthetic.tower_id as tower_id,
                synthetic.new_h3 as h3,
                synthetic.new_centroid_geog as centroid_geog
            into replacement;

            raise notice 'Moved generated tower % from % to % before contracting population anchor % at %',
                synthetic.tower_id,
                synthetic.old_h3,
                synthetic.new_h3,
                anchor.tower_id,
                anchor.h3;
        end if;

        -- Generated leaves around this population anchor can be removed only
        -- if the chosen replacement preserves the leaf's own non-population
        -- visible-neighbor set. This handles small stars without pairwise
        -- distance pruning.
        for leaf in
            select
                nb.tower_id,
                nb.h3,
                nb.centroid_geog
            from mesh_towers nb
            join mesh_los_cache anchor_leaf_link
              on anchor_leaf_link.src_h3 = least(anchor.h3, nb.h3)
             and anchor_leaf_link.dst_h3 = greatest(anchor.h3, nb.h3)
             and anchor_leaf_link.mast_height_src = mast_height
             and anchor_leaf_link.mast_height_dst = mast_height
             and anchor_leaf_link.frequency_hz = frequency
             and anchor_leaf_link.clearance > 0
            where nb.tower_id <> replacement.tower_id
              and nb.source = any(generated_sources)
              and ST_DWithin(nb.centroid_geog, anchor.centroid_geog, generated_merge_distance)
              and not exists (
                    select 1
                    from mesh_towers required
                    join mesh_los_cache leaf_link
                      on leaf_link.src_h3 = least(nb.h3, required.h3)
                     and leaf_link.dst_h3 = greatest(nb.h3, required.h3)
                     and leaf_link.mast_height_src = mast_height
                     and leaf_link.mast_height_dst = mast_height
                     and leaf_link.frequency_hz = frequency
                     and leaf_link.clearance > 0
                    where required.tower_id not in (anchor.tower_id, nb.tower_id, replacement.tower_id)
                      and required.source <> 'population'
                      and not exists (
                            select 1
                            from mesh_los_cache replacement_link
                            where replacement_link.src_h3 = least(replacement.h3, required.h3)
                              and replacement_link.dst_h3 = greatest(replacement.h3, required.h3)
                              and replacement_link.mast_height_src = mast_height
                              and replacement_link.mast_height_dst = mast_height
                              and replacement_link.frequency_hz = frequency
                              and replacement_link.clearance > 0
                        )
                )
            order by nb.tower_id
        loop
            -- Never remove a generated leaf if that would increase the number
            -- of live tower LOS components. Local neighbor preservation is not
            -- enough when population anchors bridge other population-only
            -- segments.
            with recursive current_towers as (
                select
                    tower_id,
                    h3
                from mesh_towers
            ),
            current_visible_edges as (
                select distinct
                    src.tower_id as source_id,
                    dst.tower_id as target_id
                from current_towers src
                join current_towers dst on dst.tower_id <> src.tower_id
                join mesh_los_cache link
                  on link.src_h3 = least(src.h3, dst.h3)
                 and link.dst_h3 = greatest(src.h3, dst.h3)
                 and link.mast_height_src = mast_height
                 and link.mast_height_dst = mast_height
                 and link.frequency_hz = frequency
                 and link.clearance > 0
            ),
            current_walk(root_id, tower_id, path) as (
                select
                    current_towers.tower_id as root_id,
                    current_towers.tower_id,
                    array[current_towers.tower_id]
                from current_towers

                union all

                select
                    current_walk.root_id,
                    current_visible_edges.target_id,
                    current_walk.path || current_visible_edges.target_id
                from current_walk
                join current_visible_edges on current_visible_edges.source_id = current_walk.tower_id
                where not current_visible_edges.target_id = any(current_walk.path)
            ),
            current_components as (
                select
                    tower_id,
                    min(root_id) as component_id
                from current_walk
                group by tower_id
            )
            select count(distinct component_id)
            into current_component_count
            from current_components;

            with recursive hypothetical_towers as (
                select
                    tower_id,
                    h3
                from mesh_towers
                where tower_id <> leaf.tower_id
            ),
            hypothetical_visible_edges as (
                select distinct
                    src.tower_id as source_id,
                    dst.tower_id as target_id
                from hypothetical_towers src
                join hypothetical_towers dst on dst.tower_id <> src.tower_id
                join mesh_los_cache link
                  on link.src_h3 = least(src.h3, dst.h3)
                 and link.dst_h3 = greatest(src.h3, dst.h3)
                 and link.mast_height_src = mast_height
                 and link.mast_height_dst = mast_height
                 and link.frequency_hz = frequency
                 and link.clearance > 0
            ),
            hypothetical_walk(root_id, tower_id, path) as (
                select
                    hypothetical_towers.tower_id as root_id,
                    hypothetical_towers.tower_id,
                    array[hypothetical_towers.tower_id]
                from hypothetical_towers

                union

                select
                    hypothetical_walk.root_id,
                    hypothetical_visible_edges.target_id,
                    hypothetical_walk.path || hypothetical_visible_edges.target_id
                from hypothetical_walk
                join hypothetical_visible_edges on hypothetical_visible_edges.source_id = hypothetical_walk.tower_id
                where not hypothetical_visible_edges.target_id = any(hypothetical_walk.path)
            ),
            hypothetical_components as (
                select
                    tower_id,
                    min(root_id) as component_id
                from hypothetical_walk
                group by tower_id
            )
            select
                case
                    when (select count(*) from hypothetical_towers) <= 1 then true
                    else (
                        select count(distinct component_id)
                        from hypothetical_components
                    ) <= current_component_count
                end
            into preserves_single_component;

            if not coalesce(preserves_single_component, false) then
                raise notice 'Skipping generated leaf % around population anchor % because deleting it would split the live LOS graph',
                    leaf.tower_id,
                    anchor.tower_id;
                continue;
            end if;

            insert into mesh_population_anchor_contract_deleted_h3 (h3)
            values (leaf.h3)
            on conflict do nothing;

            insert into mesh_population_anchor_contract_affected_points (centroid_geog)
            values (leaf.centroid_geog), (replacement.centroid_geog);

            if to_regclass('mesh_tower_wiggle_queue') is not null then
                delete from mesh_tower_wiggle_queue
                where tower_id = leaf.tower_id;
            end if;

            delete from mesh_towers
            where tower_id = leaf.tower_id;

            removed_generated := removed_generated + 1;
        end loop;

        -- Keep soft population anchors only when their removal does not
        -- increase the number of live LOS components. Existing replacement
        -- checks above cover local neighbor roles, while this guard protects
        -- bridges that pass through another population anchor.
        with recursive current_towers as (
            select
                tower_id,
                h3
            from mesh_towers
        ),
        current_visible_edges as (
            select distinct
                src.tower_id as source_id,
                dst.tower_id as target_id
            from current_towers src
            join current_towers dst on dst.tower_id <> src.tower_id
            join mesh_los_cache link
              on link.src_h3 = least(src.h3, dst.h3)
             and link.dst_h3 = greatest(src.h3, dst.h3)
             and link.mast_height_src = mast_height
             and link.mast_height_dst = mast_height
             and link.frequency_hz = frequency
             and link.clearance > 0
        ),
        current_walk(root_id, tower_id, path) as (
            select
                current_towers.tower_id as root_id,
                current_towers.tower_id,
                array[current_towers.tower_id]
            from current_towers

            union

            select
                current_walk.root_id,
                current_visible_edges.target_id,
                current_walk.path || current_visible_edges.target_id
            from current_walk
            join current_visible_edges on current_visible_edges.source_id = current_walk.tower_id
            where not current_visible_edges.target_id = any(current_walk.path)
        ),
        current_components as (
            select
                tower_id,
                min(root_id) as component_id
            from current_walk
            group by tower_id
        )
        select count(distinct component_id)
        into current_component_count
        from current_components;

        with recursive hypothetical_towers as (
            select
                tower_id,
                h3
            from mesh_towers
            where tower_id <> anchor.tower_id
        ),
        hypothetical_visible_edges as (
            select distinct
                src.tower_id as source_id,
                dst.tower_id as target_id
            from hypothetical_towers src
            join hypothetical_towers dst on dst.tower_id <> src.tower_id
            join mesh_los_cache link
              on link.src_h3 = least(src.h3, dst.h3)
             and link.dst_h3 = greatest(src.h3, dst.h3)
             and link.mast_height_src = mast_height
             and link.mast_height_dst = mast_height
             and link.frequency_hz = frequency
             and link.clearance > 0
        ),
        hypothetical_walk(root_id, tower_id, path) as (
            select
                hypothetical_towers.tower_id as root_id,
                hypothetical_towers.tower_id,
                array[hypothetical_towers.tower_id]
            from hypothetical_towers

            union

            select
                hypothetical_walk.root_id,
                hypothetical_visible_edges.target_id,
                hypothetical_walk.path || hypothetical_visible_edges.target_id
            from hypothetical_walk
            join hypothetical_visible_edges on hypothetical_visible_edges.source_id = hypothetical_walk.tower_id
            where not hypothetical_visible_edges.target_id = any(hypothetical_walk.path)
        ),
        hypothetical_components as (
            select
                tower_id,
                min(root_id) as component_id
            from hypothetical_walk
            group by tower_id
        )
        select
            case
                when (select count(*) from hypothetical_towers) <= 1 then true
                else (
                    select count(distinct component_id)
                    from hypothetical_components
                ) <= current_component_count
            end
        into preserves_single_component;

        if not coalesce(preserves_single_component, false) then
            raise notice 'Skipping population anchor % at % because deleting it would split the live LOS graph',
                anchor.tower_id,
                anchor.h3;
            continue;
        end if;

        insert into mesh_population_anchor_contract_deleted_h3 (h3)
        values (anchor.h3)
        on conflict do nothing;

        insert into mesh_population_anchor_contract_affected_points (centroid_geog)
        values (anchor.centroid_geog), (replacement.centroid_geog);

        if to_regclass('mesh_tower_wiggle_queue') is not null then
            delete from mesh_tower_wiggle_queue
            where tower_id = anchor.tower_id;
        end if;

        delete from mesh_towers
        where tower_id = anchor.tower_id;

        removed_population := removed_population + 1;

        raise notice 'Contracted population anchor % at % into generated tower % at %',
            anchor.tower_id,
            anchor.h3,
            replacement.tower_id,
            replacement.h3;
    end loop;

    if removed_population = 0 and removed_generated = 0 then
        raise notice 'Population anchor contraction found no contractible anchors';
        return;
    end if;

    -- Keep surface tower flags and distance fields consistent after deletions.
    update mesh_surface_h3_r8 s
    set has_tower = false,
        clearance = null,
        path_loss = null,
        visible_population = null,
        visible_uncovered_population = null
    where exists (
            select 1
            from mesh_population_anchor_contract_deleted_h3 deleted
            where deleted.h3 = s.h3
        )
      and not exists (
            select 1
            from mesh_towers t
            where t.h3 = s.h3
        );

    with affected_cells as materialized (
        select
            s.h3,
            s.centroid_geog
        from mesh_surface_h3_r8 s
        where exists (
            select 1
            from mesh_population_anchor_contract_affected_points affected
            where ST_DWithin(s.centroid_geog, affected.centroid_geog, max_distance)
        )
    ),
    recomputed_distances as (
        select
            ac.h3,
            min(ST_Distance(ac.centroid_geog, t.centroid_geog)) as distance_m
        from affected_cells ac
        join mesh_towers t on true
        group by ac.h3
    )
    update mesh_surface_h3_r8 s
    set distance_to_closest_tower = rd.distance_m,
        clearance = null,
        path_loss = null,
        visible_population = null,
        visible_uncovered_population = case
            when s.has_tower then 0
            else null
        end
    from recomputed_distances rd
    where s.h3 = rd.h3;

    if to_regclass('mesh_tower_wiggle_queue') is not null then
        update mesh_tower_wiggle_queue q
        set is_dirty = true
        from mesh_towers t
        where t.tower_id = q.tower_id
          and t.source = any(target_sources)
          and exists (
                select 1
                from mesh_population_anchor_contract_affected_points affected
                where ST_DWithin(t.centroid_geog, affected.centroid_geog, max_distance)
            );
    end if;

    raise notice 'Population anchor contraction removed % population anchor(s) and % generated tower(s)',
        removed_population,
        removed_generated;
end;
$$;
