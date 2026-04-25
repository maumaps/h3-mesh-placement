set client_min_messages = notice;

drop table if exists pg_temp.mesh_route_manual_redundancy_input;
-- Stage manually reviewed route redundancy anchors before validating and inserting them.
create temporary table mesh_route_manual_redundancy_input (
    h3_text text not null,
    source text not null,
    reason text not null
) on commit preserve rows;

insert into mesh_route_manual_redundancy_input (h3_text, source, reason)
values
    ('882c2ab507fffff', 'route', 'backup_83_103_around_100_101_bridge'),
    ('882c2ea889fffff', 'route', 'backup_101_102_around_102_103_bridge'),
    ('882c2ea8d5fffff', 'route', 'backup_101_102_around_102_103_bridge'),
    ('882c21a80dfffff', 'route', 'backup_12_84_around_12_99_bridge'),
    ('882c2e9553fffff', 'route', 'backup_80_103_around_102_cut_node'),
    ('882c2c40a5fffff', 'route', 'backup_80_103_around_102_cut_node'),
    ('882c2a488bfffff', 'route', 'backup_95_97_around_81_95_bridge'),
    ('882c052a03fffff', 'route', 'backup_81_96_around_95_cut_node'),
    ('882c05332dfffff', 'route', 'backup_81_96_around_95_cut_node'),
    ('882c056eddfffff', 'route', 'backup_91_95_around_94_95_bridge'),
    ('882c019365fffff', 'route', 'backup_141_131_around_95_cut_node'),
    ('882c019235fffff', 'route', 'backup_141_131_around_95_cut_node'),
    ('882c05332bfffff', 'route', 'backup_141_131_around_95_cut_node'),
    ('882c215049fffff', 'route', 'backup_30_109_around_12_cut_node'),
    ('882c056ad5fffff', 'route', 'backup_7_94_around_7_91_leaf_bridge'),
    ('882c056ee9fffff', 'route', 'backup_7_94_around_7_91_leaf_bridge'),
    ('882c211303fffff', 'route', 'backup_26_5_around_26_30_leaf_bridge'),
    ('882c2aa885fffff', 'route', 'backup_37_104_around_37_100_leaf_bridge'),
    ('882c0c9227fffff', 'route', 'backup_90_86_around_89_90_leaf_bridge'),
    ('882c214025fffff', 'route', 'backup_current_30_454_cut_bridge'),
    ('882c21431dfffff', 'route', 'backup_current_30_454_cut_bridge'),
    ('882c215183fffff', 'route', 'backup_current_30_454_cut_bridge'),
    ('882c2a4a03fffff', 'route', 'backup_current_382_424_439_cycle'),
    ('882c2a4a0dfffff', 'route', 'backup_current_382_424_439_cycle'),
    ('882c2a4a45fffff', 'route', 'backup_current_382_424_439_cycle'),
    ('882c2b5881fffff', 'route', 'backup_current_383_535_bridge'),
    ('882c2b5899fffff', 'route', 'backup_current_383_535_bridge'),
    ('882c2b58e7fffff', 'route', 'backup_current_383_535_bridge'),
    ('882c2ac891fffff', 'route', 'backup_current_404_578_bridge'),
    ('882c2ac8d3fffff', 'route', 'backup_current_404_578_bridge'),
    ('882c2acf67fffff', 'route', 'backup_current_404_578_bridge'),
    ('882c2129d1fffff', 'route', 'backup_current_444_497_bridge'),
    ('882c2e925dfffff', 'route', 'backup_current_444_497_bridge'),
    ('882c2e92c9fffff', 'route', 'backup_current_444_497_bridge'),
    ('882c052b5dfffff', 'route', 'backup_current_447_578_bridge'),
    ('882c057687fffff', 'route', 'backup_current_447_578_bridge'),
    ('882c057693fffff', 'route', 'backup_current_447_578_bridge'),
    ('882c2145e9fffff', 'route', 'backup_current_454_455_bridge'),
    ('882c214e15fffff', 'route', 'backup_current_454_455_bridge'),
    ('882c214ec9fffff', 'route', 'backup_current_454_455_bridge'),
    ('882c0190d9fffff', 'route', 'backup_current_518_557_bridge'),
    ('882c0192a3fffff', 'route', 'backup_current_518_557_bridge'),
    ('882c01972dfffff', 'route', 'backup_current_518_557_bridge'),
    ('882c056ec3fffff', 'route', 'backup_current_662_663_bridge'),
    ('882c056ec5fffff', 'route', 'backup_current_662_663_bridge'),
    ('882c056ec7fffff', 'route', 'backup_current_662_663_bridge'),
    ('882c2b4a0dfffff', 'route', 'backup_current_383_cut'),
    ('882c2b4a39fffff', 'route', 'backup_current_383_cut'),
    ('882c2b5991fffff', 'route', 'backup_current_383_cut'),
    ('882c2a5a0bfffff', 'route', 'backup_current_439_cut'),
    ('882c2a5a11fffff', 'route', 'backup_current_439_cut'),
    ('882c2a5acdfffff', 'route', 'backup_current_439_cut'),
    ('882c217031fffff', 'route', 'backup_current_454_cut'),
    ('882c21709dfffff', 'route', 'backup_current_454_cut'),
    ('882c21742dfffff', 'route', 'backup_current_454_cut'),
    ('882c284ad7fffff', 'route', 'backup_current_500_cut'),
    ('882c284d37fffff', 'route', 'backup_current_500_cut'),
    ('882c284f27fffff', 'route', 'backup_current_500_cut'),
    ('882c2a24b9fffff', 'route', 'backup_current_535_cut'),
    ('882c2b5121fffff', 'route', 'backup_current_535_cut'),
    ('882c2b5ad5fffff', 'route', 'backup_current_535_cut');

do
$$
declare
    rejected_anchors text;
begin
    with parsed_anchors as (
        -- Parse H3 text once and keep reason text around for operator notices.
        select
            h3_text::h3index as h3,
            source,
            reason
        from mesh_route_manual_redundancy_input
    ), invalid_sources as (
        -- Manual anchors are routed relays; other source labels would bypass current planner assumptions.
        select *
        from parsed_anchors
        where source <> 'route'
    ), missing_surface as (
        -- A manual anchor must exist in the surface table so downstream refreshes can update metrics.
        select parsed_anchors.*
        from parsed_anchors
        left join mesh_surface_h3_r8 surface on surface.h3 = parsed_anchors.h3
        where surface.h3 is null
    ), rejected as (
        -- Stop early with readable rows if the CSV drifts out of the current placement surface.
        select
            'invalid source' as reason,
            h3,
            source
        from invalid_sources

        union all

        select
            'missing surface cell' as reason,
            h3,
            source
        from missing_surface
    )
    select string_agg(
        format('%s:%s:%s', reason, h3::text, source),
        ', '
        order by h3::text
    )
    into rejected_anchors
    from rejected;

    if rejected_anchors is not null then
        raise exception 'Manual route redundancy anchors failed validation: %', rejected_anchors;
    end if;
end;
$$;

with parsed_anchors as (
    -- Re-read staged anchors for idempotent insertion after validation has passed.
    select
        h3_text::h3index as h3,
        source,
        reason
    from mesh_route_manual_redundancy_input
), inserted as (
    insert into mesh_towers (h3, source)
    select
        parsed_anchors.h3,
        parsed_anchors.source
    from parsed_anchors
    on conflict (h3) do nothing
    returning h3
)
select format(
    'Manual route redundancy anchors: %s inserted, %s already present',
    (select count(*) from inserted),
    (
        select count(*)
        from parsed_anchors
        where not exists (
            select 1
            from inserted
            where inserted.h3 = parsed_anchors.h3
        )
    )
) as status;

with parsed_anchors as (
    -- Mark anchor cells as occupied so later placement stages and exports see them immediately.
    select h3_text::h3index as h3
    from mesh_route_manual_redundancy_input
)
update mesh_surface_h3_r8 surface
set has_tower = true,
    distance_to_closest_tower = 0,
    clearance = null,
    path_loss = null,
    visible_population = null,
    visible_uncovered_population = null,
    visible_tower_count = null
from parsed_anchors
where surface.h3 = parsed_anchors.h3;

with parsed_anchors as (
    -- Keep nearest-tower spacing conservative around the manual relay cells.
    select
        surface.h3,
        surface.centroid_geog
    from mesh_route_manual_redundancy_input input
    join mesh_surface_h3_r8 surface on surface.h3 = input.h3_text::h3index
), nearest_anchor as (
    -- Compute affected-cell distances in one pass instead of using row-by-row lateral lookups.
    select
        surface.h3,
        min(ST_Distance(surface.centroid_geog, parsed_anchors.centroid_geog)) as distance_m
    from mesh_surface_h3_r8 surface
    join parsed_anchors
      on ST_DWithin(surface.centroid_geog, parsed_anchors.centroid_geog, 100000)
    group by surface.h3
)
update mesh_surface_h3_r8 surface
set distance_to_closest_tower = coalesce(
        least(
            surface.distance_to_closest_tower,
            nearest_anchor.distance_m
        ),
        nearest_anchor.distance_m
    )
from nearest_anchor
where surface.h3 = nearest_anchor.h3;
