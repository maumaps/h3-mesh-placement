set client_min_messages = notice;

drop table if exists pg_temp.mesh_route_manual_redundancy_input;
-- Stage manually reviewed route redundancy anchors before validating and inserting them.
create temporary table mesh_route_manual_redundancy_input (
    h3_text text not null,
    source text not null,
    reason text not null
) on commit drop;

\copy mesh_route_manual_redundancy_input (h3_text, source, reason) from 'data/in/mesh_route_manual_redundancy.csv' with csv header

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
