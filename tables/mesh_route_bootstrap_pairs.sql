set client_min_messages = warning;

drop table if exists mesh_route_bootstrap_pairs;
-- Store install-priority-derived LOS pairs so routing can bootstrap along
-- exported rollout points before generic pair expansion.
create table mesh_route_bootstrap_pairs (
    src_h3 h3index not null,
    dst_h3 h3index not null,
    src_point_id text not null,
    dst_point_id text not null,
    src_point_kind text not null,
    dst_point_kind text not null,
    bootstrap_rank integer not null,
    distance_m double precision not null,
    primary key (src_h3, dst_h3)
);

comment on table mesh_route_bootstrap_pairs is
    'Canonical H3 LOS pairs reconstructed from installer CSV rows, manual warmup points, nearest placeable OSM peaks, and disconnected coarse-cluster tower links so fill_mesh_los_cache can prioritize route bootstrap corridors.';

drop table if exists mesh_route_bootstrap_csv;
-- Load the install-priority export rows into a staging table so SQL can turn
-- CSV points into route-bootstrap H3 pairs.
create temporary table mesh_route_bootstrap_csv (
    cluster_key text,
    cluster_label text,
    cluster_install_rank text,
    is_next_for_cluster text,
    rollout_status text,
    installed text,
    tower_id text,
    label text,
    display_name text,
    display_type text,
    source text,
    impact_score text,
    impact_people_est text,
    impact_tower_count text,
    next_unlock_count text,
    backlink_count text,
    primary_previous_tower_id text,
    inter_cluster_neighbor_ids text,
    inter_cluster_connections text,
    blocked_reason text,
    previous_connections text,
    next_connections text,
    lon text,
    lat text,
    location_status text,
    location_en text,
    location_ru text,
    google_maps_url text,
    osm_url text
);

\copy mesh_route_bootstrap_csv from 'data/in/install_priority_bootstrap.csv' with (format csv, header true)

drop table if exists mesh_route_bootstrap_manual_csv;
-- Load manually curated warmup points that should always participate in the
-- bootstrap cache seed even when they are not part of the installer export.
create temporary table mesh_route_bootstrap_manual_csv (
    point_id text,
    label text,
    lon text,
    lat text,
    priority_group text
);

\copy mesh_route_bootstrap_manual_csv from 'data/in/install_priority_bootstrap_manual.csv' with (format csv, header true)

-- Build canonical bootstrap points from three point sources:
-- 1. installer export coordinates that already land on the planning surface,
-- 2. manually curated warmup points snapped to the nearest placeable cell,
-- 3. every OSM peak snapped to the nearest placeable cell so mountain-top
--    relays are warmed in cache before generic route expansion.
-- Additional targeted disconnected-cluster links are added later as explicit
-- H3 pairs, because those links should aim at already placed towers directly.
with parsed_csv_rows as (
    select
        'tower:' || tower_id as point_id,
        'install_priority_csv'::text as point_kind,
        200000 as priority_group,
        h3_latlng_to_cell(
            ST_SetSRID(
                ST_MakePoint(lon::double precision, lat::double precision),
                4326
            ),
            8
        ) as h3
    from mesh_route_bootstrap_csv
    where nullif(tower_id, '') is not null
      and nullif(lat, '') is not null
      and nullif(lon, '') is not null
),
csv_surface_points as (
    select
        pr.point_id,
        pr.point_kind,
        pr.priority_group,
        pr.h3
    from parsed_csv_rows pr
    join mesh_surface_h3_r8 surface on surface.h3 = pr.h3
),
manual_points as (
    select
        manual.point_id,
        'manual_point'::text as point_kind,
        coalesce(nullif(manual.priority_group, '')::integer, 0) as priority_group,
        snapped.h3
    from mesh_route_bootstrap_manual_csv manual
    join lateral (
        select
            surface.h3
        from mesh_surface_h3_r8 surface
        where surface.can_place_tower
        order by surface.centroid_geog <-> ST_SetSRID(
            ST_MakePoint(manual.lon::double precision, manual.lat::double precision),
            4326
        )::geography
        limit 1
    ) snapped on true
    where nullif(manual.point_id, '') is not null
      and nullif(manual.lat, '') is not null
      and nullif(manual.lon, '') is not null
),
peak_points as (
    select distinct on (snapped.h3)
        'peak:' || peak.osm_id as point_id,
        'peak_nearest_placeable'::text as point_kind,
        50000 as priority_group,
        snapped.h3
    from osm_for_mesh_placement peak
    join georgia_boundary boundary
      on ST_Intersects(peak.geog::geometry, boundary.geom)
    join lateral (
        select
            surface.h3
        from mesh_surface_h3_r8 surface
        where surface.can_place_tower
        order by surface.centroid_geog <-> peak.geog
        limit 1
    ) snapped on true
    where peak.tags @> '{"natural":"peak"}'
    order by snapped.h3, peak.osm_id
),
placed_tower_points as (
    select
        'placed:' || tower.tower_id as point_id,
        'placed_tower'::text as point_kind,
        10000 as priority_group,
        tower.h3
    from mesh_towers tower
    join mesh_surface_h3_r8 surface on surface.h3 = tower.h3
),
bootstrap_points as (
    select *
    from csv_surface_points
    union all
    select *
    from placed_tower_points
    union all
    select *
    from manual_points
    union all
    select *
    from peak_points
),
surface_pairs as (
    select
        least(src.h3, dst.h3) as src_h3,
        greatest(src.h3, dst.h3) as dst_h3,
        case
            when src.h3 <= dst.h3 then src.point_id
            else dst.point_id
        end as src_point_id,
        case
            when src.h3 <= dst.h3 then dst.point_id
            else src.point_id
        end as dst_point_id,
        case
            when src.h3 <= dst.h3 then src.point_kind
            else dst.point_kind
        end as src_point_kind,
        case
            when src.h3 <= dst.h3 then dst.point_kind
            else src.point_kind
        end as dst_point_kind,
        least(src.priority_group, dst.priority_group)
            + floor(
                ST_Distance(
                    least(src.h3, dst.h3)::geography,
                    greatest(src.h3, dst.h3)::geography
                )
            )::integer as bootstrap_rank,
        ST_Distance(
            least(src.h3, dst.h3)::geography,
            greatest(src.h3, dst.h3)::geography
        ) as distance_m
    from bootstrap_points src
    join bootstrap_points dst
      on dst.point_id > src.point_id
    where src.h3 <> dst.h3
),
-- Add explicit inter-cluster tower links for disconnected coarse components so
-- cache warmup seeds the shortest real tower corridors toward already placed mesh towers.
disconnected_coarse_pairs as (
    select distinct on (coarse.cluster_id, other.cluster_id)
        least(coarse.h3, other.h3) as src_h3,
        greatest(coarse.h3, other.h3) as dst_h3,
        case
            when coarse.h3 <= other.h3 then 'coarse-cluster:' || coarse.cluster_id
            else other.point_id
        end as src_point_id,
        case
            when coarse.h3 <= other.h3 then other.point_id
            else 'coarse-cluster:' || coarse.cluster_id
        end as dst_point_id,
        case
            when coarse.h3 <= other.h3 then 'coarse_cluster_link'::text
            else other.point_kind
        end as src_point_kind,
        case
            when coarse.h3 <= other.h3 then other.point_kind
            else 'coarse_cluster_link'::text
        end as dst_point_kind,
        1000
            + floor(ST_Distance(coarse.h3::geography, other.h3::geography))::integer as bootstrap_rank,
        ST_Distance(coarse.h3::geography, other.h3::geography) as distance_m
    from (
        select
            t.tower_id,
            t.h3,
            clusters.cluster_id
        from mesh_towers t
        join mesh_tower_clusters() clusters
          on clusters.tower_id = t.tower_id
        where t.source = 'coarse'
    ) coarse
    join lateral (
        select
            other_tower.h3,
            other_tower.source,
            other_clusters.cluster_id,
            'placed:' || other_tower.tower_id as point_id,
            'placed_tower'::text as point_kind
        from mesh_towers other_tower
        join mesh_tower_clusters() other_clusters
          on other_clusters.tower_id = other_tower.tower_id
        where other_clusters.cluster_id <> coarse.cluster_id
        order by coarse.h3::geography <-> other_tower.h3::geography
        limit 6
    ) other on true
    where ST_DWithin(coarse.h3::geography, other.h3::geography, 100000)
    order by coarse.cluster_id, other.cluster_id, distance_m, src_h3, dst_h3
),
bootstrap_pairs as (
    select *
    from surface_pairs

    union all

    select *
    from disconnected_coarse_pairs
)
insert into mesh_route_bootstrap_pairs (
    src_h3,
    dst_h3,
    src_point_id,
    dst_point_id,
    src_point_kind,
    dst_point_kind,
    bootstrap_rank,
    distance_m
)
select distinct on (
    bootstrap_pairs.src_h3,
    bootstrap_pairs.dst_h3
)
    bootstrap_pairs.src_h3,
    bootstrap_pairs.dst_h3,
    bootstrap_pairs.src_point_id,
    bootstrap_pairs.dst_point_id,
    bootstrap_pairs.src_point_kind,
    bootstrap_pairs.dst_point_kind,
    bootstrap_pairs.bootstrap_rank,
    bootstrap_pairs.distance_m
from bootstrap_pairs
where bootstrap_pairs.distance_m <= 100000
order by
    bootstrap_pairs.src_h3,
    bootstrap_pairs.dst_h3,
    bootstrap_pairs.bootstrap_rank,
    bootstrap_pairs.src_point_id,
    bootstrap_pairs.dst_point_id;

create index if not exists mesh_route_bootstrap_pairs_src_dst_idx
    on mesh_route_bootstrap_pairs (src_h3, dst_h3);

create index if not exists mesh_route_bootstrap_pairs_rank_brin
    on mesh_route_bootstrap_pairs using brin (bootstrap_rank, distance_m);
