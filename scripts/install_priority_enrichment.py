"""
Location enrichment helpers for the installer-priority handout export.
"""

from __future__ import annotations

from typing import Any

from scripts.install_priority_graph import PlanRow, TowerRecord
from scripts.install_priority_points import _local_distance_m
from scripts.install_priority_render import (
    build_display_name,
    format_connection_labels,
    format_location_description,
    google_maps_url,
    humanize_tower_code,
    osm_url,
)


def prepare_context_tables(cursor) -> None:
    """Create indexed temporary tables for nearest road/place lookups."""

    # Keep one polygon per country so each tower can be assigned to a local field team.
    countries_query = """
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
                end as label_en,
                case
                    when lower(coalesce(tags ->> 'ISO3166-1:alpha2', '')) = 'am' then 'Армения'
                    else 'Грузия'
                end as label_ru,
                ST_Multi(geog::geometry) as geom
            from osm_for_mesh_placement
            where tags ? 'boundary'
              and tags ->> 'boundary' = 'administrative'
              and tags ->> 'admin_level' = '2'
              and ST_GeometryType(geog::geometry) in ('ST_Polygon', 'ST_MultiPolygon')
        )
        select
            country_code,
            label_en,
            label_ru,
            ST_Union(geom) as geom
        from admin_polygons
        where country_code is not null
        group by country_code, label_en, label_ru;
    """
    cursor.execute(countries_query)
    cursor.execute(
        "create index install_priority_countries_geom_idx on install_priority_countries using gist (geom);"
    )
    cursor.execute("analyze install_priority_countries;")

    # Materialize named drivable roads once so per-tower lookups stay fast.
    roads_query = """
        create temporary table install_priority_named_roads on commit drop as
        select
            geog::geometry as geom,
            coalesce(
                nullif(tags ->> 'name:en', ''),
                nullif(tags ->> 'int_name', ''),
                tags ->> 'name'
            ) as label_en,
            coalesce(
                nullif(tags ->> 'name:ru', ''),
                nullif(tags ->> 'name:en', ''),
                nullif(tags ->> 'int_name', ''),
                tags ->> 'name'
            ) as label_ru,
            tags ->> 'highway' as highway
        from osm_for_mesh_placement
        where tags ? 'highway'
          and tags ? 'name'
          and (tags ->> 'highway') = any (
                array[
                    'motorway', 'motorway_link',
                    'trunk', 'trunk_link',
                    'primary', 'primary_link',
                    'secondary', 'secondary_link',
                    'tertiary', 'tertiary_link',
                    'unclassified', 'residential',
                    'living_street', 'service',
                    'road'
                ]
            )
          and ST_GeometryType(geog::geometry) = any (
                array['ST_LineString', 'ST_MultiLineString']
            );
    """
    cursor.execute(roads_query)
    cursor.execute(
        "create index install_priority_named_roads_geom_idx on install_priority_named_roads using gist (geom);"
    )
    cursor.execute("analyze install_priority_named_roads;")

    # Materialize named terrain/place features for mountain-friendly descriptions.
    places_query = """
        create temporary table install_priority_named_places on commit drop as
        select
            geog::geometry as geom,
            coalesce(
                nullif(tags ->> 'name:en', ''),
                nullif(tags ->> 'int_name', ''),
                tags ->> 'name'
            ) as label_en,
            coalesce(
                nullif(tags ->> 'name:ru', ''),
                nullif(tags ->> 'name:en', ''),
                nullif(tags ->> 'int_name', ''),
                tags ->> 'name'
            ) as label_ru,
            coalesce(tags ->> 'place', tags ->> 'natural') as feature_kind
        from osm_for_mesh_placement
        where tags ? 'name'
          and (
                (
                    tags ? 'place'
                    and (tags ->> 'place') = any (
                        array[
                            'city', 'town', 'village', 'hamlet', 'locality',
                            'suburb', 'neighbourhood', 'isolated_dwelling'
                        ]
                    )
                )
                or (
                    tags ? 'natural'
                    and (tags ->> 'natural') = any (
                        array[
                            'peak', 'ridge', 'saddle', 'valley',
                            'hill', 'wood', 'forest'
                        ]
                    )
                )
            );
    """
    cursor.execute(places_query)
    cursor.execute(
        "create index install_priority_named_places_geom_idx on install_priority_named_places using gist (geom);"
    )
    cursor.execute("analyze install_priority_named_places;")

    # Keep populated settlements in a separate indexed table so each tower can get
    # a stable nearby-locality estimate for the handout impact metric.
    populated_places_query = """
        create temporary table install_priority_populated_places on commit drop as
        select
            geog::geometry as geom,
            osm_type || ':' || osm_id::text as place_key,
            coalesce(
                nullif(tags ->> 'name:en', ''),
                nullif(tags ->> 'int_name', ''),
                tags ->> 'name'
            ) as label_en,
            coalesce(
                nullif(tags ->> 'name:ru', ''),
                nullif(tags ->> 'name:en', ''),
                nullif(tags ->> 'int_name', ''),
                tags ->> 'name'
            ) as label_ru,
            nullif(regexp_replace(tags ->> 'population', '[^0-9.]', '', 'g'), '')::numeric as population_est
        from osm_for_mesh_placement
        where tags ? 'place'
          and tags ? 'population'
          and nullif(regexp_replace(tags ->> 'population', '[^0-9.]', '', 'g'), '') is not null
          and (tags ->> 'place') = any (
                array[
                    'city', 'town', 'village', 'hamlet', 'locality',
                    'suburb', 'neighbourhood', 'isolated_dwelling'
                ]
            );
    """
    cursor.execute(populated_places_query)
    cursor.execute(
        "create index install_priority_populated_places_geom_idx on install_priority_populated_places using gist (geom);"
    )
    cursor.execute("analyze install_priority_populated_places;")


def fetch_local_context(cursor, lon: float, lat: float) -> dict[str, Any]:
    """Fetch nearest useful named road and place context from local OSM."""

    point_sql = "ST_SetSRID(ST_MakePoint(%s, %s), 4326)"

    country_query = f"""
        select
            country_code,
            label_en,
            label_ru
        from install_priority_countries
        where ST_Covers(geom, {point_sql})
        order by geom <-> {point_sql}
        limit 1;
    """
    cursor.execute(country_query, (lon, lat, lon, lat))
    country_row = cursor.fetchone() or (None, None, None)

    # Read a small nearest-neighbor window and dedupe names in Python.
    road_query = f"""
        select
            label_en,
            label_ru,
            highway,
            ST_Distance(geom::geography, {point_sql}::geography) as distance_m
        from install_priority_named_roads
        order by geom <-> {point_sql}
        limit 8;
    """
    cursor.execute(road_query, (lon, lat, lon, lat))
    road_rows = cursor.fetchall()

    # Read nearby named places and terrain in the same indexed fashion.
    place_query = f"""
        select
            label_en,
            label_ru,
            feature_kind,
            ST_Distance(geom::geography, {point_sql}::geography) as distance_m
        from install_priority_named_places
        order by geom <-> {point_sql}
        limit 8;
    """
    cursor.execute(place_query, (lon, lat, lon, lat))
    place_rows = cursor.fetchall()

    # Anchor each tower to one nearby populated locality so rollout impact can be
    # expressed in estimated reachable people instead of only unlocked tower counts.
    populated_place_query = f"""
        select
            place_key,
            label_en,
            label_ru,
            population_est,
            ST_Distance(geom::geography, {point_sql}::geography) as distance_m
        from install_priority_populated_places
        order by geom <-> {point_sql}
        limit 8;
    """
    cursor.execute(populated_place_query, (lon, lat, lon, lat))
    populated_place_rows = cursor.fetchall()

    road_context = _pick_unique_named_feature(road_rows, distance_limit_m=5000.0)
    place_context = _pick_unique_named_feature(place_rows, distance_limit_m=30000.0)
    populated_place_context = _pick_population_place(
        populated_place_rows,
        distance_limit_m=35000.0,
    )

    return {
        "country_code": country_row[0],
        "country_en": country_row[1],
        "country_ru": country_row[2],
        "road_en": road_context.get("label_en"),
        "road_ru": road_context.get("label_ru"),
        "road_distance_m": road_context.get("distance_m"),
        "place_en": place_context.get("label_en"),
        "place_ru": place_context.get("label_ru"),
        "place_distance_m": place_context.get("distance_m"),
        "population_place_id": populated_place_context.get("place_key"),
        "population_place_en": populated_place_context.get("label_en"),
        "population_place_ru": populated_place_context.get("label_ru"),
        "population_est": populated_place_context.get("population_est"),
        "population_place_distance_m": populated_place_context.get("distance_m"),
    }


def fetch_reachable_seed_mqtt_overview(
    cursor,
    visible_edge_table: str,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    """Fetch reachable seed/MQTT overview points and every direct link they have."""

    del visible_edge_table

    overview_points_query = """
        with params as (
            select
                coalesce((
                    select value::double precision
                    from mesh_pipeline_settings
                    where setting = 'max_los_distance_m'
                ), 100000) as max_distance_m,
                coalesce((
                    select value::double precision
                    from mesh_pipeline_settings
                    where setting = 'mast_height_m'
                ), 28) as mast_height_m,
                coalesce((
                    select value::double precision
                    from mesh_pipeline_settings
                    where setting = 'frequency_hz'
                ), 868000000) as frequency_hz
        ),
        country_filtered_points as (
            select
                mesh_initial_nodes_h3_r8.h3,
                mesh_initial_nodes_h3_r8.name,
                coalesce(mesh_initial_nodes_h3_r8.source, 'seed') as source,
                ST_X(mesh_initial_nodes_h3_r8.h3::geometry) as lon,
                ST_Y(mesh_initial_nodes_h3_r8.h3::geometry) as lat,
                install_priority_countries.country_code,
                install_priority_countries.label_en as country_name
            from mesh_initial_nodes_h3_r8
            join lateral (
                select
                    install_priority_countries.country_code,
                    install_priority_countries.label_en,
                    install_priority_countries.geom
                from install_priority_countries
                where ST_DWithin(
                    mesh_initial_nodes_h3_r8.h3::geography,
                    install_priority_countries.geom::geography,
                    100000
                )
                order by
                    ST_Distance(
                        mesh_initial_nodes_h3_r8.h3::geography,
                        install_priority_countries.geom::geography
                    ),
                    install_priority_countries.country_code
                limit 1
            ) as install_priority_countries on true
            where coalesce(mesh_initial_nodes_h3_r8.source, 'seed') in ('seed', 'mqtt')
        )
        select
            country_filtered_points.h3::text,
            country_filtered_points.name,
            country_filtered_points.source,
            country_filtered_points.lon,
            country_filtered_points.lat,
            country_filtered_points.country_code,
            country_filtered_points.country_name
        from country_filtered_points
        where exists (
            select 1
            from mesh_towers
            join params on true
            join mesh_los_cache on mesh_los_cache.clearance > 0
                and mesh_los_cache.distance_m <= params.max_distance_m
                and mesh_los_cache.mast_height_src = params.mast_height_m
                and mesh_los_cache.mast_height_dst = params.mast_height_m
                and mesh_los_cache.frequency_hz = params.frequency_hz
                and (
                    (
                        mesh_los_cache.src_h3 = country_filtered_points.h3
                        and mesh_los_cache.dst_h3 = mesh_towers.h3
                    )
                    or (
                        mesh_los_cache.dst_h3 = country_filtered_points.h3
                        and mesh_los_cache.src_h3 = mesh_towers.h3
                    )
                )
        )
        order by
            country_filtered_points.source,
            country_filtered_points.country_code,
            country_filtered_points.name;
    """
    cursor.execute(overview_points_query)
    point_rows = cursor.fetchall()
    points = [
        {
            "h3": str(h3),
            "name": str(name or "Seed node"),
            "source": str(source),
            "marker": "m" if str(source) == "mqtt" else "s",
            "lon": float(lon),
            "lat": float(lat),
            "country_code": str(country_code or ""),
            "country_name": str(country_name or ""),
        }
        for h3, name, source, lon, lat, country_code, country_name in point_rows
    ]
    points = _prefer_seed_points_over_nearby_mqtt(points)

    if not points:
        return [], []

    h3_values_sql = ", ".join(["%s"] * len(points))
    overview_links_query = f"""
        with params as (
            select
                coalesce((
                    select value::double precision
                    from mesh_pipeline_settings
                    where setting = 'max_los_distance_m'
                ), 100000) as max_distance_m,
                coalesce((
                    select value::double precision
                    from mesh_pipeline_settings
                    where setting = 'mast_height_m'
                ), 28) as mast_height_m,
                coalesce((
                    select value::double precision
                    from mesh_pipeline_settings
                    where setting = 'frequency_hz'
                ), 868000000) as frequency_hz
        ),
        relevant_points as (
            select h3
            from mesh_initial_nodes_h3_r8
            where h3::text in ({h3_values_sql})
        ),
        relevant_links as (
            select distinct
                mesh_los_cache.src_h3,
                mesh_los_cache.dst_h3
            from mesh_los_cache
            join params on true
            where mesh_los_cache.clearance > 0
              and mesh_los_cache.distance_m <= params.max_distance_m
              and mesh_los_cache.mast_height_src = params.mast_height_m
              and mesh_los_cache.mast_height_dst = params.mast_height_m
              and mesh_los_cache.frequency_hz = params.frequency_hz
              and (
                    mesh_los_cache.src_h3 in (select h3 from relevant_points)
                    or mesh_los_cache.dst_h3 in (select h3 from relevant_points)
                )
              and (
                    mesh_los_cache.src_h3 in (select h3 from relevant_points)
                    or mesh_los_cache.src_h3 in (select h3 from mesh_towers)
                )
              and (
                    mesh_los_cache.dst_h3 in (select h3 from relevant_points)
                    or mesh_los_cache.dst_h3 in (select h3 from mesh_towers)
                )
        )
        select
            ST_AsGeoJSON(
                ST_MakeLine(
                    direct_links.source_h3::geometry,
                    direct_links.target_h3::geometry
                )
            ),
            direct_links.source_h3::text,
            direct_links.target_h3::text
        from (
            select
                least(relevant_links.src_h3, relevant_links.dst_h3) as source_h3,
                greatest(relevant_links.src_h3, relevant_links.dst_h3) as target_h3
            from relevant_links
            group by
                least(relevant_links.src_h3, relevant_links.dst_h3),
                greatest(relevant_links.src_h3, relevant_links.dst_h3)
        ) as direct_links
        order by
            source_h3::text,
            target_h3::text;
    """
    h3_params = [point["h3"] for point in points]
    cursor.execute(overview_links_query, h3_params)
    links = [
        {
            "geometry": geojson_text,
            "source_h3": str(source_h3),
            "target_h3": str(target_h3),
        }
        for geojson_text, source_h3, target_h3 in cursor.fetchall()
    ]

    return points, links


def _prefer_seed_points_over_nearby_mqtt(
    points: list[dict[str, object]],
    tolerance_m: float = 1000.0,
) -> list[dict[str, object]]:
    """Drop duplicate overview MQTT points near a seed or earlier MQTT point."""

    seed_points = [
        point
        for point in points
        if str(point.get("source")) == "seed"
    ]

    filtered_points: list[dict[str, object]] = []
    kept_mqtt_points: list[dict[str, object]] = []

    for point in points:
        if str(point.get("source")) != "mqtt":
            filtered_points.append(point)
            continue

        point_lon = float(point["lon"])
        point_lat = float(point["lat"])
        has_nearby_seed = any(
            _local_distance_m(
                point_lon,
                point_lat,
                float(seed_point["lon"]),
                float(seed_point["lat"]),
            ) <= tolerance_m
            for seed_point in seed_points
        )
        has_nearby_mqtt = any(
            _local_distance_m(
                point_lon,
                point_lat,
                float(mqtt_point["lon"]),
                float(mqtt_point["lat"]),
            ) <= tolerance_m
            for mqtt_point in kept_mqtt_points
        )
        if has_nearby_seed or has_nearby_mqtt:
            continue

        filtered_points.append(point)
        kept_mqtt_points.append(point)

    return filtered_points


def _pick_unique_named_feature(
    rows: list[tuple[Any, ...]],
    distance_limit_m: float,
) -> dict[str, Any]:
    """Pick the first distinct named feature within a reasonable distance."""

    seen_pairs: set[tuple[str, str]] = set()

    for row in rows:
        label_en = (row[0] or "").strip()
        label_ru = (row[1] or "").strip()
        distance_m = float(row[3])
        name_pair = (label_en, label_ru)

        if not label_en and not label_ru:
            continue
        if distance_m > distance_limit_m:
            continue
        if name_pair in seen_pairs:
            continue

        seen_pairs.add(name_pair)
        return {
            "label_en": label_en or label_ru,
            "label_ru": label_ru or label_en,
            "distance_m": distance_m,
        }

    return {}


def _pick_population_place(
    rows: list[tuple[Any, ...]],
    distance_limit_m: float,
) -> dict[str, Any]:
    """Pick the nearest populated place that can seed a people estimate."""

    for row in rows:
        place_key = (row[0] or "").strip()
        label_en = (row[1] or "").strip()
        label_ru = (row[2] or "").strip()
        population_est = float(row[3]) if row[3] is not None else 0.0
        distance_m = float(row[4])

        if not place_key:
            continue
        if population_est <= 0:
            continue
        if distance_m > distance_limit_m:
            continue

        return {
            "place_key": place_key,
            "label_en": label_en or label_ru,
            "label_ru": label_ru or label_en,
            "population_est": population_est,
            "distance_m": distance_m,
        }

    return {}


def infer_location_status(
    geocoder_status_en: str,
    geocoder_status_ru: str,
    admin_context_en: dict[str, str | None],
    local_context: dict[str, Any],
) -> str:
    """Describe how much external enrichment landed for a row."""

    if geocoder_status_en == "ok" and geocoder_status_ru == "ok":
        if any(admin_context_en.values()):
            return "ok"
        if local_context.get("road_en") or local_context.get("place_en"):
            return "osm_only"
        return "sparse"

    if geocoder_status_en == "ok" or geocoder_status_ru == "ok":
        return "partial"

    if local_context.get("road_en") or local_context.get("place_en"):
        return "geocoder_failed"

    return "fallback_coords"


def build_output_row(
    plan_row: PlanRow,
    towers_by_id: dict[int, TowerRecord],
    local_context: dict[str, Any],
    admin_context_en: dict[str, str | None],
    admin_context_ru: dict[str, str | None],
    geocoder_status_en: str,
    geocoder_status_ru: str,
) -> dict[str, object]:
    """Combine graph planning with location enrichment into one flat row."""

    tower = towers_by_id[plan_row.tower_id]
    display_name = tower.display_name or plan_row.label
    display_type = tower.display_code or humanize_tower_code(
        plan_row.source,
        plan_row.tower_id,
        plan_row.installed,
    )
    admin_context_en_with_country = dict(admin_context_en)
    admin_context_ru_with_country = dict(admin_context_ru)

    if not admin_context_en_with_country.get("country") and local_context.get("country_en"):
        admin_context_en_with_country["country"] = local_context.get("country_en")

    if not admin_context_ru_with_country.get("country") and local_context.get("country_ru"):
        admin_context_ru_with_country["country"] = local_context.get("country_ru")

    return {
        "cluster_key": plan_row.cluster_key,
        "cluster_label": plan_row.cluster_label,
        "cluster_install_rank": (
            ""
            if plan_row.cluster_install_rank is None
            else plan_row.cluster_install_rank
        ),
        "is_next_for_cluster": "true" if plan_row.is_next_for_cluster else "false",
        "rollout_status": plan_row.rollout_status,
        "installed": "true" if plan_row.installed else "false",
        "tower_id": plan_row.tower_id,
        "label": plan_row.label,
        "display_name": display_name,
        "display_type": display_type,
        "source": plan_row.source,
        "impact_score": plan_row.impact_score,
        "impact_people_est": plan_row.impact_score,
        "impact_tower_count": plan_row.impact_tower_count,
        "next_unlock_count": plan_row.next_unlock_count,
        "backlink_count": plan_row.backlink_count,
        "primary_previous_tower_id": (
            ""
            if not plan_row.previous_connection_ids
            else plan_row.previous_connection_ids[0]
        ),
        "inter_cluster_neighbor_ids": "",
        "inter_cluster_connections": "",
        "blocked_reason": "",
        "previous_connections": format_connection_labels(
            plan_row.previous_connection_ids,
            towers_by_id,
        ),
        "next_connections": format_connection_labels(
            plan_row.next_connection_ids,
            towers_by_id,
        ),
        "lon": f"{plan_row.lon:.6f}",
        "lat": f"{plan_row.lat:.6f}",
        "country_code": local_context.get("country_code") or "",
        "country_name": local_context.get("country_en") or "",
        "location_status": infer_location_status(
            geocoder_status_en=geocoder_status_en,
            geocoder_status_ru=geocoder_status_ru,
            admin_context_en=admin_context_en,
            local_context=local_context,
        ),
        "location_en": format_location_description(
            locale="en",
            road_name=local_context.get("road_en"),
            place_name=local_context.get("place_en"),
            admin_context=admin_context_en_with_country,
            lon=plan_row.lon,
            lat=plan_row.lat,
        ),
        "location_ru": format_location_description(
            locale="ru",
            road_name=local_context.get("road_ru"),
            place_name=local_context.get("place_ru"),
            admin_context=admin_context_ru_with_country,
            lon=plan_row.lon,
            lat=plan_row.lat,
        ),
        "google_maps_url": google_maps_url(plan_row.lon, plan_row.lat),
        "osm_url": osm_url(plan_row.lon, plan_row.lat),
    }


def enrich_tower_records(
    towers_by_id: dict[int, TowerRecord],
    local_context_by_tower_id: dict[int, dict[str, Any]],
) -> dict[int, TowerRecord]:
    """Attach human display names and people estimates to tower records."""

    enriched_towers: dict[int, TowerRecord] = {}

    for tower_id, tower in towers_by_id.items():
        local_context = local_context_by_tower_id[tower_id]
        display_name = build_display_name(
            tower=tower,
            place_name=local_context.get("place_en") or local_context.get("population_place_en"),
            road_name=local_context.get("road_en"),
        )
        display_code = humanize_tower_code(
            tower.source,
            tower.tower_id,
            tower.installed,
        )
        enriched_towers[tower_id] = TowerRecord(
            tower_id=tower.tower_id,
            source=tower.source,
            lon=tower.lon,
            lat=tower.lat,
            label=tower.label,
            installed=tower.installed,
            created_at=tower.created_at,
            people_estimate=float(local_context.get("population_est") or 0.0),
            population_place_id=local_context.get("population_place_id"),
            population_place_name=local_context.get("population_place_en"),
            display_name=display_name,
            display_code=display_code,
            country_code=local_context.get("country_code"),
            country_name=local_context.get("country_en"),
        )

    return enriched_towers
