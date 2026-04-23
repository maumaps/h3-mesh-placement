"""
Database-loading helpers for the installer-priority handout export.
"""

from __future__ import annotations

from datetime import datetime

from scripts.install_priority_graph import TowerRecord


def fetch_table_columns(cursor, table_name: str) -> set[str]:
    """Fetch live column names for a public table."""

    # Inspect the current live schema before choosing a geometry strategy.
    query = """
        select column_name
        from information_schema.columns
        where table_schema = 'public'
          and table_name = %s
        order by ordinal_position;
    """
    cursor.execute(query, (table_name,))

    return {row[0] for row in cursor.fetchall()}


def choose_visible_edge_table(cursor, tower_count: int) -> str:
    """Prefer the active visibility table only when it covers the live tower set."""

    # The active table is an optional optimization from some route exports; fall
    # back to the canonical visibility table on fresh pipeline databases.
    cursor.execute("select to_regclass('public.mesh_visibility_edges_active');")
    if cursor.fetchone()[0] is None:
        return "mesh_visibility_edges"

    # Compare active-edge coverage against the full materialized visibility table.
    query = """
        with active_nodes as (
            select source_id as tower_id
            from mesh_visibility_edges_active
            where is_visible
            union
            select target_id as tower_id
            from mesh_visibility_edges_active
            where is_visible
        ),
        full_nodes as (
            select source_id as tower_id
            from mesh_visibility_edges
            where is_visible
            union
            select target_id as tower_id
            from mesh_visibility_edges
            where is_visible
        )
        select
            (select count(*) from active_nodes) as active_node_count,
            (select count(*) from full_nodes) as full_node_count;
    """
    cursor.execute(query)
    active_node_count, full_node_count = cursor.fetchone()

    if active_node_count and active_node_count == full_node_count == tower_count:
        return "mesh_visibility_edges_active"

    return "mesh_visibility_edges"


def fetch_tower_points(cursor) -> dict[int, tuple[float, float]]:
    """Fetch tower points, preferring direct geometry columns when they exist."""

    tower_columns = fetch_table_columns(cursor, "mesh_towers")

    # Use the direct geography/geometry columns when the live table has them.
    if "centroid_geog" in tower_columns:
        query = """
            select
                tower_id,
                ST_X(centroid_geog::geometry) as lon,
                ST_Y(centroid_geog::geometry) as lat
            from mesh_towers;
        """
        cursor.execute(query)

        return {row[0]: (float(row[1]), float(row[2])) for row in cursor.fetchall()}

    if "geom" in tower_columns:
        query = """
            select
                tower_id,
                ST_X(ST_PointOnSurface(geom)) as lon,
                ST_Y(ST_PointOnSurface(geom)) as lat
            from mesh_towers;
        """
        cursor.execute(query)

        return {row[0]: (float(row[1]), float(row[2])) for row in cursor.fetchall()}

    if "h3" in tower_columns:
        # h3-pg exposes direct centroid casts, which are cheaper and safer than
        # rebuilding centroids from boundaries.
        query = """
            select
                tower_id,
                ST_X(h3::geometry) as lon,
                ST_Y(h3::geometry) as lat
            from mesh_towers;
        """
        cursor.execute(query)

        return {row[0]: (float(row[1]), float(row[2])) for row in cursor.fetchall()}

    # Reconstruct points from edge endpoints when the live table stores only ids/source.
    # PostGIS computes the per-tower offset check so live exports do not approximate
    # geography in Python.
    query = """
        with endpoint_observations as (
            select
                source_id as tower_id,
                ST_X(ST_StartPoint(geom)) as lon,
                ST_Y(ST_StartPoint(geom)) as lat
            from mesh_visibility_edges
            union all
            select
                target_id as tower_id,
                ST_X(ST_EndPoint(geom)) as lon,
                ST_Y(ST_EndPoint(geom)) as lat
            from mesh_visibility_edges
        ),
        tower_centroids as (
            select
                tower_id,
                avg(lon) as lon,
                avg(lat) as lat
            from endpoint_observations
            group by tower_id
        )
        select
            centroid.tower_id,
            centroid.lon,
            centroid.lat,
            max(
                ST_Distance(
                    ST_SetSRID(ST_MakePoint(observation.lon, observation.lat), 4326)::geography,
                    ST_SetSRID(ST_MakePoint(centroid.lon, centroid.lat), 4326)::geography
                )
            ) as max_offset_m,
            count(*) as observation_count
        from tower_centroids as centroid
        join endpoint_observations as observation
          on observation.tower_id = centroid.tower_id
        group by
            centroid.tower_id,
            centroid.lon,
            centroid.lat
        order by centroid.tower_id;
    """
    cursor.execute(query)
    tower_points: dict[int, tuple[float, float]] = {}

    for tower_id, lon, lat, max_offset_m, observation_count in cursor.fetchall():
        if float(max_offset_m) > 25.0:
            raise RuntimeError(
                "Tower %s endpoint observations diverge by %.2f m across %s edges; "
                "the export cannot safely reconstruct a single point."
                % (tower_id, float(max_offset_m), int(observation_count))
            )

        tower_points[int(tower_id)] = (float(lon), float(lat))

    if not tower_points:
        raise RuntimeError(
            "No tower endpoint observations were found in mesh_visibility_edges."
        )

    return tower_points


def fetch_tower_metadata(cursor) -> list[tuple[int, str, datetime]]:
    """Fetch tower registry rows."""

    # Load the current tower registry.
    query = """
        select tower_id, source, created_at
        from mesh_towers
        order by tower_id;
    """
    cursor.execute(query)

    return [(int(row[0]), str(row[1]), row[2]) for row in cursor.fetchall()]


def fetch_visible_edges(cursor, table_name: str) -> list[tuple[int, int, float]]:
    """Fetch the visible tower graph used for rollout planning."""

    # Read the visible tower-to-tower graph from the chosen materialized table.
    query = f"""
        select source_id, target_id, distance_m
        from {table_name}
        where is_visible;
    """
    cursor.execute(query)

    return [
        (int(row[0]), int(row[1]), float(row[2]))
        for row in cursor.fetchall()
    ]


def fetch_seed_points(cursor) -> list[tuple[str, float, float]]:
    """Fetch seed node names with point coordinates."""

    seed_columns = fetch_table_columns(cursor, "mesh_initial_nodes_h3_r8")

    # Prefer direct point geometry from the seed table.
    if "geom" in seed_columns:
        query = """
            select
                name,
                ST_X(ST_PointOnSurface(geom)) as lon,
                ST_Y(ST_PointOnSurface(geom)) as lat
            from mesh_initial_nodes_h3_r8;
        """
    elif "h3" in seed_columns:
        query = """
            select
                name,
                ST_X(h3::geometry) as lon,
                ST_Y(h3::geometry) as lat
            from mesh_initial_nodes_h3_r8;
        """
    else:
        raise RuntimeError(
            "mesh_initial_nodes_h3_r8 has neither geom nor h3; seed names cannot be matched."
        )

    cursor.execute(query)

    return [
        (str(row[0]), float(row[1]), float(row[2]))
        for row in cursor.fetchall()
    ]


def match_seed_names(
    cursor,
    seed_tower_ids: list[int],
    tower_points: dict[int, tuple[float, float]],
    seed_points: list[tuple[str, float, float]],
    tolerance_m: float = 1000.0,
) -> dict[int, str]:
    """Match live seed towers to named initial nodes by nearest geometry."""

    if not seed_tower_ids:
        return {}

    if not seed_points:
        raise RuntimeError("No named seed points were available for matching.")

    candidate_match_query = """
        with tower_points(tower_id, lon, lat) as (
            values __TOWER_VALUES__
        ),
        seed_points(seed_name, lon, lat) as (
            values __SEED_VALUES__
        )
        select
            tower_points.tower_id,
            seed_points.seed_name,
            ST_Distance(
                ST_SetSRID(ST_MakePoint(tower_points.lon, tower_points.lat), 4326)::geography,
                ST_SetSRID(ST_MakePoint(seed_points.lon, seed_points.lat), 4326)::geography
            ) as distance_m
        from tower_points
        cross join seed_points
        where ST_DWithin(
            ST_SetSRID(ST_MakePoint(tower_points.lon, tower_points.lat), 4326)::geography,
            ST_SetSRID(ST_MakePoint(seed_points.lon, seed_points.lat), 4326)::geography,
            %s
        )
        order by
            distance_m,
            tower_points.tower_id,
            seed_points.seed_name;
    """
    tower_values_sql = ", ".join("(%s, %s, %s)" for _ in seed_tower_ids)
    tower_params: list[object] = []
    for tower_id in sorted(seed_tower_ids):
        tower_lon, tower_lat = tower_points[tower_id]
        tower_params.extend([tower_id, tower_lon, tower_lat])
    seed_values_sql = ", ".join(
        "(%s, %s, %s)"
        for _seed_name, _seed_lon, _seed_lat in seed_points
    )
    seed_params: list[object] = []
    for seed_name, seed_lon, seed_lat in seed_points:
        seed_params.extend([seed_name, seed_lon, seed_lat])

    cursor.execute(
        candidate_match_query
        .replace("__TOWER_VALUES__", tower_values_sql)
        .replace("__SEED_VALUES__", seed_values_sql),
        [*tower_params, *seed_params, tolerance_m],
    )
    candidate_matches = [
        (float(distance_m), int(tower_id), str(seed_name))
        for tower_id, seed_name, distance_m in cursor.fetchall()
    ]

    seed_name_by_tower_id: dict[int, str] = {}
    used_seed_names: set[str] = set()

    for distance_m, tower_id, seed_name in sorted(candidate_matches):
        if tower_id in seed_name_by_tower_id or seed_name in used_seed_names:
            continue
        if distance_m > tolerance_m:
            continue

        seed_name_by_tower_id[tower_id] = seed_name
        used_seed_names.add(seed_name)

    missing_towers = [
        tower_id for tower_id in seed_tower_ids if tower_id not in seed_name_by_tower_id
    ]
    if missing_towers:
        raise RuntimeError(
            "Could not match every installed seed tower to mesh_initial_nodes_h3_r8. "
            f"Missing tower ids: {missing_towers}"
        )

    return seed_name_by_tower_id


def build_tower_records(
    tower_metadata: list[tuple[int, str, datetime]],
    tower_points: dict[int, tuple[float, float]],
    seed_name_by_tower_id: dict[int, str],
) -> dict[int, TowerRecord]:
    """Build tower records with stable labels."""

    towers_by_id: dict[int, TowerRecord] = {}

    for tower_id, source, created_at in tower_metadata:
        if tower_id not in tower_points:
            raise RuntimeError(
                f"Tower {tower_id} exists in mesh_towers but has no reconstructable point."
            )

        lon, lat = tower_points[tower_id]
        if source == "seed":
            label = seed_name_by_tower_id[tower_id]
        else:
            label = f"{source} #{tower_id}"

        towers_by_id[tower_id] = TowerRecord(
            tower_id=tower_id,
            source=source,
            lon=lon,
            lat=lat,
            label=label,
            installed=(source in {"seed", "mqtt"}),
            created_at=created_at,
        )

    return towers_by_id
