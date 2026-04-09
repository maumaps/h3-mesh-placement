"""
Cluster-bound geometry helpers for the installer-priority handout.
"""

from __future__ import annotations

import json
from typing import Mapping, Sequence


MIN_CLUSTER_BOUND_RADIUS_M = 1000.0


def fetch_cluster_bound_features(
    cursor,
    rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Build Voronoi-based cluster polygons and merge them by cluster id."""

    cluster_rows = [
        row
        for row in rows
        if row.get("cluster_key") not in (None, "")
    ]
    unique_points = {
        (
            str(row["cluster_key"]),
            str(row["cluster_label"]),
            int(row["tower_id"]),
            float(row["lon"]),
            float(row["lat"]),
        )
        for row in cluster_rows
    }

    if not unique_points:
        return []

    if len(unique_points) == 1:
        cluster_key, cluster_label, _tower_id, lon, lat = next(iter(unique_points))

        return _fetch_single_point_feature(
            cursor=cursor,
            cluster_key=cluster_key,
            cluster_label=cluster_label,
            lon=lon,
            lat=lat,
        )

    ordered_points = sorted(unique_points)
    values_sql = ", ".join("(%s, %s, %s, %s, %s)" for _ in ordered_points)
    params: list[object] = []

    for cluster_key, cluster_label, tower_id, lon, lat in ordered_points:
        params.extend([tower_id, cluster_key, cluster_label, lon, lat])

    # Build one Voronoi cell per tower point, clip the cells to a geodesic
    # buffer around the real point cloud, then merge neighboring cells by cluster id.
    query = f"""
        with tower_points_input(tower_id, cluster_key, cluster_label, lon, lat) as (
            -- Feed all handout tower points into PostGIS as one values table.
            values {values_sql}
        ),
        tower_points as (
            -- Turn the numeric lon/lat inputs into geometry once for reuse.
            select
                tower_id,
                cluster_key,
                cluster_label,
                lon,
                lat,
                ST_SetSRID(ST_MakePoint(lon, lat), 4326) as geom
            from tower_points_input
        ),
        nearest_neighbor_radius as (
            -- Use the widest nearest-neighbor spacing as the outer geodesic clip radius.
            select
                greatest(
                    coalesce(
                        max(
                            (
                                select ST_Distance(
                                    tower_points.geom::geography,
                                    other_points.geom::geography
                                )
                                from tower_points as other_points
                                where other_points.tower_id <> tower_points.tower_id
                                order by tower_points.geom <-> other_points.geom
                                limit 1
                            )
                        ),
                        0.0
                    ),
                    %s
                ) as max_radius_m
            from tower_points
        ),
        clip_mask as (
            -- Buffer the full point cloud in real meters rather than degree padding.
            select
                ST_Buffer(
                    (select ST_Collect(geom) from tower_points)::geography,
                    (select max_radius_m from nearest_neighbor_radius)
                )::geometry as geom
        ),
        clip_extent as (
            -- Voronoi generation accepts an extent envelope, so clip to the mask later.
            select ST_Envelope(geom) as geom
            from clip_mask
        ),
        raw_voronoi_cells as (
            -- Generate the finite Voronoi cells for the full tower set.
            select
                (ST_Dump(
                    ST_VoronoiPolygons(
                        (select ST_Collect(geom) from tower_points),
                        0.0,
                        (select geom from clip_extent)
                    )
                )).geom as geom
        ),
        clipped_voronoi_cells as (
            -- Clip the Voronoi cells to the true geodesic buffer mask.
            select
                ST_Intersection(
                    raw_voronoi_cells.geom,
                    (select geom from clip_mask)
                ) as geom
            from raw_voronoi_cells
            where not ST_IsEmpty(
                ST_Intersection(
                    raw_voronoi_cells.geom,
                    (select geom from clip_mask)
                )
            )
        ),
        tower_cells as (
            -- Reattach each clipped cell to the tower point it contains.
            select
                tower_points.tower_id,
                tower_points.cluster_key,
                tower_points.cluster_label,
                cell.geom
            from tower_points
            join lateral (
                select clipped_voronoi_cells.geom
                from clipped_voronoi_cells
                where ST_Covers(clipped_voronoi_cells.geom, tower_points.geom)
                order by ST_Area(clipped_voronoi_cells.geom)
                limit 1
            ) as cell
              on true
        ),
        cluster_polygons as (
            -- Merge same-cluster cells into one multi-part cluster outline.
            select
                cluster_key,
                min(cluster_label) as cluster_label,
                ST_Union(geom) as geom
            from tower_cells
            group by cluster_key
        )
        select
            cluster_key,
            cluster_label,
            ST_AsGeoJSON(geom, 6)::text as geometry_json
        from cluster_polygons
        order by cluster_label, cluster_key;
    """
    cursor.execute(query, [*params, MIN_CLUSTER_BOUND_RADIUS_M])

    return [
        {
            "type": "Feature",
            "geometry": json.loads(geometry_json),
            "properties": {
                "cluster_key": str(cluster_key),
                "cluster_label": str(cluster_label),
            },
        }
        for cluster_key, cluster_label, geometry_json in cursor.fetchall()
    ]


def _fetch_single_point_feature(
    *,
    cursor,
    cluster_key: str,
    cluster_label: str,
    lon: float,
    lat: float,
) -> list[dict[str, object]]:
    """Build one geodesic buffer polygon when only one tower exists."""

    query = """
        select ST_AsGeoJSON(
            ST_Buffer(
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                %s
            )::geometry,
            6
        )::text;
    """
    cursor.execute(query, [lon, lat, MIN_CLUSTER_BOUND_RADIUS_M])
    geometry_json = cursor.fetchone()[0]

    return [
        {
            "type": "Feature",
            "geometry": json.loads(geometry_json),
            "properties": {
                "cluster_key": cluster_key,
                "cluster_label": cluster_label,
            },
        }
    ]
