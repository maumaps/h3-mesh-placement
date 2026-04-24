"""
Payload-shaping helpers for the installer-priority MapLibre HTML.
"""

from __future__ import annotations

import re
from typing import Mapping, Sequence


def dedupe_clusters(
    normalized_rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Keep one cluster metadata row per cluster key for the map payload."""

    connect_max_rank_by_cluster = _connect_max_rank_by_cluster(normalized_rows)
    deduped_clusters: list[dict[str, object]] = []
    seen_cluster_keys: set[str] = set()

    for row in normalized_rows:
        cluster_key = str(row["cluster_key"])
        if cluster_key in seen_cluster_keys:
            continue
        seen_cluster_keys.add(cluster_key)
        map_id = _cluster_map_id(cluster_key)
        deduped_clusters.append(
            {
                "cluster_key": cluster_key,
                "cluster_label": str(row["cluster_label"]),
                "map_id": map_id,
                "full_map_id": f"{map_id}-full",
                "connect_max_rank": connect_max_rank_by_cluster[cluster_key],
            }
        )

    return deduped_clusters


def _connect_max_rank_by_cluster(
    normalized_rows: Sequence[Mapping[str, object]],
) -> dict[str, int]:
    """Find the last rank needed to touch every planned neighboring cluster."""

    rows_by_tower_id = {
        int(row["tower_id"]): row
        for row in normalized_rows
    }
    planned_ranks_by_cluster: dict[str, list[int]] = {}
    connector_ranks_by_cluster: dict[str, dict[str, int]] = {}

    for row in normalized_rows:
        cluster_key = str(row["cluster_key"])
        rank = _optional_rank(row.get("cluster_install_rank"))

        if rank is not None and not bool(row.get("installed")):
            planned_ranks_by_cluster.setdefault(cluster_key, []).append(rank)

        if rank is None or bool(row.get("installed")):
            continue

        for neighbor_id in row.get("inter_cluster_neighbor_ids") or []:
            neighbor_row = rows_by_tower_id.get(int(neighbor_id))
            if not neighbor_row:
                continue

            neighbor_cluster_key = str(neighbor_row["cluster_key"])
            if neighbor_cluster_key == cluster_key:
                continue

            connector_ranks = connector_ranks_by_cluster.setdefault(
                cluster_key,
                {},
            )
            connector_ranks[neighbor_cluster_key] = max(
                rank,
                connector_ranks.get(neighbor_cluster_key, -1),
            )

    connect_max_rank_by_cluster: dict[str, int] = {}
    for row in normalized_rows:
        cluster_key = str(row["cluster_key"])
        if cluster_key in connect_max_rank_by_cluster:
            continue

        connector_ranks = connector_ranks_by_cluster.get(cluster_key, {})
        planned_ranks = planned_ranks_by_cluster.get(cluster_key, [])
        if connector_ranks:
            connect_max_rank_by_cluster[cluster_key] = max(connector_ranks.values())
        elif planned_ranks:
            connect_max_rank_by_cluster[cluster_key] = min(planned_ranks)
        else:
            connect_max_rank_by_cluster[cluster_key] = 0

    return connect_max_rank_by_cluster


def _optional_rank(value: object) -> int | None:
    """Normalize optional rank values from CSV or JSON-shaped rows."""

    if value in (None, ""):
        return None

    return int(value)


def _cluster_map_id(cluster_key: str) -> str:
    """Mirror the DOM id format used by the MapLibre renderer."""

    slug = re.sub(r"[^a-z0-9]+", "-", cluster_key.lower())

    return f"cluster-map-{slug.strip('-')}"


def fallback_cluster_bound_features(
    normalized_rows: Sequence[Mapping[str, object]],
    deduped_clusters: Sequence[Mapping[str, str]],
) -> list[dict[str, object]]:
    """Build simple rectangular bounds when the exporter does not pass polygons."""

    features: list[dict[str, object]] = []

    for cluster in deduped_clusters:
        cluster_rows = [
            row
            for row in normalized_rows
            if str(row["cluster_key"]) == cluster["cluster_key"]
        ]
        if not cluster_rows:
            continue
        min_lon = min(float(row["lon"]) for row in cluster_rows)
        max_lon = max(float(row["lon"]) for row in cluster_rows)
        min_lat = min(float(row["lat"]) for row in cluster_rows)
        max_lat = max(float(row["lat"]) for row in cluster_rows)
        pad_lon = max((max_lon - min_lon) * 0.12, 0.02)
        pad_lat = max((max_lat - min_lat) * 0.12, 0.02)
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [min_lon - pad_lon, min_lat - pad_lat],
                        [max_lon + pad_lon, min_lat - pad_lat],
                        [max_lon + pad_lon, max_lat + pad_lat],
                        [min_lon - pad_lon, max_lat + pad_lat],
                        [min_lon - pad_lon, min_lat - pad_lat],
                    ]],
                },
                "properties": {
                    "cluster_key": cluster["cluster_key"],
                    "cluster_label": cluster["cluster_label"],
                },
            }
        )

    return features
