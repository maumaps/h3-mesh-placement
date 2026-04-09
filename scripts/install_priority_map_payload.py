"""
Payload-shaping helpers for the installer-priority MapLibre HTML.
"""

from __future__ import annotations

import re
from typing import Mapping, Sequence


def dedupe_clusters(
    normalized_rows: Sequence[Mapping[str, object]],
) -> list[dict[str, str]]:
    """Keep one cluster metadata row per cluster key for the map payload."""

    deduped_clusters: list[dict[str, str]] = []
    seen_cluster_keys: set[str] = set()

    for row in normalized_rows:
        cluster_key = str(row["cluster_key"])
        if cluster_key in seen_cluster_keys:
            continue
        seen_cluster_keys.add(cluster_key)
        deduped_clusters.append(
            {
                "cluster_key": cluster_key,
                "cluster_label": str(row["cluster_label"]),
                "map_id": _cluster_map_id(cluster_key),
            }
        )

    return deduped_clusters


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
