"""
MapLibre-specific helpers for the installer-priority handout HTML.
"""

from __future__ import annotations

import json
import re
from typing import Mapping, Sequence

from scripts.install_priority_map_payload import (
    dedupe_clusters,
    fallback_cluster_bound_features,
    phase_one_connector_features,
)
from scripts.install_priority_maplibre_runtime import build_map_script
from scripts.install_priority_maplibre_vendor import read_maplibre_assets


def normalize_rows(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """Convert CSV-style strings into types that the HTML renderer can trust."""

    normalized_rows: list[dict[str, object]] = []

    for row in rows:
        normalized = dict(row)
        normalized["installed"] = _as_bool(row.get("installed"))
        normalized["is_next_for_cluster"] = _as_bool(row.get("is_next_for_cluster"))

        for integer_key in [
            "tower_id",
            "impact_score",
            "impact_people_est",
            "impact_tower_count",
            "next_unlock_count",
            "backlink_count",
        ]:
            normalized[integer_key] = _as_int(row.get(integer_key))

        for optional_integer_key in [
            "cluster_install_rank",
            "primary_previous_tower_id",
        ]:
            raw_value = row.get(optional_integer_key)
            normalized[optional_integer_key] = (
                ""
                if raw_value in (None, "")
                else _as_int(raw_value)
            )

        for float_key in ["lon", "lat"]:
            normalized[float_key] = float(row[float_key])

        normalized["inter_cluster_neighbor_ids"] = _as_int_list(
            row.get("inter_cluster_neighbor_ids")
        )
        normalized["previous_connection_ids"] = _as_int_list(
            row.get("previous_connection_ids")
        )
        normalized["map_order_label"] = _build_map_order_label(normalized)
        normalized_rows.append(normalized)

    return normalized_rows


def cluster_map_id(cluster_key: str) -> str:
    """Build a stable DOM id for one cluster mini map."""

    slug = re.sub(r"[^a-z0-9]+", "-", cluster_key.lower())

    return f"cluster-map-{slug.strip('-')}"


def render_map_assets(
    normalized_rows: Sequence[Mapping[str, object]],
    *,
    cluster_bound_features: Sequence[Mapping[str, object]] | None = None,
    mqtt_points: Sequence[Mapping[str, object]] | None = None,
    seed_mqtt_links: Sequence[Mapping[str, object]] | None = None,
    phase_one_tower_ids: Sequence[int] | None = None,
) -> list[str]:
    """Return inline script tags needed for the one-file MapLibre handout."""

    deduped_clusters = dedupe_clusters(normalized_rows)
    map_payload = {
        "rows": normalized_rows,
        "clusters": deduped_clusters,
        "cluster_bounds": list(
            cluster_bound_features
            or fallback_cluster_bound_features(
                normalized_rows,
                deduped_clusters,
            )
        ),
        "phase_one_connector_edges": phase_one_connector_features(
            normalized_rows,
        ),
        "phase_one_tower_ids": list(phase_one_tower_ids or []),
        "mqtt_points": list(mqtt_points or []),
        "seed_mqtt_links": list(seed_mqtt_links or []),
    }
    map_payload_json = json.dumps(map_payload, ensure_ascii=False).replace("</", "<\\/")
    maplibre_css, maplibre_js = read_maplibre_assets()
    maplibre_css = maplibre_css.replace("</", "<\\/")
    maplibre_js = maplibre_js.replace("</", "<\\/")

    return [
        f"<script id='install-priority-data' type='application/json'>{map_payload_json}</script>",
        f"<style>{maplibre_css}</style>",
        f"<script>{maplibre_js}</script>",
        "<script>",
        build_map_script(),
        "</script>",
    ]


def _as_bool(value: object) -> bool:
    """Parse booleans from CSV-style strings or plain Python values."""

    if isinstance(value, bool):
        return value
    if value is None:
        return False

    return str(value).strip().lower() == "true"


def _as_int(value: object) -> int | None:
    """Parse optional integers from CSV-style strings."""

    if value in (None, ""):
        return None
    if isinstance(value, int):
        return value

    return int(float(str(value)))


def _build_map_order_label(row: Mapping[str, object]) -> str:
    """Create a short on-map label that matches the local rollout order."""

    if bool(row.get("installed")) and str(row.get("source")) == "mqtt":
        return "M"
    if bool(row.get("installed")):
        return "S"
    if row.get("cluster_install_rank") in (None, ""):
        return ""

    return str(row["cluster_install_rank"])


def _as_int_list(value: object) -> list[int]:
    """Parse comma-separated integer lists from CSV-style strings."""

    if value in (None, ""):
        return []

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return [int(item) for item in value]

    return [
        int(item)
        for item in str(value).split(",")
        if item.strip()
    ]
