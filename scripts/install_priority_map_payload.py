"""
Payload-shaping helpers for the installer-priority MapLibre HTML.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Mapping, Sequence


@dataclass(frozen=True)
class _ClusterConnectorEdge:
    """One inter-cluster edge candidate for overview phase-one filtering."""

    left_cluster_key: str
    right_cluster_key: str
    left_rank: int
    right_rank: int
    left_tower_id: int
    right_tower_id: int

    @property
    def later_rank(self) -> int:
        """Rank when both endpoint clusters have reached this connector."""

        return max(self.left_rank, self.right_rank)

    @property
    def summed_rank(self) -> int:
        """Small tie-breaker for connector-tree ordering."""

        return self.left_rank + self.right_rank


def dedupe_clusters(
    normalized_rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Keep one cluster metadata row per cluster key for the map payload."""

    connect_max_rank_by_cluster = connect_max_rank_by_cluster_from_rows(
        normalized_rows,
    )
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


def connect_max_rank_by_cluster_from_rows(
    normalized_rows: Sequence[Mapping[str, object]],
) -> dict[str, int]:
    """Find the last rank needed to join the cluster connector tree."""

    connector_tree_edges = phase_one_connector_edges(normalized_rows)
    connector_ranks_by_cluster: dict[str, list[int]] = {}
    for edge in connector_tree_edges:
        connector_ranks_by_cluster.setdefault(edge.left_cluster_key, []).append(
            edge.left_rank,
        )
        connector_ranks_by_cluster.setdefault(edge.right_cluster_key, []).append(
            edge.right_rank,
        )

    planned_ranks_by_cluster: dict[str, list[int]] = {}
    for row in normalized_rows:
        rank = _optional_rank(row.get("cluster_install_rank"))
        if rank is not None and not bool(row.get("installed")):
            planned_ranks_by_cluster.setdefault(str(row["cluster_key"]), []).append(
                rank,
            )

    connect_max_rank_by_cluster: dict[str, int] = {}
    for row in normalized_rows:
        cluster_key = str(row["cluster_key"])
        if cluster_key in connect_max_rank_by_cluster:
            continue

        connector_ranks = connector_ranks_by_cluster.get(cluster_key, [])
        planned_ranks = planned_ranks_by_cluster.get(cluster_key, [])
        if connector_ranks:
            connect_max_rank_by_cluster[cluster_key] = max(connector_ranks)
        elif planned_ranks:
            connect_max_rank_by_cluster[cluster_key] = min(planned_ranks)
        else:
            connect_max_rank_by_cluster[cluster_key] = 0

    return connect_max_rank_by_cluster


def phase_one_connector_edges(
    normalized_rows: Sequence[Mapping[str, object]],
) -> list[_ClusterConnectorEdge]:
    """Return the connector-tree edges used by overview phase one."""

    rows_by_tower_id = {
        int(row["tower_id"]): row
        for row in normalized_rows
    }
    best_edge_by_cluster_pair: dict[tuple[str, str], _ClusterConnectorEdge] = {}

    for row in normalized_rows:
        cluster_key = str(row["cluster_key"])
        tower_id = int(row["tower_id"])
        rank = _optional_rank(row.get("cluster_install_rank"))

        if rank is None or bool(row.get("installed")):
            continue

        for neighbor_id in row.get("inter_cluster_neighbor_ids") or []:
            neighbor_row = rows_by_tower_id.get(int(neighbor_id))
            if not neighbor_row:
                continue

            neighbor_cluster_key = str(neighbor_row["cluster_key"])
            if neighbor_cluster_key == cluster_key:
                continue

            neighbor_rank = _optional_rank(neighbor_row.get("cluster_install_rank"))
            if neighbor_rank is None:
                neighbor_rank = 0
            edge = _normalize_connector_edge(
                left_cluster_key=cluster_key,
                right_cluster_key=neighbor_cluster_key,
                left_rank=rank,
                right_rank=neighbor_rank,
                left_tower_id=tower_id,
                right_tower_id=int(neighbor_id),
            )
            pair_key = (
                edge.left_cluster_key,
                edge.right_cluster_key,
            )
            if (
                pair_key not in best_edge_by_cluster_pair
                or _connector_tree_score(edge) < _connector_tree_score(
                    best_edge_by_cluster_pair[pair_key],
                )
            ):
                best_edge_by_cluster_pair[pair_key] = edge

    connector_tree_edges = _minimum_connector_tree(
        cluster_keys={str(row["cluster_key"]) for row in normalized_rows},
        edges=best_edge_by_cluster_pair.values(),
    )

    return connector_tree_edges


def phase_one_connector_features(
    normalized_rows: Sequence[Mapping[str, object]],
) -> list[dict[str, object]]:
    """Build GeoJSON lines for the overview phase-one cluster joins."""

    rows_by_tower_id = {
        int(row["tower_id"]): row
        for row in normalized_rows
    }
    features: list[dict[str, object]] = []

    for edge in phase_one_connector_edges(normalized_rows):
        left_row = rows_by_tower_id[edge.left_tower_id]
        right_row = rows_by_tower_id[edge.right_tower_id]
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "LineString",
                    "coordinates": [
                        [float(left_row["lon"]), float(left_row["lat"])],
                        [float(right_row["lon"]), float(right_row["lat"])],
                    ],
                },
                "properties": {
                    "from_cluster_key": edge.left_cluster_key,
                    "to_cluster_key": edge.right_cluster_key,
                    "from_tower_id": edge.left_tower_id,
                    "to_tower_id": edge.right_tower_id,
                    "link_kind": "phase_one_connector",
                },
            }
        )

    return features


def _normalize_connector_edge(
    *,
    left_cluster_key: str,
    right_cluster_key: str,
    left_rank: int,
    right_rank: int,
    left_tower_id: int,
    right_tower_id: int,
) -> _ClusterConnectorEdge:
    """Keep edge orientation stable for pair de-duplication."""

    if (right_cluster_key, right_tower_id) < (left_cluster_key, left_tower_id):
        return _ClusterConnectorEdge(
            left_cluster_key=right_cluster_key,
            right_cluster_key=left_cluster_key,
            left_rank=right_rank,
            right_rank=left_rank,
            left_tower_id=right_tower_id,
            right_tower_id=left_tower_id,
        )

    return _ClusterConnectorEdge(
        left_cluster_key=left_cluster_key,
        right_cluster_key=right_cluster_key,
        left_rank=left_rank,
        right_rank=right_rank,
        left_tower_id=left_tower_id,
        right_tower_id=right_tower_id,
    )


def _connector_tree_score(edge: _ClusterConnectorEdge) -> tuple[int, int, int, int]:
    """Prefer the earliest connector that grows the overview phase-one tree."""

    return (
        edge.later_rank,
        edge.summed_rank,
        edge.left_tower_id,
        edge.right_tower_id,
    )


def _minimum_connector_tree(
    *,
    cluster_keys: set[str],
    edges: Sequence[_ClusterConnectorEdge],
) -> list[_ClusterConnectorEdge]:
    """Return the minimum-rank connector tree across reachable clusters."""

    parent = {cluster_key: cluster_key for cluster_key in cluster_keys}

    def find(cluster_key: str) -> str:
        while parent[cluster_key] != cluster_key:
            parent[cluster_key] = parent[parent[cluster_key]]
            cluster_key = parent[cluster_key]

        return cluster_key

    tree_edges: list[_ClusterConnectorEdge] = []
    for edge in sorted(edges, key=_connector_tree_score):
        left_root = find(edge.left_cluster_key)
        right_root = find(edge.right_cluster_key)
        if left_root == right_root:
            continue

        parent[right_root] = left_root
        tree_edges.append(edge)

    return tree_edges


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
