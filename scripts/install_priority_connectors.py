"""
Inter-cluster connector selection for the installer handout.
"""

from __future__ import annotations

from dataclasses import dataclass
from math import inf
from typing import Mapping, Sequence

try:
    from scripts.install_priority_graph import PlanRow
    from scripts.install_priority_graph_support import multi_source_distances
except ModuleNotFoundError:
    from install_priority_graph import PlanRow  # type: ignore[no-redef]
    from install_priority_graph_support import multi_source_distances  # type: ignore[no-redef]


@dataclass(frozen=True)
class InterClusterConnector:
    """One canonical visible connector chosen for a pair of rollout clusters."""

    left_cluster_key: str
    left_cluster_label: str
    left_tower_id: int
    left_rank: int | None
    left_source: str
    right_cluster_key: str
    right_cluster_label: str
    right_tower_id: int
    right_rank: int | None
    right_source: str
    distance_m: float


def select_inter_cluster_connectors(
    plan_rows: Sequence[PlanRow],
    adjacency: Mapping[int, Mapping[int, float]],
) -> list[InterClusterConnector]:
    """Select the cheapest seed-to-seed connector for every cluster pair."""

    row_by_tower_id = {
        plan_row.tower_id: plan_row
        for plan_row in plan_rows
    }
    cluster_members_by_key: dict[str, set[int]] = {}

    for plan_row in plan_rows:
        cluster_members_by_key.setdefault(plan_row.cluster_key, set()).add(
            plan_row.tower_id
        )

    seed_distances_by_cluster = {
        cluster_key: multi_source_distances(
            start_ids={
                plan_row.tower_id
                for plan_row in plan_rows
                if plan_row.cluster_key == cluster_key
                and plan_row.installed
            },
            allowed_ids=cluster_members_by_key[cluster_key],
            adjacency=adjacency,
        )
        for cluster_key in cluster_members_by_key
    }
    best_by_pair: dict[tuple[str, str], tuple[tuple[float, float, float, float, int, int], InterClusterConnector]] = {}

    for source_id, neighbors in adjacency.items():
        if source_id not in row_by_tower_id:
            continue

        for target_id, distance_m in neighbors.items():
            if source_id >= target_id or target_id not in row_by_tower_id:
                continue

            source_row = row_by_tower_id[source_id]
            target_row = row_by_tower_id[target_id]

            if source_row.cluster_key == target_row.cluster_key:
                continue
            connector = _normalize_connector(
                source_row=source_row,
                target_row=target_row,
                distance_m=distance_m,
            )
            pair_key = (
                connector.left_cluster_key,
                connector.right_cluster_key,
            )
            score = _connector_score(
                connector,
                seed_distances_by_cluster=seed_distances_by_cluster,
            )

            if pair_key not in best_by_pair or score < best_by_pair[pair_key][0]:
                best_by_pair[pair_key] = (score, connector)

    return [
        connector
        for _, connector in sorted(
            best_by_pair.values(),
            key=lambda item: (
                item[1].left_cluster_label.lower(),
                item[1].right_cluster_label.lower(),
                item[1].left_tower_id,
                item[1].right_tower_id,
            ),
        )
    ]


def _normalize_connector(
    source_row: PlanRow,
    target_row: PlanRow,
    distance_m: float,
) -> InterClusterConnector:
    """Keep connector orientation stable so pair keys remain deterministic."""

    ordered_rows = sorted(
        [source_row, target_row],
        key=lambda plan_row: (
            plan_row.cluster_label.lower(),
            plan_row.cluster_key,
            plan_row.tower_id,
        ),
    )
    left_row, right_row = ordered_rows

    return InterClusterConnector(
        left_cluster_key=left_row.cluster_key,
        left_cluster_label=left_row.cluster_label,
        left_tower_id=left_row.tower_id,
        left_rank=left_row.cluster_install_rank,
        left_source=left_row.source,
        right_cluster_key=right_row.cluster_key,
        right_cluster_label=right_row.cluster_label,
        right_tower_id=right_row.tower_id,
        right_rank=right_row.cluster_install_rank,
        right_source=right_row.source,
        distance_m=distance_m,
    )


def _connector_score(
    connector: InterClusterConnector,
    *,
    seed_distances_by_cluster: Mapping[str, Mapping[int, float]],
) -> tuple[float, float, float, float, int, int]:
    """Score connectors by RF proxy first, then total seed-to-seed join cost."""

    left_rank = _rank_value(connector.left_rank)
    right_rank = _rank_value(connector.right_rank)
    left_seed_distances = seed_distances_by_cluster.get(
        connector.left_cluster_key,
        {},
    )
    right_seed_distances = seed_distances_by_cluster.get(
        connector.right_cluster_key,
        {},
    )
    total_join_distance = (
        left_seed_distances.get(connector.left_tower_id, inf)
        + connector.distance_m
        + right_seed_distances.get(connector.right_tower_id, inf)
    )
    later_rank = max(left_rank, right_rank)
    summed_rank = left_rank + right_rank

    return (
        _connector_edge_priority(
            connector.left_source,
            connector.right_source,
        ),
        total_join_distance,
        later_rank,
        summed_rank,
        connector.distance_m,
        connector.left_tower_id,
        connector.right_tower_id,
    )


def _rank_value(rank: int | None) -> float:
    """Treat blocked nodes as appearing after every ranked rollout step."""

    if rank is None:
        return inf

    return float(rank)


def _connector_edge_priority(source_a: str, source_b: str) -> float:
    """Prefer route-derived corridors when explicit RF loss is unavailable."""

    ordered_sources = tuple(sorted((source_a, source_b)))

    if ordered_sources == ("route", "route"):
        return 0.0
    if "route" in ordered_sources or "bridge" in ordered_sources:
        return 1.0
    if "coarse" in ordered_sources:
        return 2.0
    if "population" in ordered_sources:
        return 3.0

    return 4.0
