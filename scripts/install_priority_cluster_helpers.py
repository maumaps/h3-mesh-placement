"""
Planner-specific helpers for installer rollout queues.
"""

from __future__ import annotations

from collections import defaultdict
from math import hypot
from typing import Any, Mapping, Sequence

try:
    from scripts.install_priority_graph_support import multi_source_distances
except ModuleNotFoundError:
    from install_priority_graph_support import multi_source_distances  # type: ignore[no-redef]


def compose_cluster_key(country_code: str, cluster_key: str) -> str:
    """Keep historical call sites working while rollout queues stay country-local."""

    return cluster_key


def compose_cluster_label(country_prefix: str, cluster_label: str) -> str:
    """Keep historical call sites working while rollout queues stay country-local."""

    return cluster_label


def group_towers_by_country(
    towers_by_id: Mapping[int, Any],
) -> dict[str, set[int]]:
    """Partition towers by country when diagnostics still need the grouping."""

    grouped_ids: dict[str, set[int]] = defaultdict(set)

    for tower_id, tower in towers_by_id.items():
        grouped_ids[(tower.country_code or "").strip().lower()].add(tower_id)

    return dict(grouped_ids)


def country_label(
    tower_ids: set[int],
    towers_by_id: Mapping[int, Any],
) -> str:
    """Pick one deterministic human country label for a country-local queue."""

    for tower_id in sorted(tower_ids):
        tower = towers_by_id[tower_id]
        if tower.country_name:
            return tower.country_name
        if tower.country_code:
            return tower.country_code.upper()

    return ""


def assign_unreachable_to_nearest_seed_cluster(
    towers_by_id: Mapping[int, Any],
    seed_components: Sequence[Sequence[int]],
    unreachable_ids: Sequence[int],
) -> dict[str, set[int]]:
    """Clamp blocked towers to the nearest installed seed cluster."""

    assigned_ids: dict[str, set[int]] = defaultdict(set)

    for tower_id in sorted(unreachable_ids):
        tower = towers_by_id[tower_id]
        nearest_cluster_key = min(
            (cluster_key(seed_component) for seed_component in seed_components),
            key=lambda current_cluster_key: min(
                (
                    hypot(
                        tower.lon - towers_by_id[seed_id].lon,
                        tower.lat - towers_by_id[seed_id].lat,
                    ),
                    seed_id,
                )
                for seed_id in (
                    int(value)
                    for value in current_cluster_key.removeprefix("seed:").split("+")
                )
            ),
        )
        assigned_ids[nearest_cluster_key].add(tower_id)

    return dict(assigned_ids)


def pending_connector_ids(
    *,
    active_ids: set[int],
    cluster_by_tower_id: Mapping[int, str],
    cluster_key: str,
    full_adjacency: Mapping[int, Mapping[int, float]],
    remaining_ids: set[int],
    towers_by_id: Mapping[int, Any],
) -> set[int]:
    """Pick the best unresolved cluster-join corridor for this queue."""

    active_connected_cluster_keys = {
        cluster_by_tower_id[neighbor_id]
        for tower_id in active_ids
        for neighbor_id in full_adjacency.get(tower_id, {})
        if (
            neighbor_id in cluster_by_tower_id
            and cluster_by_tower_id[neighbor_id] != cluster_key
        )
    }
    cluster_members_by_key: dict[str, set[int]] = defaultdict(set)

    for tower_id, tower_cluster_key in cluster_by_tower_id.items():
        cluster_members_by_key[tower_cluster_key].add(tower_id)

    cluster_country_by_key = {
        current_cluster_key: _cluster_country_code(
            tower_ids=tower_ids,
            towers_by_id=towers_by_id,
        )
        for current_cluster_key, tower_ids in cluster_members_by_key.items()
    }
    own_country_code = cluster_country_by_key.get(cluster_key, "")

    own_cluster_ids = cluster_members_by_key.get(cluster_key, set())
    own_seed_ids = {
        tower_id
        for tower_id in own_cluster_ids
        if getattr(towers_by_id[tower_id], "installed", False)
    }
    own_distances = multi_source_distances(
        start_ids=own_seed_ids,
        allowed_ids=own_cluster_ids,
        adjacency=full_adjacency,
    )
    peer_distance_cache: dict[str, dict[int, float]] = {}
    best_by_peer_cluster: dict[str, tuple[tuple[float, float, int, int, int], int]] = {}

    for tower_id in sorted(remaining_ids):
        if tower_id not in own_distances:
            continue

        for neighbor_id, edge_distance_m in full_adjacency.get(tower_id, {}).items():
            peer_cluster_key = cluster_by_tower_id.get(neighbor_id)

            if not peer_cluster_key or peer_cluster_key == cluster_key:
                continue
            if peer_cluster_key in active_connected_cluster_keys:
                continue

            if peer_cluster_key not in peer_distance_cache:
                peer_cluster_ids = cluster_members_by_key.get(
                    peer_cluster_key,
                    set(),
                )
                peer_seed_ids = {
                    peer_tower_id
                    for peer_tower_id in peer_cluster_ids
                    if getattr(towers_by_id[peer_tower_id], "installed", False)
                }
                peer_distance_cache[peer_cluster_key] = multi_source_distances(
                    start_ids=peer_seed_ids,
                    allowed_ids=peer_cluster_ids,
                    adjacency=full_adjacency,
                )

            peer_distances = peer_distance_cache[peer_cluster_key]

            if neighbor_id not in peer_distances:
                continue

            score = (
                0
                if (
                    own_country_code
                    and cluster_country_by_key.get(peer_cluster_key, "") == own_country_code
                )
                else 1,
                _connector_edge_priority(
                    towers_by_id[tower_id].source,
                    towers_by_id[neighbor_id].source,
                ),
                own_distances[tower_id] + edge_distance_m + peer_distances[neighbor_id],
                edge_distance_m,
                tower_id,
                neighbor_id,
            )

            if (
                peer_cluster_key not in best_by_peer_cluster
                or score < best_by_peer_cluster[peer_cluster_key][0]
            ):
                best_by_peer_cluster[peer_cluster_key] = (score, tower_id)

    if not best_by_peer_cluster:
        return set()

    best_score = min(
        score
        for score, _tower_id in best_by_peer_cluster.values()
    )

    return {
        tower_id
        for score, tower_id in best_by_peer_cluster.values()
        if score == best_score
    }


def cluster_key(seed_ids: Sequence[int]) -> str:
    """Create a deterministic cluster key from seed tower ids."""

    return "seed:" + "+".join(str(seed_id) for seed_id in sorted(seed_ids))


def pending_same_country_connector_ids(
    *,
    active_ids: set[int],
    cluster_by_tower_id: Mapping[int, str],
    cluster_key: str,
    full_adjacency: Mapping[int, Mapping[int, float]],
    remaining_ids: set[int],
    towers_by_id: Mapping[int, Any],
) -> set[int]:
    """Backward-compatible alias for the renamed connector targeting helper."""

    return pending_connector_ids(
        active_ids=active_ids,
        cluster_by_tower_id=cluster_by_tower_id,
        cluster_key=cluster_key,
        full_adjacency=full_adjacency,
        remaining_ids=remaining_ids,
        towers_by_id=towers_by_id,
    )


def _connector_edge_priority(source_a: str, source_b: str) -> int:
    """Prefer corridors built from route/bridge towers when RF loss is unavailable."""

    ordered_sources = tuple(sorted((source_a, source_b)))

    if ordered_sources == ("route", "route"):
        return 0
    if "route" in ordered_sources or "bridge" in ordered_sources:
        return 1
    if "coarse" in ordered_sources:
        return 2
    if "population" in ordered_sources:
        return 3

    return 4


def _cluster_country_code(
    *,
    tower_ids: set[int],
    towers_by_id: Mapping[int, Any],
) -> str:
    """Use installed seeds first when deriving one stable country code per cluster."""

    installed_country_codes = sorted(
        {
            (getattr(towers_by_id[tower_id], "country_code", "") or "").strip().lower()
            for tower_id in tower_ids
            if getattr(towers_by_id[tower_id], "installed", False)
            and (getattr(towers_by_id[tower_id], "country_code", "") or "").strip()
        }
    )

    if installed_country_codes:
        return installed_country_codes[0]

    member_country_codes = sorted(
        {
            (getattr(towers_by_id[tower_id], "country_code", "") or "").strip().lower()
            for tower_id in tower_ids
            if (getattr(towers_by_id[tower_id], "country_code", "") or "").strip()
        }
    )

    if member_country_codes:
        return member_country_codes[0]

    return ""
