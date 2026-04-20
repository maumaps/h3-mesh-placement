"""
Generic graph helpers shared by the installer-priority planner.
"""

from __future__ import annotations

from collections import defaultdict, deque
from heapq import heappop, heappush
from typing import Any, Iterable, Mapping


def connector_edge_priority(source_a: str, source_b: str) -> int:
    """Rank connector source pairs when RF loss is unavailable."""

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


def build_adjacency(
    edges: Iterable[tuple[int, int, float]],
) -> dict[int, dict[int, float]]:
    """Build an undirected weighted adjacency map from edge rows."""

    adjacency: dict[int, dict[int, float]] = defaultdict(dict)

    for source_id, target_id, distance_m in edges:
        previous_distance = adjacency[source_id].get(target_id)
        best_distance = (
            distance_m
            if previous_distance is None
            else min(previous_distance, distance_m)
        )
        adjacency[source_id][target_id] = best_distance
        adjacency[target_id][source_id] = best_distance

    return dict(adjacency)


def connected_components(
    node_ids: Iterable[int],
    adjacency: Mapping[int, Mapping[int, float]],
) -> list[tuple[int, ...]]:
    """Return graph connected components for a node subset."""

    pending = set(node_ids)
    components: list[tuple[int, ...]] = []

    while pending:
        start_id = min(pending)
        queue = deque([start_id])
        pending.remove(start_id)
        component: list[int] = []

        while queue:
            tower_id = queue.popleft()
            component.append(tower_id)

            for neighbor_id in sorted(adjacency.get(tower_id, {})):
                if neighbor_id in pending:
                    pending.remove(neighbor_id)
                    queue.append(neighbor_id)

        components.append(tuple(sorted(component)))

    return components


def restrict_adjacency_to_towers(
    adjacency: Mapping[int, Mapping[int, float]],
    allowed_ids: set[int],
) -> dict[int, dict[int, float]]:
    """Drop links that leave the currently allowed tower subset."""

    return {
        tower_id: {
            neighbor_id: distance_m
            for neighbor_id, distance_m in neighbors.items()
            if neighbor_id in allowed_ids
        }
        for tower_id, neighbors in adjacency.items()
        if tower_id in allowed_ids
    }


def component_members(
    start_id: int,
    allowed_ids: set[int],
    adjacency: Mapping[int, Mapping[int, float]],
) -> set[int]:
    """Return the connected component inside the remaining subgraph."""

    seen_ids = {start_id}
    queue = deque([start_id])

    while queue:
        tower_id = queue.popleft()
        for neighbor_id in adjacency.get(tower_id, {}):
            if neighbor_id in allowed_ids and neighbor_id not in seen_ids:
                seen_ids.add(neighbor_id)
                queue.append(neighbor_id)

    return seen_ids


def estimate_component_people(
    component_ids: set[int],
    active_ids: set[int],
    towers_by_id: Mapping[int, Any],
) -> int:
    """Estimate newly reachable people by deduping nearby populated localities."""

    covered_place_ids = {
        towers_by_id[tower_id].population_place_id
        for tower_id in active_ids
        if towers_by_id[tower_id].population_place_id
    }
    seen_place_ids = set(covered_place_ids)
    total_people = 0.0
    has_people_estimates = False

    for tower_id in sorted(component_ids):
        tower = towers_by_id[tower_id]

        if tower.people_estimate > 0:
            has_people_estimates = True

        if tower.population_place_id:
            if tower.population_place_id in seen_place_ids:
                continue
            seen_place_ids.add(tower.population_place_id)

        total_people += max(tower.people_estimate, 0.0)

    if has_people_estimates:
        return int(round(total_people))

    return len(component_ids)


def multi_source_distances(
    start_ids: Iterable[int],
    allowed_ids: set[int],
    adjacency: Mapping[int, Mapping[int, float]],
) -> dict[int, float]:
    """Return weighted shortest-path distances from one or more starts."""

    best_distances: dict[int, float] = {}
    heap: list[tuple[float, int]] = []

    for start_id in sorted(set(start_ids)):
        if start_id not in allowed_ids:
            continue
        best_distances[start_id] = 0.0
        heap.append((0.0, start_id))

    while heap:
        total_distance_m, tower_id = heappop(heap)

        if total_distance_m != best_distances.get(
            tower_id,
            float("inf"),
        ):
            continue

        for neighbor_id, distance_m in adjacency.get(tower_id, {}).items():
            if neighbor_id not in allowed_ids:
                continue

            candidate_distance_m = total_distance_m + distance_m

            if candidate_distance_m >= best_distances.get(
                neighbor_id,
                float("inf"),
            ):
                continue

            best_distances[neighbor_id] = candidate_distance_m
            heappush(heap, (candidate_distance_m, neighbor_id))

    return best_distances


def shortest_path_to_targets(
    start_id: int,
    target_ids: set[int],
    allowed_ids: set[int],
    adjacency: Mapping[int, Mapping[int, float]],
) -> tuple[int, float] | None:
    """Return the hop-first shortest path from one tower to any allowed target."""

    if start_id in target_ids:
        return (0, 0.0)

    best_costs: dict[int, tuple[int, float]] = {
        start_id: (0, 0.0),
    }
    heap: list[tuple[int, float, int]] = [(0, 0.0, start_id)]

    while heap:
        hop_count, total_distance_m, tower_id = heappop(heap)

        if (hop_count, total_distance_m) > best_costs.get(
            tower_id,
            (10**9, float("inf")),
        ):
            continue

        for neighbor_id, distance_m in adjacency.get(tower_id, {}).items():
            if neighbor_id not in allowed_ids:
                continue

            candidate_cost = (
                hop_count + 1,
                total_distance_m + distance_m,
            )

            if candidate_cost >= best_costs.get(
                neighbor_id,
                (10**9, float("inf")),
            ):
                continue

            if neighbor_id in target_ids:
                return candidate_cost

            best_costs[neighbor_id] = candidate_cost
            heappush(heap, (candidate_cost[0], candidate_cost[1], neighbor_id))

    return None
