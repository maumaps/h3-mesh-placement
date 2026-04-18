"""
Graph-planning helpers for the installer-priority handout.

This module stays independent from live database and network access so we can
unit-test the rollout logic in isolation.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from heapq import heappop, heappush
from typing import Mapping, Sequence

try:
    from scripts.install_priority_cluster_helpers import (
        assign_unreachable_to_nearest_seed_cluster,
        cluster_key,
        group_towers_by_country,
        pending_connector_ids,
    )
    from scripts.install_priority_graph_support import (
        build_adjacency,
        component_members,
        connected_components,
        estimate_component_people,
        restrict_adjacency_to_towers,
        shortest_path_to_targets,
    )
    from scripts.install_priority_points import EndpointObservation, reconstruct_tower_points
except ModuleNotFoundError:
    from install_priority_cluster_helpers import (  # type: ignore[no-redef]
        assign_unreachable_to_nearest_seed_cluster,
        cluster_key,
        group_towers_by_country,
        pending_connector_ids,
    )
    from install_priority_graph_support import (  # type: ignore[no-redef]
        build_adjacency,
        component_members,
        connected_components,
        estimate_component_people,
        restrict_adjacency_to_towers,
        shortest_path_to_targets,
    )
    from install_priority_points import (  # type: ignore[no-redef]
        EndpointObservation,
        reconstruct_tower_points,
    )


SOURCE_PRIORITY = {
    "coarse": 3,
    "population": 3,
    "route": 2,
    "cluster_slim": 1,
    "bridge": 1,
    "greedy": 1,
}


@dataclass(frozen=True)
class TowerRecord:
    """Tower metadata needed for rollout planning."""

    tower_id: int
    source: str
    lon: float
    lat: float
    label: str
    installed: bool
    created_at: datetime | None = None
    people_estimate: float = 0.0
    population_place_id: str | None = None
    population_place_name: str | None = None
    display_name: str | None = None
    display_code: str | None = None
    country_code: str | None = None
    country_name: str | None = None


@dataclass(frozen=True)
class PlanRow:
    """One row in the cluster-local rollout plan before enrichment."""

    cluster_key: str
    cluster_label: str
    cluster_install_rank: int | None
    is_next_for_cluster: bool
    rollout_status: str
    installed: bool
    tower_id: int
    label: str
    source: str
    impact_score: int
    impact_tower_count: int
    next_unlock_count: int
    backlink_count: int
    previous_connection_ids: tuple[int, ...]
    next_connection_ids: tuple[int, ...]
    lon: float
    lat: float


def build_cluster_plan(
    towers_by_id: Mapping[int, TowerRecord],
    adjacency: Mapping[int, Mapping[int, float]],
) -> list[PlanRow]:
    """Plan a separate rollout queue for every installed seed cluster."""

    if not any(tower.installed for tower in towers_by_id.values()):
        raise ValueError("No installed seed towers were found in mesh_towers.")

    plan_rows: list[PlanRow] = []
    installed_seed_ids = sorted(
        tower_id
        for tower_id, tower in towers_by_id.items()
        if tower.installed
    )
    grouped_tower_ids = group_towers_by_country(towers_by_id)
    seed_components: list[tuple[int, ...]] = []
    assignment: dict[int, str] = {}
    cluster_members: dict[str, set[int]] = defaultdict(set)

    for _country_code, country_tower_ids in sorted(grouped_tower_ids.items()):
        country_seed_ids = sorted(
            tower_id
            for tower_id in installed_seed_ids
            if tower_id in country_tower_ids
        )
        if not country_seed_ids:
            continue

        country_seed_components = connected_components(
            country_seed_ids,
            adjacency,
        )
        seed_components.extend(country_seed_components)
        country_assignment, country_cluster_members = _assign_nodes_to_seed_clusters(
            towers_by_id=towers_by_id,
            adjacency=adjacency,
            seed_components=country_seed_components,
            allowed_ids=country_tower_ids,
        )
        assignment.update(country_assignment)
        for current_cluster_key, tower_ids in country_cluster_members.items():
            cluster_members.setdefault(current_cluster_key, set()).update(tower_ids)

        unreachable_country_ids = sorted(
            tower_id
            for tower_id in country_tower_ids
            if tower_id not in country_assignment
        )
        if unreachable_country_ids:
            for current_cluster_key, blocked_ids in assign_unreachable_to_nearest_seed_cluster(
                towers_by_id=towers_by_id,
                seed_components=country_seed_components,
                unreachable_ids=unreachable_country_ids,
            ).items():
                cluster_members.setdefault(current_cluster_key, set()).update(
                    blocked_ids
                )
                for blocked_id in blocked_ids:
                    assignment[blocked_id] = current_cluster_key

    unreachable_ids = sorted(
        tower_id
        for tower_id in towers_by_id
        if tower_id not in assignment
    )

    if unreachable_ids and seed_components:
        for current_cluster_key, blocked_ids in assign_unreachable_to_nearest_seed_cluster(
            towers_by_id=towers_by_id,
            seed_components=seed_components,
            unreachable_ids=unreachable_ids,
        ).items():
            cluster_members.setdefault(current_cluster_key, set()).update(
                blocked_ids
            )
            for blocked_id in blocked_ids:
                assignment[blocked_id] = current_cluster_key

    cluster_by_tower_id = dict(assignment)

    for seed_component in seed_components:
        current_cluster_key = cluster_key(seed_component)
        cluster_seed_ids = tuple(sorted(seed_component))
        cluster_tower_ids = tuple(
            sorted(cluster_members.get(current_cluster_key, set()))
        )
        plan_rows.extend(
            _plan_seed_cluster(
                towers_by_id=towers_by_id,
                adjacency=adjacency,
                full_adjacency=adjacency,
                cluster_key=current_cluster_key,
                cluster_by_tower_id=cluster_by_tower_id,
                cluster_seed_ids=cluster_seed_ids,
                cluster_tower_ids=cluster_tower_ids,
            )
        )

    return sorted(
        plan_rows,
        key=lambda row: (
            row.cluster_label.lower(),
            row.cluster_install_rank is None,
            row.cluster_install_rank if row.cluster_install_rank is not None else 10**9,
            not row.installed,
            row.tower_id,
        ),
    )


def _assign_nodes_to_seed_clusters(
    towers_by_id: Mapping[int, TowerRecord],
    adjacency: Mapping[int, Mapping[int, float]],
    seed_components: Sequence[Sequence[int]],
    allowed_ids: set[int] | None = None,
) -> tuple[dict[int, str], dict[str, set[int]]]:
    """Assign every reachable tower to the nearest installed seed cluster."""

    allowed_tower_ids = set(towers_by_id) if allowed_ids is None else set(allowed_ids)
    # Order seed clusters once so tie-breaking stays deterministic.
    cluster_order = {
        cluster_key(seed_component): index
        for index, seed_component in enumerate(
            sorted(seed_components, key=lambda component: (min(component), tuple(component)))
        )
    }
    cluster_members: dict[str, set[int]] = defaultdict(set)
    best_costs: dict[int, tuple[int, float, int]] = {}
    assignment: dict[int, str] = {}
    heap: list[tuple[int, float, int, int]] = []

    # Run a multi-source Dijkstra-like wave from all installed seed clusters.
    for seed_cluster_key, order_index in sorted(cluster_order.items(), key=lambda item: item[1]):
        seed_ids = tuple(int(value) for value in seed_cluster_key.removeprefix("seed:").split("+"))
        for seed_id in seed_ids:
            if seed_id not in allowed_tower_ids:
                continue
            heappush(heap, (0, 0.0, order_index, seed_id))

    while heap:
        hop_count, total_distance_m, order_index, tower_id = heappop(heap)
        candidate_cost = (hop_count, total_distance_m, order_index)

        if tower_id in best_costs and candidate_cost >= best_costs[tower_id]:
            continue

        best_costs[tower_id] = candidate_cost
        assigned_cluster_key = next(
            key
            for key, key_order in cluster_order.items()
            if key_order == order_index
        )
        assignment[tower_id] = assigned_cluster_key
        cluster_members[assigned_cluster_key].add(tower_id)

        for neighbor_id, distance_m in adjacency.get(tower_id, {}).items():
            if neighbor_id not in allowed_tower_ids:
                continue
            heappush(
                heap,
                (hop_count + 1, total_distance_m + distance_m, order_index, neighbor_id),
            )

    # Keep installed seeds inside their final cluster membership even if they had
    # no outbound traversal during the wavefront.
    for tower_id, tower in towers_by_id.items():
        if tower.installed and tower_id in allowed_tower_ids:
            assigned_cluster_key = assignment.get(tower_id)
            if assigned_cluster_key:
                cluster_members[assigned_cluster_key].add(tower_id)

    return assignment, dict(cluster_members)


def _plan_seed_cluster(
    towers_by_id: Mapping[int, TowerRecord],
    adjacency: Mapping[int, Mapping[int, float]],
    full_adjacency: Mapping[int, Mapping[int, float]],
    cluster_key: str,
    cluster_by_tower_id: Mapping[int, str],
    cluster_seed_ids: Sequence[int],
    cluster_tower_ids: Sequence[int],
) -> list[PlanRow]:
    """Build the local rollout queue for one seed-rooted cluster."""

    seed_labels = [towers_by_id[seed_id].label for seed_id in cluster_seed_ids]
    cluster_label = ", ".join(sorted(seed_labels, key=str.lower))
    active_ids = set(cluster_seed_ids)
    remaining_ids = set(cluster_tower_ids) - active_ids
    plan_rows: list[PlanRow] = []

    # Emit installed towers first so field teams see their current backbone.
    for seed_id in sorted(cluster_seed_ids, key=lambda item: towers_by_id[item].label.lower()):
        tower = towers_by_id[seed_id]
        plan_rows.append(
            PlanRow(
                cluster_key=cluster_key,
                cluster_label=cluster_label,
                cluster_install_rank=0,
                is_next_for_cluster=False,
                rollout_status="installed",
                installed=True,
                tower_id=tower.tower_id,
                label=tower.label,
                source=tower.source,
                impact_score=0,
                impact_tower_count=0,
                next_unlock_count=0,
                backlink_count=0,
                previous_connection_ids=(),
                next_connection_ids=(),
                lon=tower.lon,
                lat=tower.lat,
            )
        )

    rank = 1
    first_candidate = True

    while remaining_ids:
        target_boundary_ids = pending_connector_ids(
            active_ids=active_ids,
            cluster_by_tower_id=cluster_by_tower_id,
            cluster_key=cluster_key,
            full_adjacency=full_adjacency,
            remaining_ids=remaining_ids,
            towers_by_id=towers_by_id,
        )
        # Only nodes already adjacent to the active backbone are eligible now.
        frontier_ids = {
            tower_id
            for tower_id in remaining_ids
            if any(neighbor_id in active_ids for neighbor_id in adjacency.get(tower_id, {}))
        }

        if not frontier_ids:
            plan_rows.extend(
                _plan_blocked_cluster(
                    towers_by_id=towers_by_id,
                    cluster_key=cluster_key,
                    cluster_label=cluster_label,
                    tower_ids=tuple(sorted(remaining_ids)),
                )
            )
            break

        best_choice: tuple[tuple[int, int, float, int, int, int, int, int], int, tuple[int, ...], tuple[int, ...]] | None = None

        for tower_id in sorted(frontier_ids):
            # Keep explicit backlinks so the handout can say what this node reaches now.
            previous_connection_ids = tuple(
                sorted(
                    neighbor_id
                    for neighbor_id in adjacency.get(tower_id, {})
                    if neighbor_id in active_ids
                )
            )

            # Impact counts the downstream frontier size, while the main score is an
            # estimated newly reachable people count derived from the candidate and the
            # towers it unlocks behind it.
            unlocked_component_ids = component_members(
                start_id=tower_id,
                allowed_ids=remaining_ids,
                adjacency=adjacency,
            )
            unlocked_component_size = len(unlocked_component_ids)
            impact_people_est = estimate_component_people(
                component_ids=unlocked_component_ids,
                active_ids=active_ids,
                towers_by_id=towers_by_id,
            )
            connector_path = shortest_path_to_targets(
                start_id=tower_id,
                target_ids=target_boundary_ids,
                allowed_ids=remaining_ids,
                adjacency=adjacency,
            )
            connector_reachable = 1 if connector_path is not None else 0
            connector_hops = -connector_path[0] if connector_path else -10**6
            connector_distance = -connector_path[1] if connector_path else float("-inf")
            connector_cluster_count = len(
                {
                    cluster_by_tower_id[neighbor_id]
                    for unlocked_id in (unlocked_component_ids & target_boundary_ids)
                    for neighbor_id in full_adjacency.get(unlocked_id, {})
                    if (
                        neighbor_id in cluster_by_tower_id
                        and cluster_by_tower_id[neighbor_id] != cluster_key
                    )
                }
            )

            # Surface the immediate next nodes this tower would unlock after installation.
            next_connection_ids = tuple(
                sorted(
                    other_id
                    for other_id in remaining_ids
                    if other_id != tower_id
                    and any(neighbor_id == tower_id for neighbor_id in adjacency.get(other_id, {}))
                    and not any(
                        neighbor_id in active_ids
                        for neighbor_id in adjacency.get(other_id, {})
                    )
                )
            )

            choice_score = (
                connector_reachable,
                connector_hops,
                connector_distance,
                connector_cluster_count,
                impact_people_est,
                unlocked_component_size,
                len(next_connection_ids),
                len(previous_connection_ids),
                SOURCE_PRIORITY.get(towers_by_id[tower_id].source, 0),
                -tower_id,
            )

            if best_choice is None or choice_score > best_choice[0]:
                best_choice = (
                    choice_score,
                    tower_id,
                    previous_connection_ids,
                    next_connection_ids,
                )

        assert best_choice is not None, "Frontier selection unexpectedly produced no candidate."
        _, chosen_id, previous_connection_ids, next_connection_ids = best_choice
        chosen_tower = towers_by_id[chosen_id]
        plan_rows.append(
            PlanRow(
                cluster_key=cluster_key,
                cluster_label=cluster_label,
                cluster_install_rank=rank,
                is_next_for_cluster=first_candidate,
                rollout_status="next" if first_candidate else "planned",
                installed=False,
                tower_id=chosen_tower.tower_id,
                label=chosen_tower.label,
                source=chosen_tower.source,
                impact_score=best_choice[0][4],
                impact_tower_count=best_choice[0][5],
                next_unlock_count=len(next_connection_ids),
                backlink_count=len(previous_connection_ids),
                previous_connection_ids=previous_connection_ids,
                next_connection_ids=next_connection_ids,
                lon=chosen_tower.lon,
                lat=chosen_tower.lat,
            )
        )
        active_ids.add(chosen_id)
        remaining_ids.remove(chosen_id)
        rank += 1
        first_candidate = False

    return plan_rows


def _plan_blocked_cluster(
    towers_by_id: Mapping[int, TowerRecord],
    cluster_key: str,
    cluster_label: str,
    tower_ids: Sequence[int],
) -> list[PlanRow]:
    """Mark towers that cannot be reached from any installed backbone."""

    blocked_rows: list[PlanRow] = []

    for tower_id in sorted(tower_ids, key=lambda item: towers_by_id[item].label.lower()):
        tower = towers_by_id[tower_id]
        blocked_rows.append(
            PlanRow(
                cluster_key=cluster_key,
                cluster_label=cluster_label,
                cluster_install_rank=None,
                is_next_for_cluster=False,
                rollout_status="blocked",
                installed=False,
                tower_id=tower.tower_id,
                label=tower.label,
                source=tower.source,
                impact_score=0,
                impact_tower_count=0,
                next_unlock_count=0,
                backlink_count=0,
                previous_connection_ids=(),
                next_connection_ids=(),
                lon=tower.lon,
                lat=tower.lat,
            )
        )

    return blocked_rows
