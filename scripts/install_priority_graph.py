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

from scripts.install_priority_cluster_helpers import (
    assign_unreachable_to_nearest_seed_cluster,
    cluster_key,
    pending_connector_ids,
)
from scripts.install_priority_graph_support import (
    build_adjacency,
    component_members,
    connected_components,
    estimate_component_people,
    multi_source_path_costs,
    restrict_adjacency_to_towers,
    shortest_path_to_targets,
)
from scripts.install_priority_points import EndpointObservation, reconstruct_tower_points


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

    if not any(
        tower.installed and tower.source == "seed"
        for tower in towers_by_id.values()
    ):
        raise ValueError("No installed seed towers were found in mesh_towers.")

    plan_rows: list[PlanRow] = []
    installed_seed_ids = sorted(
        tower_id
        for tower_id, tower in towers_by_id.items()
        if tower.installed and tower.source == "seed"
    )
    seed_components: list[tuple[int, ...]] = []
    assignment: dict[int, str] = {}
    cluster_members: dict[str, set[int]] = defaultdict(set)

    seed_components = connected_components(
        installed_seed_ids,
        adjacency,
    )
    assignment, cluster_members = _assign_nodes_to_seed_clusters(
        towers_by_id=towers_by_id,
        adjacency=adjacency,
        seed_components=seed_components,
    )

    unreachable_ids = sorted(
        tower_id
        for tower_id in towers_by_id
        if tower_id not in assignment
    )

    if unreachable_ids and seed_components:
        for unreachable_component in connected_components(unreachable_ids, adjacency):
            for current_cluster_key, detached_ids in assign_unreachable_to_nearest_seed_cluster(
                towers_by_id=towers_by_id,
                seed_components=seed_components,
                unreachable_ids=(min(unreachable_component),),
            ).items():
                cluster_members.setdefault(current_cluster_key, set()).update(
                    unreachable_component
                )
                for detached_id in unreachable_component:
                    assignment[detached_id] = current_cluster_key

    _repair_cluster_assignments_for_local_connectivity(
        towers_by_id=towers_by_id,
        assignment=assignment,
        cluster_members=cluster_members,
        seed_components=seed_components,
        adjacency=adjacency,
    )

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
    """Assign every reachable tower to the nearest eligible installed seed cluster."""

    allowed_tower_ids = set(towers_by_id) if allowed_ids is None else set(allowed_ids)
    # Order seed clusters once so tie-breaking stays deterministic.
    cluster_order = {
        cluster_key(seed_component): index
        for index, seed_component in enumerate(
            sorted(seed_components, key=lambda component: (min(component), tuple(component)))
        )
    }
    cluster_members: dict[str, set[int]] = defaultdict(set)
    assignment: dict[int, str] = {}
    cluster_country_codes: dict[str, set[str]] = {}
    path_costs_by_cluster_key: dict[str, dict[int, tuple[float, int]]] = {}

    # Precompute seed-to-node path costs once per cluster so ownership can prefer
    # same-country queues without losing the underlying visible-distance signal.
    for seed_component in seed_components:
        current_cluster_key = cluster_key(seed_component)
        cluster_country_codes[current_cluster_key] = {
            (towers_by_id[seed_id].country_code or "").strip().lower()
            for seed_id in seed_component
            if (towers_by_id[seed_id].country_code or "").strip()
        }
        path_costs_by_cluster_key[current_cluster_key] = multi_source_path_costs(
            start_ids=seed_component,
            allowed_ids=allowed_tower_ids,
            adjacency=adjacency,
        )

    ordered_cluster_keys = [
        current_cluster_key
        for current_cluster_key, _order_index in sorted(
            cluster_order.items(),
            key=lambda item: item[1],
        )
    ]

    for tower_id in sorted(allowed_tower_ids):
        tower_country_code = (towers_by_id[tower_id].country_code or "").strip().lower()
        reachable_cluster_keys = [
            current_cluster_key
            for current_cluster_key in ordered_cluster_keys
            if tower_id in path_costs_by_cluster_key[current_cluster_key]
        ]

        if not reachable_cluster_keys:
            continue

        same_country_cluster_keys = [
            current_cluster_key
            for current_cluster_key in reachable_cluster_keys
            if tower_country_code
            and tower_country_code in cluster_country_codes[current_cluster_key]
        ]
        eligible_cluster_keys = (
            same_country_cluster_keys
            if same_country_cluster_keys
            else reachable_cluster_keys
        )
        assigned_cluster_key = min(
            eligible_cluster_keys,
            key=lambda current_cluster_key: (
                path_costs_by_cluster_key[current_cluster_key][tower_id][0],
                path_costs_by_cluster_key[current_cluster_key][tower_id][1],
                cluster_order[current_cluster_key],
            ),
        )
        assignment[tower_id] = assigned_cluster_key
        cluster_members[assigned_cluster_key].add(tower_id)

    # Keep installed seeds inside their final cluster membership even if they had
    # no outbound traversal during the wavefront.
    for tower_id, tower in towers_by_id.items():
        if tower.installed and tower_id in allowed_tower_ids:
            assigned_cluster_key = assignment.get(tower_id)
            if assigned_cluster_key:
                cluster_members[assigned_cluster_key].add(tower_id)

    return assignment, dict(cluster_members)


def _repair_cluster_assignments_for_local_connectivity(
    *,
    towers_by_id: Mapping[int, TowerRecord],
    assignment: dict[int, str],
    cluster_members: dict[str, set[int]],
    seed_components: Sequence[Sequence[int]],
    adjacency: Mapping[int, Mapping[int, float]],
) -> None:
    """Move detached assigned islands to the cluster that actually reaches them."""

    seed_ids_by_cluster_key = {
        cluster_key(seed_component): set(seed_component)
        for seed_component in seed_components
    }
    seed_country_codes_by_cluster_key = {
        cluster_key(seed_component): {
            (towers_by_id[seed_id].country_code or "").strip().lower()
            for seed_id in seed_component
            if (towers_by_id[seed_id].country_code or "").strip()
        }
        for seed_component in seed_components
    }

    while True:
        moved_component = False

        for current_cluster_key in sorted(cluster_members):
            cluster_ids = set(cluster_members.get(current_cluster_key, set()))
            if not cluster_ids:
                continue

            seed_ids = seed_ids_by_cluster_key.get(current_cluster_key, set())
            for component in connected_components(sorted(cluster_ids), adjacency):
                component_ids = set(component)
                if component_ids & seed_ids:
                    continue

                component_country_codes = {
                    (towers_by_id[tower_id].country_code or "").strip().lower()
                    for tower_id in component_ids
                    if (towers_by_id[tower_id].country_code or "").strip()
                }
                bridge_candidates: list[tuple[int, float, str, int, int]] = []
                for tower_id in sorted(component_ids):
                    for neighbor_id, distance_m in adjacency.get(tower_id, {}).items():
                        neighbor_cluster_key = assignment.get(neighbor_id)
                        if not neighbor_cluster_key:
                            continue
                        if neighbor_cluster_key == current_cluster_key:
                            continue

                        seed_country_codes = seed_country_codes_by_cluster_key.get(
                            neighbor_cluster_key,
                            set(),
                        )
                        country_priority = (
                            0
                            if component_country_codes
                            and component_country_codes & seed_country_codes
                            else 1
                        )
                        bridge_candidates.append(
                            (
                                country_priority,
                                distance_m,
                                neighbor_cluster_key,
                                tower_id,
                                neighbor_id,
                            )
                        )

                if not bridge_candidates:
                    continue

                _country_priority, _distance_m, target_cluster_key, _tower_id, _neighbor_id = min(
                    bridge_candidates
                )
                cluster_members[current_cluster_key].difference_update(component_ids)
                cluster_members.setdefault(target_cluster_key, set()).update(component_ids)
                for tower_id in component_ids:
                    assignment[tower_id] = target_cluster_key

                moved_component = True
                break

            if moved_component:
                break

        if not moved_component:
            return


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

    display_seed_ids = [
        seed_id
        for seed_id in cluster_seed_ids
        if towers_by_id[seed_id].source == "seed"
    ] or list(cluster_seed_ids)
    seed_labels = [towers_by_id[seed_id].label for seed_id in display_seed_ids]
    cluster_label = ", ".join(sorted(seed_labels, key=str.lower))
    installed_ids = {
        tower_id
        for tower_id in cluster_tower_ids
        if towers_by_id[tower_id].installed
    }
    active_ids = set(installed_ids)
    remaining_ids = set(cluster_tower_ids) - active_ids
    plan_rows: list[PlanRow] = []

    # Emit installed towers first so field teams see their current backbone.
    for seed_id in sorted(
        installed_ids,
        key=lambda item: (
            0 if towers_by_id[item].source == "seed" else 1,
            towers_by_id[item].label.lower(),
            item,
        ),
    ):
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
                _plan_detached_cluster(
                    towers_by_id=towers_by_id,
                    cluster_key=cluster_key,
                    cluster_label=cluster_label,
                    adjacency=adjacency,
                    tower_ids=tuple(sorted(remaining_ids)),
                    start_rank=rank,
                    first_candidate=first_candidate,
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


def _plan_detached_cluster(
    towers_by_id: Mapping[int, TowerRecord],
    cluster_key: str,
    cluster_label: str,
    adjacency: Mapping[int, Mapping[int, float]],
    tower_ids: Sequence[int],
    start_rank: int,
    first_candidate: bool,
) -> list[PlanRow]:
    """Continue a rollout queue for towers without a current backbone path."""

    plan_rows: list[PlanRow] = []
    remaining_ids = set(tower_ids)
    active_detached_ids: set[int] = set()
    rank = start_rank

    while remaining_ids:
        # Detached components still need a deterministic field order.
        # Once the first detached tower is installed, its visible neighbors become
        # the local frontier for the rest of this queue.
        frontier_ids = {
            tower_id
            for tower_id in remaining_ids
            if any(neighbor_id in active_detached_ids for neighbor_id in adjacency.get(tower_id, {}))
        }
        candidate_ids = frontier_ids or remaining_ids
        best_choice: tuple[tuple[int, int, int, int, int], int, tuple[int, ...], tuple[int, ...]] | None = None

        for tower_id in sorted(candidate_ids):
            previous_connection_ids = tuple(
                sorted(
                    neighbor_id
                    for neighbor_id in adjacency.get(tower_id, {})
                    if neighbor_id in active_detached_ids
                )
            )
            unlocked_component_ids = component_members(
                start_id=tower_id,
                allowed_ids=remaining_ids,
                adjacency=adjacency,
            )
            impact_people_est = estimate_component_people(
                component_ids=unlocked_component_ids,
                active_ids=set(),
                towers_by_id=towers_by_id,
            )
            next_connection_ids = tuple(
                sorted(
                    other_id
                    for other_id in remaining_ids
                    if other_id != tower_id
                    and any(neighbor_id == tower_id for neighbor_id in adjacency.get(other_id, {}))
                    and not any(
                        neighbor_id in active_detached_ids
                        for neighbor_id in adjacency.get(other_id, {})
                    )
                )
            )
            choice_score = (
                len(previous_connection_ids),
                impact_people_est,
                len(unlocked_component_ids),
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

        assert best_choice is not None, "Detached rollout selection unexpectedly produced no candidate."
        _, tower_id, previous_connection_ids, next_connection_ids = best_choice
        tower = towers_by_id[tower_id]
        unlocked_component_ids = component_members(
            start_id=tower_id,
            allowed_ids=remaining_ids,
            adjacency=adjacency,
        )
        impact_people_est = estimate_component_people(
            component_ids=unlocked_component_ids,
            active_ids=set(),
            towers_by_id=towers_by_id,
        )
        plan_rows.append(
            PlanRow(
                cluster_key=cluster_key,
                cluster_label=cluster_label,
                cluster_install_rank=rank,
                is_next_for_cluster=first_candidate,
                rollout_status="next" if first_candidate else "planned",
                installed=False,
                tower_id=tower.tower_id,
                label=tower.label,
                source=tower.source,
                impact_score=impact_people_est,
                impact_tower_count=len(unlocked_component_ids),
                next_unlock_count=len(next_connection_ids),
                backlink_count=len(previous_connection_ids),
                previous_connection_ids=previous_connection_ids,
                next_connection_ids=next_connection_ids,
                lon=tower.lon,
                lat=tower.lat,
            )
        )
        active_detached_ids.add(tower_id)
        remaining_ids.remove(tower_id)
        rank += 1
        first_candidate = False

    return plan_rows
