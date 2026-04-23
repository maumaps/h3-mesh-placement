"""Unit tests for the installer-priority handout helpers."""

from __future__ import annotations

import unittest

from scripts.install_priority_connectors import select_inter_cluster_connectors
from scripts.install_priority_cluster_helpers import pending_connector_ids
from scripts.install_priority_lib import (
    EndpointObservation,
    TowerRecord,
    build_adjacency,
    build_cluster_plan,
    reconstruct_tower_points,
)


class InstallPriorityTests(unittest.TestCase):
    """Verify graph planning and rendering for the handout export."""

    def test_reconstruct_tower_points_collapses_consistent_endpoints(self) -> None:
        """Consistent edge endpoints should reconstruct one stable tower point."""

        observations = [
            EndpointObservation(tower_id=10, lon=41.700000, lat=41.800000),
            EndpointObservation(tower_id=10, lon=41.700001, lat=41.800001),
            EndpointObservation(tower_id=11, lon=41.710000, lat=41.810000),
        ]

        tower_points = reconstruct_tower_points(observations, tolerance_m=200.0)

        self.assertEqual(
            set(tower_points),
            {10, 11},
            msg=f"Expected both tower ids to survive reconstruction, got {tower_points!r}",
        )
        self.assertAlmostEqual(
            tower_points[10][0],
            41.7000005,
            places=6,
            msg=f"Tower 10 longitude should be averaged from consistent observations, got {tower_points[10][0]!r}",
        )
        self.assertAlmostEqual(
            tower_points[10][1],
            41.8000005,
            places=6,
            msg=f"Tower 10 latitude should be averaged from consistent observations, got {tower_points[10][1]!r}",
        )

    def test_reconstruct_tower_points_rejects_divergent_endpoints(self) -> None:
        """Large endpoint disagreement should fail loudly."""

        observations = [
            EndpointObservation(tower_id=12, lon=41.700000, lat=41.800000),
            EndpointObservation(tower_id=12, lon=41.720000, lat=41.820000),
        ]

        with self.assertRaisesRegex(
            ValueError,
            "Tower 12 endpoint observations diverge",
            msg=f"Divergent observations should raise a clear reconstruction error, got {observations!r}",
        ):
            reconstruct_tower_points(observations, tolerance_m=50.0)

    def test_build_cluster_plan_picks_a_different_next_node_per_seed_cluster(self) -> None:
        """Separate seed clusters should keep separate local next-step queues."""

        towers_by_id = {
            1: TowerRecord(1, "seed", 41.60, 41.70, "Batumi", True),
            2: TowerRecord(2, "seed", 44.70, 41.80, "Tbilisi", True),
            3: TowerRecord(3, "route", 41.61, 41.71, "route #3", False, people_estimate=1200),
            4: TowerRecord(4, "route", 44.71, 41.81, "route #4", False, people_estimate=1100),
            5: TowerRecord(5, "cluster_slim", 41.62, 41.72, "cluster_slim #5", False, people_estimate=900),
            6: TowerRecord(6, "cluster_slim", 44.72, 41.82, "cluster_slim #6", False, people_estimate=800),
        }
        adjacency = build_adjacency(
            [
                (1, 3, 1000.0),
                (3, 5, 1200.0),
                (2, 4, 900.0),
                (4, 6, 1100.0),
            ]
        )

        plan_rows = build_cluster_plan(towers_by_id, adjacency)
        next_rows = {
            row.cluster_label: row.label
            for row in plan_rows
            if row.is_next_for_cluster
        }

        self.assertEqual(
            next_rows,
            {"Batumi": "route #3", "Tbilisi": "route #4"},
            msg=f"Each installed seed cluster should get its own next node, got {next_rows!r} from rows {plan_rows!r}",
        )

    def test_build_cluster_plan_prefers_more_people_even_if_route_exists(self) -> None:
        """Impact is now estimated people reach, so a higher-reach cluster node can beat a route node."""

        towers_by_id = {
            1: TowerRecord(1, "seed", 41.60, 41.70, "Batumi", True),
            2: TowerRecord(
                2,
                "route",
                41.61,
                41.71,
                "route #2",
                False,
                people_estimate=1000,
                population_place_id="node:route-town",
            ),
            3: TowerRecord(
                3,
                "cluster_slim",
                41.62,
                41.72,
                "cluster_slim #3",
                False,
                people_estimate=2200,
                population_place_id="node:big-village",
            ),
        }
        adjacency = build_adjacency(
            [
                (1, 2, 1000.0),
                (1, 3, 1000.0),
            ]
        )

        plan_rows = build_cluster_plan(towers_by_id, adjacency)
        next_row = next(
            row for row in plan_rows if row.is_next_for_cluster
        )

        self.assertEqual(
            next_row.tower_id,
            3,
            msg=f"Estimated newly reachable people should outrank type preference, got next row {next_row!r}",
        )
        self.assertEqual(
            next_row.impact_score,
            2200,
            msg=f"Impact score should now represent estimated people reach, got next row {next_row!r}",
        )

    def test_build_cluster_plan_assigns_shared_frontier_to_nearest_seed_cluster(self) -> None:
        """Shared frontier towers should be claimed by the nearest seed cluster deterministically."""

        towers_by_id = {
            1: TowerRecord(1, "seed", 41.60, 41.70, "Batumi", True),
            2: TowerRecord(2, "seed", 44.70, 41.80, "Tbilisi", True),
            3: TowerRecord(3, "route", 42.20, 41.75, "route #3", False),
            4: TowerRecord(4, "route", 42.30, 41.76, "route #4", False),
        }
        adjacency = build_adjacency(
            [
                (1, 3, 1000.0),
                (2, 3, 2000.0),
                (3, 4, 900.0),
            ]
        )

        plan_rows = build_cluster_plan(towers_by_id, adjacency)
        route_three_row = next(
            row for row in plan_rows if row.tower_id == 3
        )
        route_four_row = next(
            row for row in plan_rows if row.tower_id == 4
        )

        self.assertEqual(
            route_three_row.cluster_label,
            "Batumi",
            msg=f"Route tower 3 should attach to the nearest seed cluster, got row {route_three_row!r}",
        )
        self.assertEqual(
            route_four_row.cluster_label,
            "Batumi",
            msg=f"Downstream tower 4 should follow tower 3 into the same assigned cluster, got row {route_four_row!r}",
        )

    def test_build_cluster_plan_prefers_shorter_visible_distance_over_fewer_hops(self) -> None:
        """A frontier tower should follow the cheaper visible path even if another seed is fewer hops away."""

        towers_by_id = {
            3: TowerRecord(3, "seed", 44.70, 41.70, "Tbilisi", True, country_code="ge", country_name="Georgia"),
            8: TowerRecord(8, "seed", 44.50, 40.17, "Yerevan", True, country_code="am", country_name="Armenia"),
            17: TowerRecord(17, "route", 44.72, 41.60, "route #17", False, country_code="ge", country_name="Georgia"),
            23: TowerRecord(23, "route", 44.30, 40.50, "route #23", False, country_code="am", country_name="Armenia"),
            30: TowerRecord(30, "route", 44.60, 41.00, "route #30", False, country_code="ge", country_name="Georgia"),
            32: TowerRecord(32, "route", 44.20, 40.70, "route #32", False, country_code="am", country_name="Armenia"),
        }
        adjacency = build_adjacency(
            [
                (3, 17, 11823.0),
                (17, 30, 43648.4),
                (30, 32, 76372.3),
                (8, 23, 1000.0),
                (23, 32, 44819.6),
            ]
        )

        plan_rows = build_cluster_plan(towers_by_id, adjacency)
        route_thirty_two_row = next(
            row for row in plan_rows if row.tower_id == 32
        )

        self.assertEqual(
            route_thirty_two_row.cluster_label,
            "Yerevan",
            msg=f"Route tower 32 should stay with the cheaper Yerevan visible corridor instead of the longer Tbilisi path, got row {route_thirty_two_row!r}",
        )
        self.assertEqual(
            route_thirty_two_row.previous_connection_ids,
            (23,),
            msg=f"Route tower 32 should keep the Armenian visible predecessor once cluster assignment follows weighted distance, got row {route_thirty_two_row!r}",
        )

    def test_build_cluster_plan_allows_cross_country_rollout_links(self) -> None:
        """Cross-border links should still stay eligible across separate country-local queues."""

        towers_by_id = {
            1: TowerRecord(1, "seed", 41.60, 41.70, "Batumi", True, country_code="ge", country_name="Georgia"),
            2: TowerRecord(2, "route", 41.70, 41.75, "route #2", False, country_code="ge", country_name="Georgia"),
            3: TowerRecord(3, "route", 44.52, 40.18, "route #3", False, country_code="am", country_name="Armenia"),
            4: TowerRecord(4, "seed", 44.50, 40.17, "Yerevan", True, country_code="am", country_name="Armenia"),
        }
        adjacency = build_adjacency(
            [
                (1, 2, 1000.0),
                (2, 3, 800.0),
                (4, 3, 900.0),
            ]
        )

        plan_rows = build_cluster_plan(towers_by_id, adjacency)
        next_rows = {
            row.cluster_label: row.tower_id
            for row in plan_rows
            if row.is_next_for_cluster
        }

        self.assertEqual(
            next_rows,
            {"Batumi": 2, "Yerevan": 3},
            msg=f"Cross-border visibility should still allow separate queues to progress toward each other, got {next_rows!r} from rows {plan_rows!r}",
        )

    def test_build_cluster_plan_keeps_visible_cross_border_chain_connected(self) -> None:
        """A visible chain should stay attached to the seed it can actually reach."""

        towers_by_id = {
            1: TowerRecord(1, "seed", 41.60, 41.70, "Batumi", True, country_code="ge", country_name="Georgia"),
            2: TowerRecord(2, "route", 41.70, 41.75, "route #2", False, country_code="ge", country_name="Georgia"),
            3: TowerRecord(3, "route", 44.40, 40.10, "route #3", False, country_code="am", country_name="Armenia"),
            4: TowerRecord(4, "seed", 44.50, 40.17, "Yerevan", True, country_code="am", country_name="Armenia"),
        }
        adjacency = build_adjacency(
            [
                (1, 2, 1000.0),
                (2, 3, 800.0),
            ]
        )

        plan_rows = build_cluster_plan(towers_by_id, adjacency)
        connected_row = next(
            row for row in plan_rows if row.tower_id == 3
        )

        self.assertEqual(
            connected_row.cluster_label,
            "Batumi",
            msg=f"Cross-border visible tower should stay with the seed cluster reached through visible edges, got row {connected_row!r}",
        )
        self.assertEqual(
            connected_row.rollout_status,
            "planned",
            msg=f"Cross-border visible tower should remain a normal planned continuation, got row {connected_row!r}",
        )
        self.assertEqual(
            connected_row.previous_connection_ids,
            (2,),
            msg=f"Cross-border visible tower should keep its visible predecessor in the plan, got row {connected_row!r}",
        )
        self.assertNotIn(
            "blocked",
            {row.rollout_status for row in plan_rows},
            msg=f"Installer plan should not expose blocked rollout nodes, got rows {plan_rows!r}",
        )

    def test_build_cluster_plan_prioritizes_cluster_connection_before_reach(self) -> None:
        """A boundary node that joins another cluster should outrank a higher-reach internal option."""

        towers_by_id = {
            1: TowerRecord(1, "seed", 41.60, 41.70, "Batumi", True, country_code="ge", country_name="Georgia"),
            2: TowerRecord(2, "route", 41.61, 41.71, "route #2", False, people_estimate=100, country_code="ge", country_name="Georgia"),
            3: TowerRecord(3, "cluster_slim", 41.62, 41.72, "cluster_slim #3", False, people_estimate=5000, country_code="ge", country_name="Georgia"),
            10: TowerRecord(10, "seed", 42.10, 42.20, "Tbilisi", True, country_code="ge", country_name="Georgia"),
            11: TowerRecord(11, "route", 42.11, 42.21, "route #11", False, people_estimate=100, country_code="ge", country_name="Georgia"),
        }
        adjacency = build_adjacency(
            [
                (1, 2, 1000.0),
                (1, 3, 1000.0),
                (10, 11, 1000.0),
                (2, 11, 900.0),
            ]
        )

        plan_rows = build_cluster_plan(towers_by_id, adjacency)
        next_rows = {
            row.cluster_label: row.tower_id
            for row in plan_rows
            if row.is_next_for_cluster
        }

        self.assertEqual(
            next_rows["Batumi"],
            2,
            msg=f"Batumi queue should choose the cluster-joining boundary node before the higher-reach internal node, got next rows {next_rows!r} from rows {plan_rows!r}",
        )

    def test_build_cluster_plan_can_choose_cross_country_cluster_join(self) -> None:
        """Cross-country joins should still be eligible once no same-country join beats them."""

        towers_by_id = {
            1: TowerRecord(1, "seed", 43.80, 40.79, "Gyumri", True, country_code="am", country_name="Armenia"),
            2: TowerRecord(2, "route", 43.90, 40.80, "route #2", False, people_estimate=100, country_code="am", country_name="Armenia"),
            3: TowerRecord(3, "route", 44.00, 40.81, "route #3", False, people_estimate=100, country_code="am", country_name="Armenia"),
            9: TowerRecord(9, "seed", 44.50, 40.17, "Yerevan", True, country_code="am", country_name="Armenia"),
            10: TowerRecord(10, "route", 44.10, 40.50, "route #10", False, people_estimate=100, country_code="am", country_name="Armenia"),
            20: TowerRecord(20, "seed", 44.90, 41.70, "Tbilisi", True, country_code="ge", country_name="Georgia"),
            21: TowerRecord(21, "route", 44.20, 40.90, "route #21", False, people_estimate=100, country_code="ge", country_name="Georgia"),
        }
        adjacency = build_adjacency(
            [
                (1, 2, 1000.0),
                (2, 3, 1000.0),
                (9, 10, 1000.0),
                (3, 10, 1000.0),
                (2, 21, 1000.0),
                (20, 21, 1000.0),
            ]
        )

        plan_rows = build_cluster_plan(towers_by_id, adjacency)
        next_rows = {
            row.cluster_label: row.tower_id
            for row in plan_rows
            if row.is_next_for_cluster
        }

        self.assertEqual(
            next_rows["Gyumri"],
            2,
            msg=f"Gyumri queue should still start toward the best visible connector when the best remaining join is cross-country, got next rows {next_rows!r} from rows {plan_rows!r}",
        )

    def test_pending_connector_ids_prefers_lowest_cost_join_even_cross_country(self) -> None:
        """Connector targeting should not bias the rollout toward same-country peers."""

        towers_by_id = {
            1: TowerRecord(1, "seed", 41.60, 41.70, "Batumi", True, country_code="ge", country_name="Georgia"),
            2: TowerRecord(2, "route", 41.61, 41.71, "route #2", False, country_code="ge", country_name="Georgia"),
            10: TowerRecord(10, "seed", 42.10, 42.20, "Tbilisi", True, country_code="ge", country_name="Georgia"),
            11: TowerRecord(11, "route", 42.11, 42.21, "route #11", False, country_code="ge", country_name="Georgia"),
            12: TowerRecord(12, "route", 42.12, 42.22, "route #12", False, country_code="ge", country_name="Georgia"),
            20: TowerRecord(20, "seed", 43.80, 40.79, "Gyumri", True, country_code="am", country_name="Armenia"),
            21: TowerRecord(21, "route", 43.81, 40.80, "route #21", False, country_code="am", country_name="Armenia"),
        }
        full_adjacency = build_adjacency(
            [
                (10, 11, 1000.0),
                (10, 12, 1000.0),
                (11, 2, 8000.0),
                (12, 21, 1000.0),
                (1, 2, 1000.0),
                (20, 21, 1000.0),
            ]
        )

        cluster_by_tower_id = {
            1: "seed:1",
            2: "seed:1",
            10: "seed:10",
            11: "seed:10",
            12: "seed:10",
            20: "seed:20",
            21: "seed:20",
        }
        target_boundary_ids = pending_connector_ids(
            active_ids={10},
            cluster_by_tower_id=cluster_by_tower_id,
            cluster_key="seed:10",
            full_adjacency=full_adjacency,
            remaining_ids={11, 12},
            towers_by_id=towers_by_id,
        )

        self.assertEqual(
            target_boundary_ids,
            {12},
            msg=f"Connector targeting should choose the cheapest join boundary even when it crosses a country boundary, got {target_boundary_ids!r}",
        )

    def test_build_cluster_plan_keeps_boundary_node_with_cheapest_seed_cluster(self) -> None:
        """A contested boundary node should stay with the seed cluster that reaches it more cheaply."""

        towers_by_id = {
            1: TowerRecord(1, "seed", 41.60, 41.70, "Batumi", True, country_code="ge", country_name="Georgia"),
            2: TowerRecord(2, "route", 41.61, 41.71, "route #2", False, people_estimate=100, country_code="ge", country_name="Georgia"),
            20: TowerRecord(20, "route", 41.62, 41.72, "route #20", False, people_estimate=100, country_code="ge", country_name="Georgia"),
            10: TowerRecord(10, "seed", 42.10, 42.20, "Tbilisi", True, country_code="ge", country_name="Georgia"),
            11: TowerRecord(11, "route", 42.11, 42.21, "route #11", False, people_estimate=100, country_code="ge", country_name="Georgia"),
            21: TowerRecord(21, "route", 42.12, 42.22, "route #21", False, people_estimate=100, country_code="ge", country_name="Georgia"),
        }
        adjacency = build_adjacency(
            [
                (1, 2, 1000.0),
                (1, 20, 5000.0),
                (10, 11, 100000.0),
                (10, 21, 1000.0),
                (2, 11, 1000.0),
                (20, 21, 1000.0),
            ]
        )

        plan_rows = build_cluster_plan(towers_by_id, adjacency)
        next_rows = {
            row.cluster_label: row.tower_id
            for row in plan_rows
            if row.is_next_for_cluster
        }
        route_twenty_row = next(
            row for row in plan_rows if row.tower_id == 20
        )

        self.assertEqual(
            next_rows,
            {"Batumi": 2, "Tbilisi": 21},
            msg=f"Queues should keep a contested boundary node with the cheaper seed-side corridor, got next rows {next_rows!r} from rows {plan_rows!r}",
        )
        self.assertEqual(
            route_twenty_row.cluster_label,
            "Tbilisi",
            msg=f"Route tower 20 should stay with the cheaper Tbilisi-side visible path instead of being claimed by Batumi, got row {route_twenty_row!r}",
        )

    def test_build_cluster_plan_prefers_route_corridor_proxy_when_distance_only_is_misleading(self) -> None:
        """Route-derived connectors should outrank a slightly shorter cluster-slim bridge."""

        towers_by_id = {
            1: TowerRecord(1, "seed", 43.80, 40.79, "Gyumri", True, country_code="am", country_name="Armenia"),
            51: TowerRecord(51, "route", 43.82, 40.80, "route #51", False, country_code="am", country_name="Armenia"),
            61: TowerRecord(61, "route", 43.83, 40.81, "route #61", False, country_code="am", country_name="Armenia"),
            79: TowerRecord(79, "cluster_slim", 43.84, 40.82, "cluster_slim #79", False, country_code="am", country_name="Armenia"),
            9: TowerRecord(9, "seed", 44.50, 40.17, "Yerevan", True, country_code="am", country_name="Armenia"),
            52: TowerRecord(52, "route", 44.30, 40.30, "route #52", False, country_code="am", country_name="Armenia"),
            53: TowerRecord(53, "route", 44.31, 40.29, "route #53", False, country_code="am", country_name="Armenia"),
            78: TowerRecord(78, "cluster_slim", 44.20, 40.31, "cluster_slim #78", False, country_code="am", country_name="Armenia"),
        }
        adjacency = build_adjacency(
            [
                (1, 51, 1000.0),
                (1, 61, 1200.0),
                (61, 79, 900.0),
                (9, 53, 1000.0),
                (53, 52, 900.0),
                (9, 78, 1200.0),
                (51, 52, 43068.1),
                (79, 78, 39722.7),
            ]
        )

        plan_rows = build_cluster_plan(towers_by_id, adjacency)
        next_rows = {
            row.cluster_label: row.tower_id
            for row in plan_rows
            if row.is_next_for_cluster
        }

        self.assertEqual(
            next_rows,
            {"Gyumri": 51, "Yerevan": 53},
            msg=f"Route-derived connector proxy should start both queues on the route corridor instead of the slightly shorter cluster-slim bridge, got next rows {next_rows!r} from rows {plan_rows!r}",
        )

    def test_select_inter_cluster_connectors_uses_lowest_total_join_cost(self) -> None:
        """Inter-cluster context should highlight the cheapest seed-to-seed join corridor."""

        towers_by_id = {
            1: TowerRecord(1, "seed", 41.60, 41.70, "Batumi", True, country_code="ge", country_name="Georgia"),
            2: TowerRecord(2, "route", 41.61, 41.71, "route #2", False, country_code="ge", country_name="Georgia"),
            3: TowerRecord(3, "route", 41.62, 41.72, "route #3", False, country_code="ge", country_name="Georgia"),
            4: TowerRecord(4, "route", 41.63, 41.73, "route #4", False, country_code="ge", country_name="Georgia"),
            10: TowerRecord(10, "seed", 42.10, 42.20, "Tbilisi", True, country_code="ge", country_name="Georgia"),
            11: TowerRecord(11, "route", 42.11, 42.21, "route #11", False, country_code="ge", country_name="Georgia"),
            12: TowerRecord(12, "route", 42.12, 42.22, "route #12", False, country_code="ge", country_name="Georgia"),
            13: TowerRecord(13, "route", 42.13, 42.23, "route #13", False, country_code="ge", country_name="Georgia"),
        }
        adjacency = build_adjacency(
            [
                (1, 2, 10.0),
                (2, 3, 10.0),
                (3, 4, 10.0),
                (10, 11, 10.0),
                (11, 12, 10.0),
                (12, 13, 10.0),
                (4, 13, 100.0),
                (3, 12, 1000.0),
            ]
        )

        plan_rows = build_cluster_plan(towers_by_id, adjacency)
        connectors = select_inter_cluster_connectors(plan_rows, adjacency)
        connector = next(
            item for item in connectors
            if {item.left_cluster_label, item.right_cluster_label} == {"Batumi", "Tbilisi"}
        )

        self.assertEqual(
            {connector.left_tower_id, connector.right_tower_id},
            {4, 13},
            msg=f"Connector overlay should use the lowest total seed-to-seed join cost, got {connector!r} from rows {plan_rows!r}",
        )

    def test_select_inter_cluster_connectors_includes_cross_country_pairs(self) -> None:
        """Context connectors should include cross-country pairs now that rollout is global."""

        towers_by_id = {
            1: TowerRecord(1, "seed", 43.80, 40.79, "Gyumri", True, country_code="am", country_name="Armenia"),
            2: TowerRecord(2, "route", 43.90, 40.80, "route #2", False, country_code="am", country_name="Armenia"),
            9: TowerRecord(9, "seed", 44.50, 40.17, "Yerevan", True, country_code="am", country_name="Armenia"),
            10: TowerRecord(10, "route", 44.10, 40.50, "route #10", False, country_code="am", country_name="Armenia"),
            20: TowerRecord(20, "seed", 44.90, 41.70, "Tbilisi", True, country_code="ge", country_name="Georgia"),
            21: TowerRecord(21, "route", 44.20, 40.90, "route #21", False, country_code="ge", country_name="Georgia"),
        }
        adjacency = build_adjacency(
            [
                (1, 2, 1000.0),
                (9, 10, 1000.0),
                (2, 10, 900.0),
                (2, 21, 400.0),
                (20, 21, 1000.0),
            ]
        )

        plan_rows = build_cluster_plan(towers_by_id, adjacency)
        connectors = select_inter_cluster_connectors(plan_rows, adjacency)
        connector_pairs = {
            frozenset({connector.left_cluster_label, connector.right_cluster_label})
            for connector in connectors
        }

        self.assertEqual(
            connector_pairs,
            {
                frozenset({"Gyumri", "Yerevan"}),
                frozenset({"Gyumri", "Tbilisi"}),
            },
            msg=f"Connector overlay should include cross-country cluster pairs when they are visible, got {connector_pairs!r} from connectors {connectors!r}",
        )

if __name__ == "__main__":
    unittest.main()
