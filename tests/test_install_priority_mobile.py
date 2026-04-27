"""Mobile-layout and accessibility tests for the installer-priority handout."""

from __future__ import annotations

import unittest

from scripts.install_priority_lib import render_html_document


def sample_row(**overrides: object) -> dict[str, object]:
    """Build one minimal handout row for renderer-focused tests."""

    row = {
        "cluster_key": "ge:seed:1",
        "cluster_label": "Georgia / Batumi",
        "cluster_install_rank": 0,
        "is_next_for_cluster": False,
        "rollout_status": "installed",
        "installed": True,
        "tower_id": 1,
        "label": "Batumi",
        "display_name": "Batumi",
        "display_type": "Installed Seed",
        "source": "seed",
        "impact_score": 0,
        "impact_people_est": 0,
        "impact_tower_count": 0,
        "next_unlock_count": 0,
        "backlink_count": 0,
        "primary_previous_tower_id": "",
        "inter_cluster_neighbor_ids": "",
        "inter_cluster_connections": "",
        "blocked_reason": "",
        "previous_connections": "",
        "next_connections": "",
        "lon": 41.60,
        "lat": 41.70,
        "location_status": "ok",
        "location_en": "Batumi, Adjara, Georgia",
        "location_ru": "Батуми, Аджария, Грузия",
        "google_maps_url": "https://maps.google.com/?q=41.700000,41.600000",
        "osm_url": "https://www.openstreetmap.org/?mlat=41.700000&mlon=41.600000#map=14/41.700000/41.600000",
    }
    row.update(overrides)

    return row


class InstallPriorityMobileTests(unittest.TestCase):
    """Check mobile-specific HTML behavior and accessible markup."""

    def test_html_includes_accessible_summary_table_and_mobile_cards(self) -> None:
        """The handout should keep semantic tables and add card views for small screens."""

        html_text = render_html_document(
            [
                sample_row(),
                sample_row(
                    cluster_install_rank=1,
                    is_next_for_cluster=True,
                    rollout_status="next",
                    installed=False,
                    tower_id=2,
                    label="route #2",
                    display_name="Mtirala ridge / Mtirala access road",
                    display_type="Route 2",
                    impact_people_est=2500,
                    impact_tower_count=4,
                    previous_connections="Batumi",
                    next_connections="Chakvi ridge, Kintrishi road",
                    blocked_reason="",
                    lon=41.68,
                    lat=41.81,
                    location_en="Mtirala ridge, near Mtirala access road, Adjara, Georgia",
                    location_ru="Хребет Мтирала, рядом с дорогой к Мтирале, Аджария, Грузия",
                ),
            ],
            generated_at="2026-04-09 15:30:00 +04",
            geocoder_base_url="https://geocoder.batu.market",
        )

        self.assertIn(
            "<caption class='sr-only'>Next suggested node for each currently active rollout cluster.</caption>",
            html_text,
            msg="Summary table should include a caption so screen readers can identify what the comparison table contains.",
        )
        self.assertIn(
            "<th scope='row'>Georgia / Batumi</th>",
            html_text,
            msg="Summary rows should expose the cluster name as a row header for easier table navigation.",
        )
        self.assertIn(
            "<th scope='row' class='name-header'>",
            html_text,
            msg="Cluster detail tables should expose the node name as the row header instead of leaving every detail row unlabeled.",
        )
        self.assertIn(
            "class='cluster-card-list'",
            html_text,
            msg="Cluster detail should include mobile card markup so narrow screens do not have to read a ten-column table.",
        )
        self.assertIn(
            "Show this phase on overview map",
            html_text,
            msg="Cluster detail should focus the shared overview map instead of embedding a separate mini map.",
        )
        self.assertIn(
            ".cluster-table-wrap{display:none}.cluster-cards{display:block}",
            html_text,
            msg="Responsive CSS should switch from the wide table to mobile cards on narrow screens.",
        )

    def test_html_uses_one_shared_map_on_mobile(self) -> None:
        """The report should use one MapLibre context and switch its data for cluster drilldown."""

        html_text = render_html_document(
            [
                sample_row(),
                sample_row(
                    cluster_install_rank=1,
                    is_next_for_cluster=True,
                    rollout_status="next",
                    installed=False,
                    tower_id=2,
                    label="route #2",
                    display_name="Mtirala ridge / Mtirala access road",
                    display_type="Route 2",
                    impact_people_est=2500,
                    impact_tower_count=4,
                    previous_connections="Batumi",
                    next_connections="Chakvi ridge, Kintrishi road",
                    lon=41.68,
                    lat=41.81,
                ),
            ],
            generated_at="2026-04-09 15:30:00 +04",
            geocoder_base_url="https://geocoder.batu.market",
        )

        self.assertIn(
            ".order-marker.cluster{width:12px;height:12px;font-size:7px;border-width:1px",
            html_text,
            msg="Cluster-focused order badges should be smaller than overview badges on the shared map.",
        )
        self.assertIn(
            "mapMode === 'cluster' ? 4 : 7",
            html_text,
            msg="The shared map runtime should keep a cluster-focused marker sizing mode.",
        )
        self.assertIn(
            "addNodeLayers(overviewMap, 'overview-nodes', initialOverviewCollections.collection, 'overview')",
            html_text,
            msg="The overview map should still opt into the larger overview marker sizing path while using the active phase collection.",
        )
        self.assertIn(
            "overviewOrderMarkers = addOrderMarkers(overviewMap, collections.collection, scope === 'cluster' ? 'cluster' : 'overview')",
            html_text,
            msg="Cluster drilldown should reuse the single overview map and switch marker label sizing by scope.",
        )
        self.assertIn(
            "buildClusterCollections",
            html_text,
            msg="The runtime should build cluster-scoped collections for the shared map.",
        )
        self.assertNotIn(
            "fitToFeatures(clusterMap",
            html_text,
            msg="Cluster drilldown should not create or fit separate cluster maps.",
        )
        self.assertNotIn(
            ".cluster-map{",
            html_text,
            msg="Rendered HTML should not carry mini-map containers that would create extra WebGL contexts.",
        )
        self.assertIn(
            "attributionControl: false",
            html_text,
            msg="Mobile map rendering should disable the default expanded attribution banner so it does not cover the lower half of small cluster maps.",
        )
        self.assertIn(
            "new maplibregl.AttributionControl({ compact: true })",
            html_text,
            msg="The shared map should add compact attribution explicitly so credits stay visible without swallowing the map on phones.",
        )
        self.assertEqual(
            html_text.count("new maplibregl.Map"),
            1,
            msg=f"The handout should create exactly one MapLibre WebGL context, got runtime HTML {html_text!r}",
        )
        self.assertIn(
            "window.__installPriorityMaps = {",
            html_text,
            msg="The mobile handout should expose the shared map reference for phone-side debugging.",
        )
        self.assertNotIn(
            "activeClusterMaps",
            html_text,
            msg="The single-map report should not keep cluster-map WebGL state.",
        )
        self.assertNotIn(
            "new IntersectionObserver",
            html_text,
            msg="The single-map report should not lazily mount offscreen MapLibre maps.",
        )
        self.assertNotIn(
            "mountClusterMap",
            html_text,
            msg="The report should switch data on the shared map instead of mounting cluster maps.",
        )
        self.assertNotIn(
            "syncVisibleClusterMaps",
            html_text,
            msg="The old mini-map sync path should be gone with the extra WebGL contexts.",
        )


if __name__ == "__main__":
    unittest.main()
