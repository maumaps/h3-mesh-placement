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
            "role='img' aria-label='Map for Georgia / Batumi rollout cluster'",
            html_text,
            msg="Each cluster mini map should include an accessible label describing which rollout cluster it shows.",
        )
        self.assertIn(
            ".cluster-table-wrap{display:none}.cluster-cards{display:block}",
            html_text,
            msg="Responsive CSS should switch from the wide table to mobile cards on narrow screens.",
        )

    def test_html_distinguishes_cluster_marker_sizes_from_overview(self) -> None:
        """Mini maps should use smaller node visuals than the overview map."""

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
            msg="Cluster map badges should be smaller than overview badges so mini maps stay legible on phones.",
        )
        self.assertIn(
            "mapMode === 'cluster' ? 4 : 7",
            html_text,
            msg="Cluster point circles should use a smaller radius than overview points in the MapLibre bootstrap.",
        )
        self.assertIn(
            "addNodeLayers(overviewMap, 'overview-nodes', overviewCollection, 'overview')",
            html_text,
            msg="The overview map should still opt into the larger overview marker sizing path.",
        )
        self.assertIn(
            "clusterCollection, 'cluster'",
            html_text,
            msg="Cluster mini maps should explicitly opt into the smaller cluster marker sizing path.",
        )
        self.assertIn(
            "clusterRoutes.features.length ? [...clusterFeatures, ...clusterRoutes.features] : clusterFeatures",
            html_text,
            msg="Cluster mini maps should fit to their own points and rollout lines so huge Voronoi polygons do not zoom the later maps out to the country scale.",
        )
        self.assertNotIn(
            "fitToFeatures(clusterMap, [...clusterBounds.features, ...clusterFeatures, ...clusterContext.features])",
            html_text,
            msg="Cluster mini maps should not include context connector lines in their fit bounds because that makes later maps look empty.",
        )
        self.assertNotIn(
            "clusterBounds.features.length ? [...clusterBounds.features, ...clusterFeatures] : clusterFeatures",
            html_text,
            msg="Cluster mini maps should not fit to the full Voronoi polygon extent because those merged bounds can dwarf the local rollout geometry.",
        )
        self.assertLess(
            html_text.find("safeOverlayStep(`${cluster.map_id} bounds`"),
            html_text.find("safeOverlayStep(`${cluster.map_id} nodes`"),
            msg="Cluster bound polygons should be added before node circles so the lines and polygons stay visually underneath the point overlays.",
        )
        self.assertLess(
            html_text.find("safeOverlayStep(`${cluster.map_id} nodes`"),
            html_text.find("safeOverlayStep(`${cluster.map_id} routes`"),
            msg="Cluster rollout lines should be added after the node circles so the local path stays visible even in dense marker groups.",
        )
        self.assertIn(
            "requestAnimationFrame(() => { clusterMap.resize(); fitToFeatures(",
            html_text,
            msg="Cluster mini maps should resize after layout before fitting bounds so later maps do not keep stale canvas dimensions.",
        )
        self.assertIn(
            "attributionControl: false",
            html_text,
            msg="Mobile map rendering should disable the default expanded attribution banner so it does not cover the lower half of small cluster maps.",
        )
        self.assertIn(
            "new maplibregl.AttributionControl({ compact: true })",
            html_text,
            msg="Mini maps should add the compact attribution control explicitly so the required map credits stay visible without swallowing the map on phones.",
        )
        self.assertIn(
            "const activeClusterMaps = new Map();",
            html_text,
            msg="The mobile handout should keep explicit cluster-map state so later mini maps can be mounted and unmounted instead of leaking WebGL contexts.",
        )
        self.assertIn(
            "window.__installPriorityMaps = {",
            html_text,
            msg="The mobile handout should expose live map references on window so phone debugging can inspect which mini maps are currently mounted.",
        )
        self.assertIn(
            "prefersLazyClusterMaps = () => window.matchMedia('(max-width: 920px)').matches",
            html_text,
            msg="Cluster-map lazy mounting should activate only on narrow screens where mobile Chrome is most likely to run out of rendering resources.",
        )
        self.assertIn(
            "new IntersectionObserver((entries) => {",
            html_text,
            msg="Cluster mini maps should be lazily mounted with IntersectionObserver so offscreen maps do not all initialize at once on phones.",
        )
        self.assertIn(
            "clusterMap.remove();",
            html_text,
            msg="Offscreen cluster maps should be removed on mobile so later cards can render without inheriting stale or exhausted WebGL state.",
        )
        self.assertNotIn(
            "requestAnimationFrame(syncVisibleClusterMaps)",
            html_text,
            msg="Legacy requestAnimationFrame-only syncing should be gone because hidden mobile tabs can miss that callback and never mount later cluster maps.",
        )
        self.assertIn(
            "window.addEventListener('pageshow', syncVisibleClusterMaps)",
            html_text,
            msg="The mobile handout should resync lazy cluster maps when an in-app browser shows the page again after backgrounding it.",
        )
        self.assertIn(
            "document.addEventListener('visibilitychange', () => {",
            html_text,
            msg="The mobile handout should resync lazy cluster maps when the tab becomes visible so backgrounded Telegram or Chrome tabs still mount their overlays.",
        )
        self.assertIn(
            "window.__installPriorityMaps.syncVisibleClusterMaps = syncVisibleClusterMaps;",
            html_text,
            msg="Phone-side debugging should be able to invoke the same cluster-map sync path that the runtime uses when overlays go missing.",
        )
        self.assertNotIn(
            "requestAnimationFrame(syncVisibleClusterMaps);",
            html_text,
            msg="Initial cluster-map mounting should not rely only on requestAnimationFrame because that callback can be throttled away in hidden mobile tabs.",
        )


if __name__ == "__main__":
    unittest.main()
