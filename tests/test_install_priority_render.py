"""Rendering and presentation tests for the installer-priority handout."""

from __future__ import annotations

import unittest

from scripts.export_install_priority import (
    build_output_row,
    choose_primary_previous_tower_id,
)
from scripts.install_priority_cluster_bounds import (
    fetch_cluster_bound_features,
)
from scripts.install_priority_enrichment import enrich_tower_records
from scripts.install_priority_lib import (
    CSV_COLUMNS,
    PlanRow,
    TowerRecord,
    build_display_name,
    format_location_description,
    render_html_document,
)


class InstallPriorityRenderTests(unittest.TestCase):
    """Verify display labels, output rows, and HTML rendering."""

    def test_cluster_bound_query_uses_geodesic_buffer_mask_for_voronoi_clip(self) -> None:
        """Voronoi bounds should be clipped by a meter-based geography buffer, not degree padding."""

        class FakeCursor:
            def __init__(self) -> None:
                self.query = ""
                self.params = []

            def execute(self, query, params) -> None:
                self.query = str(query)
                self.params = list(params)

            def fetchall(self):
                return [
                    (
                        "seed:1",
                        "Batumi",
                        '{"type":"Polygon","coordinates":[[[41.58,41.68],[41.63,41.68],[41.63,41.73],[41.58,41.73],[41.58,41.68]]]}',
                    )
                ]

        fake_cursor = FakeCursor()

        fetch_cluster_bound_features(
            fake_cursor,
            [
                {
                    "cluster_key": "seed:1",
                    "cluster_label": "Batumi",
                    "tower_id": 1,
                    "lon": 41.60,
                    "lat": 41.70,
                },
                {
                    "cluster_key": "seed:2",
                    "cluster_label": "Mtirala",
                    "tower_id": 2,
                    "lon": 41.72,
                    "lat": 41.84,
                },
            ],
        )

        self.assertIn(
            "ST_Buffer(",
            fake_cursor.query,
            msg=f"Cluster-bound query should build a buffered clip mask, got SQL {fake_cursor.query!r}",
        )
        self.assertIn(
            "::geography",
            fake_cursor.query,
            msg=f"Cluster-bound query should buffer in geography meters, got SQL {fake_cursor.query!r}",
        )
        self.assertIn(
            "ST_Intersection",
            fake_cursor.query,
            msg=f"Cluster-bound query should intersect Voronoi cells with the clip mask, got SQL {fake_cursor.query!r}",
        )
        self.assertNotIn(
            "ST_Expand(",
            fake_cursor.query,
            msg=f"Cluster-bound query should not fall back to degree-based ST_Expand padding, got SQL {fake_cursor.query!r}",
        )

    def test_single_point_cluster_bound_uses_geodesic_buffer(self) -> None:
        """A one-point cluster should still get a meter-based geodesic polygon."""

        class FakeCursor:
            def __init__(self) -> None:
                self.query = ""
                self.params = []

            def execute(self, query, params) -> None:
                self.query = str(query)
                self.params = list(params)

            def fetchone(self):
                return (
                    '{"type":"Polygon","coordinates":[[[41.59,41.69],[41.61,41.69],[41.61,41.71],[41.59,41.71],[41.59,41.69]]]}',
                )

        fake_cursor = FakeCursor()

        fetch_cluster_bound_features(
            fake_cursor,
            [
                {
                    "cluster_key": "seed:1",
                    "cluster_label": "Batumi",
                    "tower_id": 1,
                    "lon": 41.60,
                    "lat": 41.70,
                }
            ],
        )

        self.assertIn(
            "ST_Buffer(",
            fake_cursor.query,
            msg=f"Single-point cluster bounds should still come from a PostGIS buffer, got SQL {fake_cursor.query!r}",
        )
        self.assertIn(
            "::geography",
            fake_cursor.query,
            msg=f"Single-point cluster bounds should use geography meters, got SQL {fake_cursor.query!r}",
        )

    def test_choose_primary_previous_tower_id_prefers_latest_ranked_step(self) -> None:
        """Map route segments should follow the rollout corridor instead of the smallest id."""

        plan_row = PlanRow(
            cluster_key="am:seed:1",
            cluster_label="Armenia / Gyumri",
            cluster_install_rank=5,
            is_next_for_cluster=False,
            rollout_status="planned",
            installed=False,
            tower_id=74,
            label="cluster_slim #74",
            source="cluster_slim",
            impact_score=0,
            impact_tower_count=0,
            next_unlock_count=0,
            backlink_count=2,
            previous_connection_ids=(61, 73),
            next_connection_ids=(),
            lon=43.9,
            lat=40.7,
        )

        primary_previous_tower_id = choose_primary_previous_tower_id(
            plan_row,
            {
                61: 2,
                73: 4,
            },
        )

        self.assertEqual(
            primary_previous_tower_id,
            73,
            msg=f"Primary map predecessor should prefer the latest earlier rollout step, got {primary_previous_tower_id!r} for row {plan_row!r}",
        )

    def test_build_display_name_prefers_place_road_and_hides_raw_numbering(self) -> None:
        """Human display names should be location-first instead of raw source-number labels."""

        tower = TowerRecord(70, "cluster_slim", 42.3, 42.0, "cluster_slim #70", False)

        display_name = build_display_name(
            tower=tower,
            place_name="Zemo Abasha",
            road_name="Sajavakho - Chokhatauri - Ozurgeti - Kobuleti",
        )

        self.assertEqual(
            display_name,
            "Zemo Abasha / Sajavakho - Chokhatauri - Ozurgeti - Kobuleti",
            msg=f"Display names should lead with place and road, got {display_name!r}",
        )

    def test_format_location_description_prefers_place_and_road_over_address_style(self) -> None:
        """Field descriptions should read like road/place hints rather than postal addresses."""

        location_en = format_location_description(
            locale="en",
            road_name="Mtirala access road",
            place_name="Mtirala ridge",
            admin_context={
                "city": "Chakvi",
                "district": None,
                "province": "Adjara",
                "country": "Georgia",
            },
            lon=41.70,
            lat=41.80,
        )
        location_ru = format_location_description(
            locale="ru",
            road_name="Дорога к Мтирале",
            place_name="Хребет Мтирала",
            admin_context={
                "city": "Чакви",
                "district": None,
                "province": "Аджария",
                "country": "Грузия",
            },
            lon=41.70,
            lat=41.80,
        )

        self.assertEqual(
            location_en,
            "Mtirala ridge, near Mtirala access road, Chakvi, Adjara, Georgia",
            msg=f"English field description should combine place, road, and admin context, got {location_en!r}",
        )
        self.assertEqual(
            location_ru,
            "Хребет Мтирала, рядом с Дорога к Мтирале, Чакви, Аджария, Грузия",
            msg=f"Russian field description should combine place, road, and admin context, got {location_ru!r}",
        )

    def test_output_row_and_html_include_maplibre_and_true_next_summary_only(self) -> None:
        """Serialized rows and HTML should keep maps, human names, and only real next rows in the summary."""

        towers_by_id = enrich_tower_records(
            towers_by_id={
                1: TowerRecord(1, "seed", 41.60, 41.70, "Batumi", True),
                3: TowerRecord(3, "route", 41.61, 41.71, "route #3", False),
            },
            local_context_by_tower_id={
                1: {
                    "road_en": "Batumi road",
                    "road_ru": "Батумская дорога",
                    "place_en": "Batumi",
                    "place_ru": "Батуми",
                    "population_place_en": "Batumi",
                    "population_place_id": "node:batumi",
                    "population_est": 1000,
                },
                3: {
                    "road_en": "Mtirala access road",
                    "road_ru": "Дорога к Мтирале",
                    "place_en": "Mtirala ridge",
                    "place_ru": "Хребет Мтирала",
                    "population_place_en": "Chakvi",
                    "population_place_id": "node:chakvi",
                    "population_est": 2500,
                },
            },
        )
        next_plan_row = PlanRow(
            cluster_key="seed:1",
            cluster_label="Batumi",
            cluster_install_rank=1,
            is_next_for_cluster=True,
            rollout_status="next",
            installed=False,
            tower_id=3,
            label="route #3",
            source="route",
            impact_score=2500,
            impact_tower_count=2,
            next_unlock_count=1,
            backlink_count=1,
            previous_connection_ids=(1,),
            next_connection_ids=(),
            lon=41.610000,
            lat=41.710000,
        )
        installed_plan_row = PlanRow(
            cluster_key="seed:1",
            cluster_label="Batumi",
            cluster_install_rank=0,
            is_next_for_cluster=False,
            rollout_status="installed",
            installed=True,
            tower_id=1,
            label="Batumi",
            source="seed",
            impact_score=0,
            impact_tower_count=0,
            next_unlock_count=0,
            backlink_count=0,
            previous_connection_ids=(),
            next_connection_ids=(),
            lon=41.600000,
            lat=41.700000,
        )

        output_rows = [
            build_output_row(
                plan_row=installed_plan_row,
                towers_by_id=towers_by_id,
                local_context={
                    "road_en": "Batumi road",
                    "road_ru": "Батумская дорога",
                    "place_en": "Batumi",
                    "place_ru": "Батуми",
                },
                admin_context_en={
                    "city": "Batumi",
                    "district": None,
                    "province": "Adjara",
                    "country": "Georgia",
                },
                admin_context_ru={
                    "city": "Батуми",
                    "district": None,
                    "province": "Аджария",
                    "country": "Грузия",
                },
                geocoder_status_en="ok",
                geocoder_status_ru="ok",
            ),
            build_output_row(
                plan_row=next_plan_row,
                towers_by_id=towers_by_id,
                local_context={
                    "road_en": "Mtirala access road",
                    "road_ru": "Дорога к Мтирале",
                    "place_en": "Mtirala ridge",
                    "place_ru": "Хребет Мтирала",
                },
                admin_context_en={
                    "city": "Chakvi",
                    "district": None,
                    "province": "Adjara",
                    "country": "Georgia",
                },
                admin_context_ru={
                    "city": "Чакви",
                    "district": None,
                    "province": "Аджария",
                    "country": "Грузия",
                },
                geocoder_status_en="ok",
                geocoder_status_ru="ok",
            ),
        ]
        html_text = render_html_document(
            rows=output_rows,
            generated_at="2026-04-09T00:00:00+00:00",
            geocoder_base_url="https://geocoder.batu.market",
            cluster_bound_features=[
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [41.58, 41.68],
                            [41.63, 41.68],
                            [41.63, 41.73],
                            [41.58, 41.73],
                            [41.58, 41.68],
                        ]],
                    },
                    "properties": {
                        "cluster_key": "seed:1",
                        "cluster_label": "Batumi",
                    },
                }
            ],
            seed_mqtt_points=[
                {
                    "h3": "882c2026d7fffff",
                    "name": "Feria 2",
                    "source": "seed",
                    "marker": "S",
                    "lon": 41.60,
                    "lat": 41.70,
                    "country_code": "ge",
                    "country_name": "Georgia",
                },
                {
                    "h3": "882c01da1bfffff",
                    "name": "Yerevan MQTT",
                    "source": "mqtt",
                    "marker": "M",
                    "lon": 44.52,
                    "lat": 40.18,
                    "country_code": "am",
                    "country_name": "Armenia",
                },
            ],
            seed_mqtt_links=[
                {
                    "geometry": "{\"type\":\"LineString\",\"coordinates\":[[41.60,41.70],[41.61,41.71]]}",
                    "source_h3": "882c2026d7fffff",
                    "target_h3": "882c20275dfffff",
                }
            ],
        )

        self.assertEqual(
            set(output_rows[1]),
            set(CSV_COLUMNS),
            msg=f"Output row should expose the full CSV schema, got keys {sorted(output_rows[1])}",
        )
        self.assertEqual(
            output_rows[1]["display_name"],
            "Mtirala ridge / Mtirala access road",
            msg=f"Output rows should store the new location-first display name, got {output_rows[1]['display_name']!r}",
        )
        self.assertEqual(
            output_rows[1]["impact_people_est"],
            2500,
            msg=f"Output rows should expose estimated people reach, got row {output_rows[1]!r}",
        )
        self.assertEqual(
            output_rows[1]["primary_previous_tower_id"],
            1,
            msg=f"Output rows should keep the chosen predecessor tower for map route segments, got row {output_rows[1]!r}",
        )
        self.assertEqual(
            output_rows[1]["inter_cluster_neighbor_ids"],
            "",
            msg=f"Output rows should default to no inter-cluster connector ids until the exporter fills them, got row {output_rows[1]!r}",
        )
        self.assertEqual(
            output_rows[1]["country_code"],
            "",
            msg=f"Output rows should leave country_code empty when no local country fallback exists, got row {output_rows[1]!r}",
        )
        self.assertIn(
            "maplibre-gl.js",
            html_text,
            msg="HTML handout should inline the vendored MapLibre bootstrap so forwarded Telegram files do not depend on loading an external script.",
        )
        self.assertNotIn(
            "https://unpkg.com/maplibre-gl",
            html_text,
            msg="HTML handout should not depend on external MapLibre JS or CSS URLs because mobile content:// viewers can refuse those requests.",
        )
        self.assertIn(
            "tiles.openfreemap.org/styles/liberty",
            html_text,
            msg="HTML handout should use a basemap style that does not depend on referrer-based tile access.",
        )
        self.assertIn(
            "overview-map",
            html_text,
            msg="HTML handout should include the overview map container.",
        )
        self.assertIn(
            "cluster-map-seed-1",
            html_text,
            msg="HTML handout should include the per-cluster mini map container.",
        )
        self.assertIn(
            "addRouteLayers",
            html_text,
            msg="HTML handout should render route segments so installers can follow the rollout path on the map.",
        )
        self.assertIn(
            "addContextLayers",
            html_text,
            msg="HTML handout should render dashed context connectors so neighboring rollout clusters are easier to understand.",
        )
        self.assertIn(
            "map_order_label",
            html_text,
            msg="HTML handout should embed on-map order labels for the rollout sequence.",
        )
        self.assertIn(
            "map.isStyleLoaded()",
            html_text,
            msg="HTML handout should bootstrap overlays even when the basemap style is already cached or preloaded in an in-app browser.",
        )
        self.assertIn(
            "Install priority overlay step failed",
            html_text,
            msg="HTML handout should isolate overlay failures so one broken layer does not hide all vector overlays.",
        )
        self.assertIn(
            "FullscreenControl",
            html_text,
            msg="HTML handout should expose fullscreen controls for overview and cluster maps.",
        )
        self.assertIn(
            "Cheapest cluster connector",
            html_text,
            msg="HTML handout should explain that dashed connector lines show the cheapest cluster corridor.",
        )
        self.assertIn(
            "Cluster bounds",
            html_text,
            msg="HTML handout should explain the overview cluster bounds in the legend.",
        )
        self.assertIn(
            "\"cluster_bounds\"",
            html_text,
            msg="HTML handout should embed cluster-bound GeoJSON in the map payload.",
        )
        self.assertIn(
            "\"mqtt_points\"",
            html_text,
            msg="HTML handout should embed the seed/MQTT overview points in the map payload.",
        )
        self.assertIn(
            "\"seed_mqtt_links\"",
            html_text,
            msg="HTML handout should embed the direct seed/MQTT link overlays in the map payload.",
        )
        self.assertIn(
            "addSeedMqttMarkers",
            html_text,
            msg="HTML handout should render dedicated M/S map markers for reachable seed and MQTT points.",
        )
        self.assertIn(
            "addSeedMqttLinkLayers",
            html_text,
            msg="HTML handout should render every direct visible link touching the seed/MQTT overview points.",
        )
        self.assertIn(
            "markerEl.textContent = source === 'mqtt' ? 'M' : 'S';",
            html_text,
            msg="Reachable overview points should be labeled with M/S markers on the map.",
        )
        self.assertIn(
            "[41.58, 41.68]",
            html_text,
            msg="HTML handout should preserve the exporter-provided cluster polygon coordinates instead of rebuilding generic rectangles in the browser.",
        )
        self.assertIn(
            "<h2>Next Node Per Cluster</h2>",
            html_text,
            msg="The HTML handout should still include the next-node summary section.",
        )
        self.assertNotIn(
            "<td>Batumi</td>\n<td>\n<div class='node-title'>Batumi</div>",
            html_text,
            msg="Installed seed rows should not leak into the next-node summary table.",
        )

    def test_html_marks_installed_mqtt_rows_with_m_not_numeric_order(self) -> None:
        """Installed MQTT rows should render as already-present backbone points, not install steps."""

        html_text = render_html_document(
            rows=[
                {
                    "cluster_key": "seed:1",
                    "cluster_label": "Batumi",
                    "cluster_install_rank": 6,
                    "is_next_for_cluster": "false",
                    "rollout_status": "installed",
                    "installed": "true",
                    "tower_id": 8,
                    "label": "mqtt #8",
                    "display_name": "Marmarik / Hankavan-Hrazdan Highway",
                    "display_type": "Installed MQTT",
                    "source": "mqtt",
                    "impact_score": 1620,
                    "impact_people_est": 1620,
                    "impact_tower_count": 0,
                    "next_unlock_count": 0,
                    "backlink_count": 0,
                    "primary_previous_tower_id": "",
                    "inter_cluster_neighbor_ids": "",
                    "inter_cluster_connections": "",
                    "blocked_reason": "",
                    "previous_connections": "",
                    "next_connections": "",
                    "lon": "44.700000",
                    "lat": "40.800000",
                    "country_code": "am",
                    "country_name": "Armenia",
                    "location_status": "ok",
                    "location_en": "Marmarik, near Hankavan-Hrazdan Highway, Armenia",
                    "location_ru": "Мармарик, рядом с трассой Анкаван-Раздан, Армения",
                    "google_maps_url": "https://maps.google.com/?q=40.800000,44.700000",
                    "osm_url": "https://www.openstreetmap.org/?mlat=40.800000&mlon=44.700000#map=14/40.800000/44.700000",
                }
            ],
            generated_at="2026-04-24T00:00:00+00:00",
            geocoder_base_url="https://geocoder.batu.market",
        )

        self.assertIn(
            "\"map_order_label\": \"M\"",
            html_text,
            msg="Installed MQTT rows should expose map_order_label M instead of a numeric rollout rank in the map payload.",
        )
        self.assertIn(
            "Installed MQTT",
            html_text,
            msg="Installed MQTT rows should render as already-installed backbone nodes in the popup and table text.",
        )

    def test_build_output_row_falls_back_to_local_country_when_admin_context_is_empty(self) -> None:
        """Location text should still carry the country from local OSM context."""

        output_row = build_output_row(
            plan_row=PlanRow(
                cluster_key="seed:20",
                cluster_label="Tbilisi hackerspace",
                cluster_install_rank=3,
                is_next_for_cluster=False,
                rollout_status="planned",
                installed=False,
                tower_id=32,
                label="route #32",
                source="route",
                impact_score=5,
                impact_tower_count=2,
                next_unlock_count=0,
                backlink_count=1,
                previous_connection_ids=(31,),
                next_connection_ids=(),
                lon=45.238069,
                lat=40.869971,
            ),
            towers_by_id={
                31: TowerRecord(31, "route", 45.210000, 40.860000, "route #31", False),
                32: TowerRecord(32, "route", 45.238069, 40.869971, "route #32", False),
            },
            local_context={
                "road_en": "Ijevan-Berd Highway",
                "road_ru": "Иджеван-Бердское шоссе",
                "place_en": "Skhtorut",
                "place_ru": "Схторут",
                "country_code": "am",
                "country_en": "Armenia",
                "country_ru": "Армения",
            },
            admin_context_en={
                "city": None,
                "district": None,
                "province": None,
                "country": None,
            },
            admin_context_ru={
                "city": None,
                "district": None,
                "province": None,
                "country": None,
            },
            geocoder_status_en="error",
            geocoder_status_ru="error",
        )

        self.assertEqual(
            output_row["country_code"],
            "am",
            msg=f"Output row should preserve local OSM country_code, got row {output_row!r}",
        )
        self.assertEqual(
            output_row["country_name"],
            "Armenia",
            msg=f"Output row should preserve local OSM country_name, got row {output_row!r}",
        )
        self.assertEqual(
            output_row["location_en"],
            "Skhtorut, near Ijevan-Berd Highway, Armenia",
            msg=f"Location fallback should append the local country when admin context is empty, got row {output_row!r}",
        )


if __name__ == "__main__":
    unittest.main()
