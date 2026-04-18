"""Tests for canonical seed GeoJSON generation."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.merge_seed_nodes import build_canonical_seed_features, write_canonical_geojson


class SeedNodeMergeTests(unittest.TestCase):
    """Verify curated and Meshtastic seeds merge deterministically."""

    def _write_json(self, path: Path, payload: object) -> None:
        path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    def test_build_canonical_seed_features_removes_pruned_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            curated_path = tmp_path / "curated.geojson"
            raw_path = tmp_path / "raw.json"

            self._write_json(
                curated_path,
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"name": "Gudauri"},
                            "geometry": {"type": "Point", "coordinates": [44.492348, 42.469766]},
                        },
                        {
                            "type": "Feature",
                            "properties": {"name": "Komzpa"},
                            "geometry": {"type": "Point", "coordinates": [41.5906879, 41.6212024]},
                        },
                    ],
                },
            )
            self._write_json(
                raw_path,
                {
                    "nodes": [
                        {
                            "long_name": "Zhar",
                            "short_name": "Zhar",
                            "latitude": 416383965,
                            "longitude": 417399589,
                            "position_precision": 16,
                        }
                    ]
                },
            )

            canonical = build_canonical_seed_features(curated_path=curated_path, raw_path=raw_path)
            canonical_names = [item.name for item in canonical]

            self.assertEqual(
                canonical_names,
                ["Komzpa"],
                msg=f"Canonical seeds should drop Gudauri/Zhar and keep only curated survivors, got {canonical_names!r}",
            )

    def test_build_canonical_seed_features_prefers_curated_then_best_precision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            curated_path = tmp_path / "curated.geojson"
            raw_path = tmp_path / "raw.json"

            self._write_json(
                curated_path,
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"name": "Curated Batumi"},
                            "geometry": {"type": "Point", "coordinates": [41.5904, 41.6211]},
                        }
                    ],
                },
            )
            self._write_json(
                raw_path,
                {
                    "nodes": [
                        {
                            "long_name": "Lower Precision Duplicate",
                            "short_name": "LPD",
                            "node_id_hex": "!aaa",
                            "updated_at": "2026-04-14T00:00:00Z",
                            "latitude": 416211000,
                            "longitude": 415904000,
                            "position_precision": 12,
                        },
                        {
                            "long_name": "Higher Precision Duplicate",
                            "short_name": "HPD",
                            "node_id_hex": "!bbb",
                            "updated_at": "2026-04-14T01:00:00Z",
                            "latitude": 416211200,
                            "longitude": 415904200,
                            "position_precision": 16,
                        },
                        {
                            "long_name": "Yerevan Roof",
                            "short_name": "YRVN",
                            "node_id_hex": "!ccc",
                            "updated_at": "2026-04-14T02:00:00Z",
                            "latitude": 401802042,
                            "longitude": 445055760,
                            "position_precision": 15,
                        },
                    ]
                },
            )

            canonical = build_canonical_seed_features(curated_path=curated_path, raw_path=raw_path)
            canonical_names = [item.name for item in canonical]

            self.assertEqual(
                canonical_names,
                ["Curated Batumi", "Yerevan Roof"],
                msg=f"Canonical seeds should keep curated duplicates over Meshtastic points and retain unique regional nodes, got {canonical_names!r}",
            )
            self.assertEqual(
                canonical[1].source,
                "mqtt",
                msg=f"Liam Cottle imported nodes should always be labeled mqtt in the canonical seed file, got source={canonical[1].source!r} for {canonical[1].name!r}",
            )

    def test_build_canonical_seed_features_keeps_raw_meshtastic_nodes_for_postgis_clip(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            curated_path = tmp_path / "curated.geojson"
            raw_path = tmp_path / "raw.json"

            self._write_json(
                curated_path,
                {
                    "type": "FeatureCollection",
                    "features": [],
                },
            )
            self._write_json(
                raw_path,
                {
                    "nodes": [
                        {
                            "long_name": "Tbilisi MQTT",
                            "short_name": "TBL",
                            "node_id_hex": "!tbilisi",
                            "updated_at": "2026-04-14T03:00:00Z",
                            "latitude": 417120000,
                            "longitude": 447900000,
                            "position_precision": 16,
                        },
                        {
                            "long_name": "Vladikavkaz MQTT",
                            "short_name": "VLK",
                            "node_id_hex": "!vladik",
                            "updated_at": "2026-04-14T04:00:00Z",
                            "latitude": 430200000,
                            "longitude": 447000000,
                            "position_precision": 16,
                        },
                    ]
                },
            )

            canonical = build_canonical_seed_features(curated_path=curated_path, raw_path=raw_path)
            canonical_names = [item.name for item in canonical]

            self.assertEqual(
                canonical_names,
                ["Tbilisi MQTT", "Vladikavkaz MQTT"],
                msg=f"Canonical seed generation should leave geographic clipping to PostGIS and keep raw MQTT points for later SQL filtering, got {canonical_names!r}",
            )

    def test_build_canonical_seed_features_keeps_eu868_or_unknown_regions_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            curated_path = tmp_path / "curated.geojson"
            raw_path = tmp_path / "raw.json"

            self._write_json(
                curated_path,
                {
                    "type": "FeatureCollection",
                    "features": [],
                },
            )
            self._write_json(
                raw_path,
                {
                    "nodes": [
                        {
                            "long_name": "Known EU868",
                            "short_name": "EU",
                            "node_id_hex": "!eu868",
                            "updated_at": "2026-04-14T05:00:00Z",
                            "latitude": 417120000,
                            "longitude": 447900000,
                            "position_precision": 16,
                            "region": 3,
                            "region_name": "EU_868",
                        },
                        {
                            "long_name": "Unknown Region",
                            "short_name": "UNK",
                            "node_id_hex": "!unknown",
                            "updated_at": "2026-04-14T06:00:00Z",
                            "latitude": 401802042,
                            "longitude": 445055760,
                            "position_precision": 16,
                            "region": None,
                            "region_name": None,
                        },
                        {
                            "long_name": "RU433 Node",
                            "short_name": "RU",
                            "node_id_hex": "!ru433",
                            "updated_at": "2026-04-14T07:00:00Z",
                            "latitude": 417220000,
                            "longitude": 447950000,
                            "position_precision": 16,
                            "region": 5,
                            "region_name": "RU_433",
                        },
                    ]
                },
            )

            canonical = build_canonical_seed_features(curated_path=curated_path, raw_path=raw_path)
            canonical_names = [item.name for item in canonical]

            self.assertEqual(
                canonical_names,
                ["Known EU868", "Unknown Region"],
                msg=f"Canonical MQTT import should keep only EU_868 or unknown-region nodes and drop RU/433-style nodes, got {canonical_names!r}",
            )

    def test_write_canonical_geojson_keeps_valid_feature_collection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            output_path = tmp_path / "existing_mesh_nodes.geojson"
            curated_path = tmp_path / "curated.geojson"

            self._write_json(
                curated_path,
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"name": "Poti"},
                            "geometry": {"type": "Point", "coordinates": [41.6614681, 42.1381603]},
                        }
                    ],
                },
            )

            canonical = build_canonical_seed_features(curated_path=curated_path, raw_path=None)
            write_canonical_geojson(canonical, output_path)
            rendered = json.loads(output_path.read_text(encoding="utf-8"))

            self.assertEqual(
                rendered["type"],
                "FeatureCollection",
                msg=f"Canonical seed output should stay a GeoJSON FeatureCollection, got {rendered!r}",
            )
            self.assertEqual(
                rendered["features"][0]["properties"]["name"],
                "Poti",
                msg=f"Canonical GeoJSON should preserve seed names for import, got {rendered['features']!r}",
            )


if __name__ == "__main__":
    unittest.main()
