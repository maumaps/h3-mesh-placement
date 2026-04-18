"""Merge curated seed points with an optional Meshtastic snapshot.

The pipeline imports only the canonical ``data/in/existing_mesh_nodes.geojson`` file.
This helper rebuilds that canonical file from:
- a curated repo-managed GeoJSON baseline
- an optional raw Meshtastic Liam Cottle node snapshot

The external snapshot is deduplicated by best reported precision, H3 cell, and
curated-seed preference.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


REMOVED_SEED_NAMES = {"Gudauri", "Zhar"}

POSITION_PRECISION_METERS = {
    1: 622000,
    2: 303000,
    3: 76000,
    4: 38000,
    5: 19000,
    6: 4700,
    7: 2400,
    8: 1200,
    9: 596,
    10: 298,
    11: 149,
    12: 75,
    13: 37,
    14: 19,
    15: 9,
    16: 5,
    17: 2,
    18: 1,
}


@dataclass(frozen=True)
class SeedFeature:
    """Normalized seed feature ready for canonical GeoJSON export."""

    name: str
    lon: float
    lat: float
    source: str
    position_precision: int | None
    node_id_hex: str | None
    updated_at: str | None

    @property
    def h3_key(self) -> str:
        """Return a coarse dedupe key approximating the planning H3 cell.

        The SQL pipeline performs the real H3 projection on import.
        For canonical-file dedupe we use rounded coordinates that are comfortably
        tighter than one resolution-8 H3 cell inside the target region.
        """

        return f"{round(self.lon, 3):.3f},{round(self.lat, 3):.3f}"

    @property
    def precision_rank(self) -> tuple[int, int]:
        """Return a sortable precision tuple where smaller is better."""

        if self.position_precision is None:
            return (1, 10**9)

        return (0, POSITION_PRECISION_METERS.get(self.position_precision, 10**9))

    def to_geojson_feature(self, feature_id: int) -> dict[str, Any]:
        """Render a GeoJSON feature for ogr2ogr import."""

        properties: dict[str, Any] = {
            "name": self.name,
            "source": self.source,
        }
        if self.position_precision is not None:
            properties["position_precision"] = self.position_precision
        if self.node_id_hex:
            properties["node_id_hex"] = self.node_id_hex
        if self.updated_at:
            properties["updated_at"] = self.updated_at

        return {
            "type": "Feature",
            "properties": properties,
            "geometry": {
                "type": "Point",
                "coordinates": [self.lon, self.lat],
            },
            "id": feature_id,
        }


def _load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _iter_curated_features(curated_path: Path) -> list[SeedFeature]:
    payload = _load_json(curated_path)
    features = payload.get("features", [])
    curated: list[SeedFeature] = []

    for feature in features:
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        coordinates = geometry.get("coordinates", [])
        name = properties.get("name")

        if geometry.get("type") != "Point" or len(coordinates) != 2 or not name:
            continue
        if name in REMOVED_SEED_NAMES:
            continue

        curated.append(
            SeedFeature(
                name=str(name),
                lon=float(coordinates[0]),
                lat=float(coordinates[1]),
                source="curated",
                position_precision=(
                    int(properties["position_precision"])
                    if properties.get("position_precision") is not None
                    else None
                ),
                node_id_hex=(
                    str(properties["node_id_hex"])
                    if properties.get("node_id_hex")
                    else None
                ),
                updated_at=str(properties["updated_at"]) if properties.get("updated_at") else None,
            )
        )

    return curated


def _choose_name(node: dict[str, Any]) -> str | None:
    long_name = node.get("long_name")
    short_name = node.get("short_name")

    if isinstance(long_name, str) and long_name.strip():
        return long_name.strip()
    if isinstance(short_name, str) and short_name.strip():
        return short_name.strip()
    return None


def _iter_meshtastic_features(raw_path: Path | None) -> list[SeedFeature]:
    if raw_path is None or not raw_path.exists():
        return []

    payload = _load_json(raw_path)
    nodes = payload.get("nodes", [])
    merged: list[SeedFeature] = []

    for node in nodes:
        latitude_raw = node.get("latitude")
        longitude_raw = node.get("longitude")
        if latitude_raw is None or longitude_raw is None:
            continue

        region_name = node.get("region_name")
        region = node.get("region")
        if region is not None and region_name != "EU_868":
            continue

        lat = float(latitude_raw) / 10_000_000
        lon = float(longitude_raw) / 10_000_000
        name = _choose_name(node)
        if not name or name in REMOVED_SEED_NAMES:
            continue

        precision = node.get("position_precision")
        merged.append(
            SeedFeature(
                name=name,
                lon=lon,
                lat=lat,
                source="mqtt",
                position_precision=int(precision) if precision is not None else None,
                node_id_hex=str(node["node_id_hex"]) if node.get("node_id_hex") else None,
                updated_at=str(node["updated_at"]) if node.get("updated_at") else None,
            )
        )

    return merged


def _sort_seed(seed: SeedFeature) -> tuple[int, tuple[int, int], str, str]:
    source_rank = 0 if seed.source == "curated" else 1
    return (source_rank, seed.precision_rank, seed.name.lower(), seed.h3_key)


def build_canonical_seed_features(curated_path: Path, raw_path: Path | None) -> list[SeedFeature]:
    """Build canonical seed features sorted and deduplicated for import."""

    curated = _iter_curated_features(curated_path)
    meshtastic = _iter_meshtastic_features(raw_path)

    best_by_h3: dict[str, SeedFeature] = {}
    for seed in sorted([*curated, *meshtastic], key=_sort_seed):
        existing = best_by_h3.get(seed.h3_key)
        if existing is None:
            best_by_h3[seed.h3_key] = seed
            continue
        if _sort_seed(seed) < _sort_seed(existing):
            best_by_h3[seed.h3_key] = seed

    # Keep stable deterministic order for diffs and downstream matching.
    return sorted(best_by_h3.values(), key=lambda item: (item.source != "curated", item.name.lower(), item.lon, item.lat))


def write_canonical_geojson(features: list[SeedFeature], output_path: Path) -> None:
    """Write the canonical seed GeoJSON used by the pipeline."""

    output = {
        "type": "FeatureCollection",
        "features": [
            feature.to_geojson_feature(feature_id=index)
            for index, feature in enumerate(features)
        ],
    }
    output_path.write_text(
        json.dumps(output, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--curated-geojson", required=True, type=Path)
    parser.add_argument("--raw-json", type=Path)
    parser.add_argument("--output-geojson", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    canonical = build_canonical_seed_features(
        curated_path=args.curated_geojson,
        raw_path=args.raw_json,
    )
    write_canonical_geojson(canonical, args.output_geojson)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
