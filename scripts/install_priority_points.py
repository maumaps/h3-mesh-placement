"""
Point-reconstruction helpers for installer-priority exports.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from math import cos, radians, sqrt
from typing import Sequence


@dataclass(frozen=True)
class EndpointObservation:
    """Single observed tower point derived from an edge endpoint."""

    tower_id: int
    lon: float
    lat: float


def reconstruct_tower_points(
    observations: Sequence[EndpointObservation],
    tolerance_m: float = 25.0,
) -> dict[int, tuple[float, float]]:
    """Collapse many edge-endpoint observations into one point per tower."""

    grouped: dict[int, list[EndpointObservation]] = defaultdict(list)

    for observation in observations:
        grouped[observation.tower_id].append(observation)

    if not grouped:
        raise ValueError("No tower endpoint observations were provided.")

    tower_points: dict[int, tuple[float, float]] = {}

    for tower_id, tower_observations in grouped.items():
        avg_lon = sum(item.lon for item in tower_observations) / len(tower_observations)
        avg_lat = sum(item.lat for item in tower_observations) / len(tower_observations)
        max_offset_m = max(
            _local_distance_m(item.lon, item.lat, avg_lon, avg_lat)
            for item in tower_observations
        )

        if max_offset_m > tolerance_m:
            raise ValueError(
                "Tower %s endpoint observations diverge by %.2f m across %s edges; "
                "the export cannot safely reconstruct a single point."
                % (tower_id, max_offset_m, len(tower_observations))
            )

        tower_points[tower_id] = (avg_lon, avg_lat)

    return tower_points


def _local_distance_m(
    lon_a: float,
    lat_a: float,
    lon_b: float,
    lat_b: float,
) -> float:
    """Measure short point-to-point offsets for DB-free unit tests."""

    radius_m = 6_371_000.0
    mean_lat_radians = radians((lat_a + lat_b) / 2.0)
    delta_lon = radians(lon_b - lon_a) * cos(mean_lat_radians)
    delta_lat = radians(lat_b - lat_a)

    return radius_m * sqrt(delta_lon * delta_lon + delta_lat * delta_lat)

