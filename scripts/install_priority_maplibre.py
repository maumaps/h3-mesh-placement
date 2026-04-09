"""
MapLibre-specific helpers for the installer-priority handout HTML.
"""

from __future__ import annotations

import json
import re
from typing import Mapping, Sequence

try:
    from scripts.install_priority_map_payload import (
        dedupe_clusters,
        fallback_cluster_bound_features,
    )
except ModuleNotFoundError:
    from install_priority_map_payload import (  # type: ignore[no-redef]
        dedupe_clusters,
        fallback_cluster_bound_features,
    )


def normalize_rows(rows: Sequence[Mapping[str, object]]) -> list[dict[str, object]]:
    """Convert CSV-style strings into types that the HTML renderer can trust."""

    normalized_rows: list[dict[str, object]] = []

    for row in rows:
        normalized = dict(row)
        normalized["installed"] = _as_bool(row.get("installed"))
        normalized["is_next_for_cluster"] = _as_bool(row.get("is_next_for_cluster"))

        for integer_key in [
            "tower_id",
            "impact_score",
            "impact_people_est",
            "impact_tower_count",
            "next_unlock_count",
            "backlink_count",
        ]:
            normalized[integer_key] = _as_int(row.get(integer_key))

        for optional_integer_key in [
            "cluster_install_rank",
            "primary_previous_tower_id",
        ]:
            raw_value = row.get(optional_integer_key)
            normalized[optional_integer_key] = (
                ""
                if raw_value in (None, "")
                else _as_int(raw_value)
            )

        for float_key in ["lon", "lat"]:
            normalized[float_key] = float(row[float_key])

        normalized["inter_cluster_neighbor_ids"] = _as_int_list(
            row.get("inter_cluster_neighbor_ids")
        )
        normalized["map_order_label"] = _build_map_order_label(normalized)
        normalized_rows.append(normalized)

    return normalized_rows


def cluster_map_id(cluster_key: str) -> str:
    """Build a stable DOM id for one cluster mini map."""

    slug = re.sub(r"[^a-z0-9]+", "-", cluster_key.lower())

    return f"cluster-map-{slug.strip('-')}"


def render_map_assets(
    normalized_rows: Sequence[Mapping[str, object]],
    *,
    cluster_bound_features: Sequence[Mapping[str, object]] | None = None,
) -> list[str]:
    """Return inline script tags needed for the one-file MapLibre handout."""

    deduped_clusters = dedupe_clusters(normalized_rows)
    map_payload = {
        "rows": normalized_rows,
        "clusters": deduped_clusters,
        "cluster_bounds": list(
            cluster_bound_features
            or fallback_cluster_bound_features(
                normalized_rows,
                deduped_clusters,
            )
        ),
    }
    map_payload_json = json.dumps(map_payload, ensure_ascii=False).replace("</", "<\\/")

    return [
        f"<script id='install-priority-data' type='application/json'>{map_payload_json}</script>",
        "<script src='https://unpkg.com/maplibre-gl@4.7.1/dist/maplibre-gl.js'></script>",
        "<script>",
        _map_script(),
        "</script>",
    ]


def _as_bool(value: object) -> bool:
    """Parse booleans from CSV-style strings or plain Python values."""

    if isinstance(value, bool):
        return value
    if value is None:
        return False

    return str(value).strip().lower() == "true"


def _as_int(value: object) -> int | None:
    """Parse optional integers from CSV-style strings."""

    if value in (None, ""):
        return None
    if isinstance(value, int):
        return value

    return int(float(str(value)))


def _build_map_order_label(row: Mapping[str, object]) -> str:
    """Create a short on-map label that matches the local rollout order."""

    if bool(row.get("installed")):
        return "S"
    if row.get("cluster_install_rank") in (None, ""):
        return ""

    return str(row["cluster_install_rank"])


def _as_int_list(value: object) -> list[int]:
    """Parse comma-separated integer lists from CSV-style strings."""

    if value in (None, ""):
        return []

    return [
        int(item)
        for item in str(value).split(",")
        if item.strip()
    ]


def _map_script() -> str:
    """Return the inline MapLibre bootstrap script for overview and mini maps."""

    return """
const payloadEl = document.getElementById('install-priority-data');
const fallbackEl = document.getElementById('map-fallback');

if (!payloadEl || !window.maplibregl) {
  if (fallbackEl) fallbackEl.style.display = 'block';
} else {
  const payload = JSON.parse(payloadEl.textContent);
  const rowsByTowerId = new Map(payload.rows.map((row) => [row.tower_id, row]));
  const styleUrl = 'https://tiles.openfreemap.org/styles/liberty';
  const features = payload.rows.map((row) => ({
    type: 'Feature',
    geometry: {
      type: 'Point',
      coordinates: [row.lon, row.lat],
    },
    properties: row,
  }));

  const mapOptions = (container, center, zoom) => ({
    container,
    style: styleUrl,
    center,
    zoom,
  });

  const popupHtml = (properties) => `
    <div class="node-title">${properties.display_name}</div>
    <div class="node-subtitle">${properties.display_type}</div>
    <div style="margin-top:8px"><strong>Order:</strong> ${properties.map_order_label || 'blocked'}</div>
    <div style="margin-top:8px"><strong>Reach:</strong> ${properties.impact_people_est}</div>
    <div><strong>Location:</strong> ${properties.location_en}</div>
    ${properties.blocked_reason ? `<div style="margin-top:8px"><strong>Blocked:</strong> ${properties.blocked_reason}</div>` : ''}
    ${properties.inter_cluster_connections ? `<div style="margin-top:8px"><strong>Cluster connector:</strong> ${properties.inter_cluster_connections}</div>` : ''}
    <div style="margin-top:8px">
      <a href="${properties.google_maps_url}" target="_blank" rel="noreferrer">Google</a>
      <a href="${properties.osm_url}" target="_blank" rel="noreferrer">OSM</a>
    </div>
  `;

  const routeFeatures = payload.rows.flatMap((row) => {
    if (!row.primary_previous_tower_id) return [];
    const previousRow = rowsByTowerId.get(row.primary_previous_tower_id);
    if (!previousRow) return [];

    return [{
      type: 'Feature',
      geometry: {
        type: 'LineString',
        coordinates: [
          [previousRow.lon, previousRow.lat],
          [row.lon, row.lat],
        ],
      },
      properties: {
        cluster_key: row.cluster_key,
        rollout_status: row.rollout_status,
        cluster_install_rank: row.cluster_install_rank,
      },
    }];
  });
  const contextFeatures = [];
  const contextPairs = new Set();

  payload.rows.forEach((row) => {
    (row.inter_cluster_neighbor_ids || []).forEach((neighborId) => {
      const neighborRow = rowsByTowerId.get(neighborId);
      if (!neighborRow) return;
      const pairKey = [row.tower_id, neighborId].sort((left, right) => left - right).join(':');
      if (contextPairs.has(pairKey)) return;
      contextPairs.add(pairKey);
      contextFeatures.push({
        type: 'Feature',
        geometry: {
          type: 'LineString',
          coordinates: [
            [row.lon, row.lat],
            [neighborRow.lon, neighborRow.lat],
          ],
        },
        properties: {
          from_cluster_key: row.cluster_key,
          to_cluster_key: neighborRow.cluster_key,
          from_tower_id: row.tower_id,
          to_tower_id: neighborId,
        },
      });
    });
  });
  const clusterBoundFeatures = payload.cluster_bounds || [];

  const addRouteLayers = (map, sourceName, featureCollection) => {
    map.addSource(sourceName, { type: 'geojson', data: featureCollection });
    map.addLayer({
      id: `${sourceName}-routes`,
      type: 'line',
      source: sourceName,
      layout: {
        'line-cap': 'round',
        'line-join': 'round',
      },
      paint: {
        'line-width': [
          'case',
          ['==', ['get', 'rollout_status'], 'next'], 5,
          3,
        ],
        'line-color': [
          'case',
          ['==', ['get', 'rollout_status'], 'next'], '#d97706',
          '#5c7c4a',
        ],
        'line-opacity': [
          'case',
          ['==', ['get', 'rollout_status'], 'next'], 0.95,
          0.62,
        ],
      },
    });
  };
  const addContextLayers = (map, sourceName, featureCollection) => {
    map.addSource(sourceName, { type: 'geojson', data: featureCollection });
    map.addLayer({
      id: `${sourceName}-context`,
      type: 'line',
      source: sourceName,
      layout: {
        'line-cap': 'round',
        'line-join': 'round',
      },
      paint: {
        'line-width': 2,
        'line-color': '#7a8694',
        'line-opacity': 0.68,
        'line-dasharray': [2, 1.4],
      },
    });
  };
  const addClusterBoundLayers = (map, sourceName, featureCollection) => {
    map.addSource(sourceName, { type: 'geojson', data: featureCollection });
    map.addLayer({
      id: `${sourceName}-fill`,
      type: 'fill',
      source: sourceName,
      paint: {
        'fill-color': '#8b5e3c',
        'fill-opacity': 0.05,
      },
    });
    map.addLayer({
      id: `${sourceName}-outline`,
      type: 'line',
      source: sourceName,
      layout: {
        'line-cap': 'round',
        'line-join': 'round',
      },
      paint: {
        'line-color': '#8b5e3c',
        'line-width': 2,
        'line-opacity': 0.55,
        'line-dasharray': [2.4, 1.2],
      },
    });
  };

  const addNodeLayers = (map, sourceName, featureCollection) => {
    map.addSource(sourceName, { type: 'geojson', data: featureCollection });
    map.addLayer({
      id: `${sourceName}-points`,
      type: 'circle',
      source: sourceName,
      paint: {
        'circle-radius': [
          'case',
          ['==', ['get', 'installed'], true], 7,
          ['==', ['get', 'is_next_for_cluster'], true], 9,
          6,
        ],
        'circle-color': [
          'case',
          ['==', ['get', 'installed'], true], '#27548a',
          ['==', ['get', 'is_next_for_cluster'], true], '#d97706',
          ['==', ['get', 'rollout_status'], 'blocked'], '#b45309',
          '#4b8b3b',
        ],
        'circle-stroke-color': [
          'case',
          ['==', ['get', 'is_next_for_cluster'], true], '#fff7ed',
          '#ffffff',
        ],
        'circle-stroke-width': 1.5,
      },
    });

    map.on('click', `${sourceName}-points`, (event) => {
      const feature = event.features && event.features[0];
      if (!feature) return;
      new maplibregl.Popup({ offset: 12 })
        .setLngLat(feature.geometry.coordinates)
        .setHTML(popupHtml(feature.properties))
        .addTo(map);
    });

    map.on('mouseenter', `${sourceName}-points`, () => {
      map.getCanvas().style.cursor = 'pointer';
    });
    map.on('mouseleave', `${sourceName}-points`, () => {
      map.getCanvas().style.cursor = '';
    });
  };

  const addOrderMarkers = (map, featureCollection, labelMode) => {
    featureCollection.features.forEach((feature) => {
      const properties = feature.properties;
      const label = properties.map_order_label;

      if (!label) return;

      const markerEl = document.createElement('div');
      const statusClass = properties.installed
        ? 'installed'
        : (properties.is_next_for_cluster ? 'next' : (properties.rollout_status || 'planned'));
      markerEl.className = `order-marker ${statusClass} ${labelMode}`;
      markerEl.textContent = label;

      new maplibregl.Marker({ element: markerEl, anchor: 'center' })
        .setLngLat(feature.geometry.coordinates)
        .addTo(map);
    });
  };

  const fitToFeatures = (map, subset) => {
    if (!subset.length) return;
    const bounds = new maplibregl.LngLatBounds();
    subset.forEach((feature) => {
      if (feature.geometry.type === 'Point') {
        bounds.extend(feature.geometry.coordinates);
        return;
      }

      if (feature.geometry.type === 'LineString') {
        feature.geometry.coordinates.forEach((coordinate) => bounds.extend(coordinate));
        return;
      }

      if (feature.geometry.type === 'Polygon') {
        feature.geometry.coordinates.forEach((ring) => {
          ring.forEach((coordinate) => bounds.extend(coordinate));
        });
        return;
      }

      if (feature.geometry.type === 'MultiPolygon') {
        feature.geometry.coordinates.forEach((polygon) => {
          polygon.forEach((ring) => {
            ring.forEach((coordinate) => bounds.extend(coordinate));
          });
        });
      }
    });
    map.fitBounds(bounds, { padding: 40, maxZoom: 11, duration: 0 });
  };

  const overviewCollection = {
    type: 'FeatureCollection',
    features,
  };
  const overviewRoutes = {
    type: 'FeatureCollection',
    features: routeFeatures,
  };
  const overviewContext = {
    type: 'FeatureCollection',
    features: contextFeatures,
  };
  const overviewBounds = {
    type: 'FeatureCollection',
    features: clusterBoundFeatures,
  };
  const overviewMap = new maplibregl.Map({
    ...mapOptions('overview-map', [43.5, 41.8], 6),
  });
  overviewMap.addControl(new maplibregl.NavigationControl({ visualizePitch: false }), 'top-right');
  overviewMap.addControl(new maplibregl.FullscreenControl(), 'top-right');
  overviewMap.on('load', () => {
    addClusterBoundLayers(overviewMap, 'overview-cluster-bounds', overviewBounds);
    addContextLayers(overviewMap, 'overview-context-segments', overviewContext);
    addRouteLayers(overviewMap, 'overview-route-segments', overviewRoutes);
    addNodeLayers(overviewMap, 'overview-nodes', overviewCollection);
    addOrderMarkers(overviewMap, overviewCollection, 'overview');
    fitToFeatures(overviewMap, features);
  });

  payload.clusters.forEach((cluster) => {
    const container = document.getElementById(cluster.map_id);
    if (!container) return;
    const clusterFeatures = features.filter((feature) => feature.properties.cluster_key === cluster.cluster_key);
    const clusterCollection = {
      type: 'FeatureCollection',
      features: clusterFeatures,
    };
    const clusterRoutes = {
      type: 'FeatureCollection',
      features: routeFeatures.filter((feature) => feature.properties.cluster_key === cluster.cluster_key),
    };
    const clusterContext = {
      type: 'FeatureCollection',
      features: contextFeatures.filter((feature) => (
        feature.properties.from_cluster_key === cluster.cluster_key
        || feature.properties.to_cluster_key === cluster.cluster_key
      )),
    };
    const clusterBounds = {
      type: 'FeatureCollection',
      features: clusterBoundFeatures.filter((feature) => feature.properties.cluster_key === cluster.cluster_key),
    };
    const clusterMap = new maplibregl.Map({
      ...mapOptions(
        cluster.map_id,
        clusterFeatures[0] ? clusterFeatures[0].geometry.coordinates : [43.5, 41.8],
        8,
      ),
      interactive: true,
    });
    clusterMap.addControl(new maplibregl.FullscreenControl(), 'top-right');
    clusterMap.on('load', () => {
      addClusterBoundLayers(clusterMap, `${cluster.map_id}-cluster-bounds`, clusterBounds);
      addContextLayers(clusterMap, `${cluster.map_id}-context-segments`, clusterContext);
      addRouteLayers(clusterMap, `${cluster.map_id}-route-segments`, clusterRoutes);
      addNodeLayers(clusterMap, `${cluster.map_id}-nodes`, clusterCollection);
      addOrderMarkers(clusterMap, clusterCollection, 'cluster');
      fitToFeatures(clusterMap, [...clusterBounds.features, ...clusterFeatures, ...clusterContext.features]);
    });
  });
}
"""
