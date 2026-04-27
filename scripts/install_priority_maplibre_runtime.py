"""Inline MapLibre runtime script for the installer-priority handout."""

from __future__ import annotations


def build_map_script() -> str:
    """Return the inline MapLibre bootstrap script for the shared report map."""

    return """
const payloadEl = document.getElementById('install-priority-data');
const fallbackEl = document.getElementById('map-fallback');

if (!payloadEl || !window.maplibregl) {
  if (fallbackEl) fallbackEl.style.display = 'block';
} else {
  const payload = JSON.parse(payloadEl.textContent);
  const rowsByTowerId = new Map(payload.rows.map((row) => [row.tower_id, row]));
  const styleUrl = {
    version: 8,
    sources: {
      cartoVoyager: {
        type: 'raster',
        tiles: [
          'https://a.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png',
          'https://b.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png',
          'https://c.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}.png',
        ],
        tileSize: 256,
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      },
      esriWorldImagery: {
        type: 'raster',
        tiles: [
          'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
        ],
        tileSize: 256,
        attribution: 'Tiles &copy; Esri',
      },
      openTopoMap: {
        type: 'raster',
        tiles: [
          'https://a.tile.opentopomap.org/{z}/{x}/{y}.png',
          'https://b.tile.opentopomap.org/{z}/{x}/{y}.png',
          'https://c.tile.opentopomap.org/{z}/{x}/{y}.png',
        ],
        tileSize: 256,
        attribution: 'Map data &copy; OpenStreetMap contributors, SRTM | Map style &copy; OpenTopoMap',
      },
    },
    layers: [
      {
        id: 'background',
        type: 'background',
        paint: {
          'background-color': '#eef2f1',
        },
      },
      {
        id: 'carto-voyager',
        type: 'raster',
        source: 'cartoVoyager',
      },
      {
        id: 'esri-world-imagery',
        type: 'raster',
        source: 'esriWorldImagery',
        layout: {
          visibility: 'none',
        },
      },
      {
        id: 'open-topo-map',
        type: 'raster',
        source: 'openTopoMap',
        layout: {
          visibility: 'none',
        },
      },
    ],
  };
  const features = payload.rows.map((row) => ({
    type: 'Feature',
    geometry: {
      type: 'Point',
      coordinates: [row.lon, row.lat],
    },
    properties: row,
  }));
  const rankNumber = (value) => {
    if (value === null || value === undefined || value === '') return null;
    const parsed = Number(value);

    return Number.isFinite(parsed) ? parsed : null;
  };

  const mapOptions = (container, center, zoom) => ({
    container,
    style: styleUrl,
    center,
    zoom,
    attributionControl: false,
  });
  const basemapLayerIds = ['carto-voyager', 'esri-world-imagery', 'open-topo-map'];
  const basemapOptions = [
    { id: 'carto-voyager', label: 'Map' },
    { id: 'esri-world-imagery', label: 'Satellite' },
    { id: 'open-topo-map', label: 'Terrain' },
  ];
  const setBasemap = (map, layerId) => {
    basemapLayerIds.forEach((candidateLayerId) => {
      if (!map.getLayer(candidateLayerId)) return;

      map.setLayoutProperty(
        candidateLayerId,
        'visibility',
        candidateLayerId === layerId ? 'visible' : 'none',
      );
    });
  };
  class BasemapControl {
    onAdd(map) {
      this.map = map;
      this.container = document.createElement('div');
      this.container.className = 'maplibregl-ctrl maplibregl-ctrl-group basemap-control';

      basemapOptions.forEach((option) => {
        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = option.label;
        button.dataset.basemapLayer = option.id;
        button.className = option.id === basemapOptions[0].id ? 'active' : '';
        button.setAttribute('aria-pressed', option.id === basemapOptions[0].id ? 'true' : 'false');
        button.addEventListener('click', () => {
          setBasemap(map, option.id);
          this.container.querySelectorAll('button').forEach((candidate) => {
            const selected = candidate.dataset.basemapLayer === option.id;
            candidate.classList.toggle('active', selected);
            candidate.setAttribute('aria-pressed', selected ? 'true' : 'false');
          });
        });
        this.container.appendChild(button);
      });

      return this.container;
    }

    onRemove() {
      this.container.parentNode.removeChild(this.container);
      this.map = undefined;
    }
  }
  const safeOverlayStep = (stepName, callback) => {
    try { callback(); } catch (error) { console.warn(`Install priority overlay step failed: ${stepName}`, error); }
  };
  const runOnStyleReady = (map, callback) => {
    let ran = false;
    const once = () => { if (ran) return; ran = true; callback(); };
    if (map.isStyleLoaded()) { once(); return; }
    map.once('style.load', once);
    map.once('load', once);
  };

  const popupHtml = (properties) => `
    <div class="node-title">${properties.display_name}</div>
    <div class="node-subtitle">${properties.display_type}</div>
    <div style="margin-top:8px"><strong>Order:</strong> ${properties.map_order_label || 'unranked'}</div>
    <div style="margin-top:8px"><strong>Reach:</strong> ${properties.impact_people_est}</div>
    <div><strong>Location:</strong> ${properties.location_en}</div>
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
        from_tower_id: row.tower_id,
        to_tower_id: previousRow.tower_id,
      },
    }];
  });
  const contextFeatures = [];
  const contextPairs = new Set();

  payload.rows.forEach((row) => {
    (row.previous_connection_ids || []).forEach((previousId) => {
      if (previousId === row.primary_previous_tower_id) return;

      const previousRow = rowsByTowerId.get(previousId);
      if (!previousRow || previousRow.cluster_key !== row.cluster_key) return;

      const pairKey = ['previous', ...[row.tower_id, previousId].sort((left, right) => left - right)].join(':');
      if (contextPairs.has(pairKey)) return;
      contextPairs.add(pairKey);
      contextFeatures.push({
        type: 'Feature',
        geometry: {
          type: 'LineString',
          coordinates: [
            [previousRow.lon, previousRow.lat],
            [row.lon, row.lat],
          ],
        },
        properties: {
          from_cluster_key: row.cluster_key,
          to_cluster_key: previousRow.cluster_key,
          from_tower_id: row.tower_id,
          to_tower_id: previousId,
          link_kind: 'previous',
        },
      });
    });

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
  const installedBackboneFeatures = [];
  const installedBackbonePairs = new Set();

  payload.rows.forEach((row) => {
    if (!row.installed) return;

    (row.previous_connection_ids || []).forEach((previousId) => {
      const previousRow = rowsByTowerId.get(previousId);
      if (!previousRow || !previousRow.installed || previousRow.cluster_key !== row.cluster_key) return;

      const pairKey = [row.tower_id, previousId].sort((left, right) => left - right).join(':');
      if (installedBackbonePairs.has(pairKey)) return;
      installedBackbonePairs.add(pairKey);
      installedBackboneFeatures.push({
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
          from_tower_id: row.tower_id,
          to_tower_id: previousId,
          link_kind: 'installed_backbone',
        },
      });
    });
  });
  const clusterBoundFeatures = payload.cluster_bounds || [];
  const seedMqttFeatures = (payload.mqtt_points || []).map((point) => ({
    type: 'Feature',
    geometry: {
      type: 'Point',
      coordinates: [point.lon, point.lat],
    },
    properties: point,
  }));
  const seedMqttLinkFeatures = (payload.seed_mqtt_links || []).map((item) => ({
    type: 'Feature',
    geometry: JSON.parse(item.geometry),
    properties: {
      source_h3: item.source_h3,
      target_h3: item.target_h3,
    },
  }));

  const addRouteLayers = (map, sourceName, featureCollection, mapMode) => {
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
          ['==', ['get', 'rollout_status'], 'next'], mapMode === 'cluster' ? 8 : 5,
          mapMode === 'cluster' ? 6 : 3,
        ],
        'line-color': [
          'case',
          ['==', ['get', 'rollout_status'], 'next'], mapMode === 'cluster' ? '#c55f0b' : '#d97706',
          mapMode === 'cluster' ? '#244f7a' : '#5c7c4a',
        ],
        'line-opacity': [
          'case',
          ['==', ['get', 'rollout_status'], 'next'], mapMode === 'cluster' ? 1 : 0.95,
          mapMode === 'cluster' ? 0.92 : 0.62,
        ],
      },
    });
  };
  const addContextLayers = (map, sourceName, featureCollection, mapMode) => {
    map.addSource(sourceName, { type: 'geojson', data: featureCollection });
    map.addLayer({
      id: `${sourceName}-context`,
      type: 'line',
      source: sourceName,
      filter: ['!=', ['get', 'link_kind'], 'phase_one_connector'],
      layout: {
        'line-cap': 'round',
        'line-join': 'round',
      },
      paint: {
        'line-width': mapMode === 'cluster' ? 3 : 2,
        'line-color': '#7a8694',
        'line-opacity': mapMode === 'cluster' ? 0.88 : 0.68,
        'line-dasharray': [2, 1.4],
      },
    });
    map.addLayer({
      id: `${sourceName}-phase-one-halo`,
      type: 'line',
      source: sourceName,
      filter: ['==', ['get', 'link_kind'], 'phase_one_connector'],
      layout: {
        'line-cap': 'round',
        'line-join': 'round',
      },
      paint: {
        'line-width': mapMode === 'cluster' ? 8 : 7,
        'line-color': '#ffffff',
        'line-opacity': 0.88,
      },
    });
    map.addLayer({
      id: `${sourceName}-phase-one`,
      type: 'line',
      source: sourceName,
      filter: ['==', ['get', 'link_kind'], 'phase_one_connector'],
      layout: {
        'line-cap': 'round',
        'line-join': 'round',
      },
      paint: {
        'line-width': mapMode === 'cluster' ? 4.8 : 4.4,
        'line-color': '#111827',
        'line-opacity': 0.95,
        'line-dasharray': [1.4, 0.7],
      },
    });
  };
  const addClusterBoundLayers = (map, sourceName, featureCollection, mapMode) => {
    map.addSource(sourceName, { type: 'geojson', data: featureCollection });
    map.addLayer({
      id: `${sourceName}-fill`,
      type: 'fill',
      source: sourceName,
      paint: {
        'fill-color': '#8b5e3c',
        'fill-opacity': mapMode === 'cluster' ? 0.12 : 0.05,
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
        'line-width': mapMode === 'cluster' ? 3.5 : 2,
        'line-opacity': mapMode === 'cluster' ? 0.9 : 0.55,
        'line-dasharray': [2.4, 1.2],
      },
    });
  };
  const addSeedMqttLinkLayers = (map, sourceName, featureCollection) => {
    map.addSource(sourceName, { type: 'geojson', data: featureCollection });
    map.addLayer({
      id: `${sourceName}-links`,
      type: 'line',
      source: sourceName,
      layout: {
        'line-cap': 'round',
        'line-join': 'round',
      },
      paint: {
        'line-width': 1.5,
        'line-color': '#6f7f90',
        'line-opacity': 0.5,
      },
    });
  };
  const addInstalledBackboneLayers = (map, sourceName, featureCollection, mapMode) => {
    map.addSource(sourceName, { type: 'geojson', data: featureCollection });
    map.addLayer({
      id: `${sourceName}-halo`,
      type: 'line',
      source: sourceName,
      layout: {
        'line-cap': 'round',
        'line-join': 'round',
      },
      paint: {
        'line-width': mapMode === 'cluster' ? 6.5 : 5,
        'line-color': '#ffffff',
        'line-opacity': 0.82,
      },
    });
    map.addLayer({
      id: `${sourceName}-links`,
      type: 'line',
      source: sourceName,
      layout: {
        'line-cap': 'round',
        'line-join': 'round',
      },
      paint: {
        'line-width': mapMode === 'cluster' ? 3.5 : 2.6,
        'line-color': '#27548a',
        'line-opacity': mapMode === 'cluster' ? 0.9 : 0.72,
      },
    });
  };
  const addSeedMqttMarkers = (map, featureCollection) => {
    const markers = [];

    featureCollection.features.forEach((feature) => {
      const markerEl = document.createElement('div');
      const source = String(feature.properties.source || '').toLowerCase();
      markerEl.className = 'order-marker seed-mqtt-marker installed overview';
      markerEl.textContent = source === 'mqtt' ? 'M' : 'S';
      markerEl.title = `${feature.properties.name} (${feature.properties.country_name || feature.properties.country_code || 'unknown'})`;
      markerEl.tabIndex = 0;
      markerEl.setAttribute('role', 'button');
      markerEl.setAttribute('aria-label', markerEl.title);

      const marker = new maplibregl.Marker({ element: markerEl, anchor: 'center' })
        .setLngLat(feature.geometry.coordinates)
        .setPopup(
          new maplibregl.Popup({ offset: 12 }).setHTML(`
            <div class="node-title">${feature.properties.name}</div>
            <div class="node-subtitle">${source === 'mqtt' ? 'MQTT node' : 'Seed node'}</div>
            <div style="margin-top:8px"><strong>Country:</strong> ${feature.properties.country_name || feature.properties.country_code || 'unknown'}</div>
          `)
        )
        .addTo(map);
      markerEl.addEventListener('keydown', (event) => {
        if (event.key !== 'Enter' && event.key !== ' ') {
          return;
        }
        event.preventDefault();
        marker.togglePopup();
      });
      markers.push(marker);
    });

    return markers;
  };

  const addNodeLayers = (map, sourceName, featureCollection, mapMode) => {
    const installedRadius = mapMode === 'cluster' ? 4 : 7;
    const nextRadius = mapMode === 'cluster' ? 5.2 : 9;
    const defaultRadius = mapMode === 'cluster' ? 3.2 : 6;
    map.addSource(sourceName, { type: 'geojson', data: featureCollection });
    map.addLayer({
      id: `${sourceName}-points`,
      type: 'circle',
      source: sourceName,
      paint: {
        'circle-radius': [
          'case',
          ['==', ['get', 'installed'], true], installedRadius,
          ['==', ['get', 'is_next_for_cluster'], true], nextRadius,
          defaultRadius,
        ],
        'circle-color': [
          'case',
          ['==', ['get', 'installed'], true], '#27548a',
          ['==', ['get', 'is_next_for_cluster'], true], '#d97706',
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
    map.on('mouseleave', `${sourceName}-points`, () => { map.getCanvas().style.cursor = ''; });
  };

  const addOrderMarkers = (map, featureCollection, labelMode) => {
    const markers = [];

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

      const marker = new maplibregl.Marker({ element: markerEl, anchor: 'center' })
        .setLngLat(feature.geometry.coordinates)
        .addTo(map);

      markers.push(marker);
    });

    return markers;
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

  const overviewConnectCutoffByCluster = new Map(
    payload.clusters.map((cluster) => [cluster.cluster_key, Number(cluster.connect_max_rank || 0)])
  );
  const overviewConnectTowerIds = new Set(payload.phase_one_tower_ids || []);

  const overviewFeatureIsInConnectView = (feature) => {
    if (feature.properties.installed) return true;
    if (overviewConnectTowerIds.size) return overviewConnectTowerIds.has(feature.properties.tower_id);

    const rank = rankNumber(feature.properties.cluster_install_rank);
    const cutoff = overviewConnectCutoffByCluster.get(feature.properties.cluster_key);

    return rank !== null && cutoff !== undefined && rank <= cutoff;
  };
  const buildOverviewCollections = (viewMode) => {
    const overviewFeatures = viewMode === 'coverage'
      ? features
      : features.filter(overviewFeatureIsInConnectView);
    const includedTowerIds = new Set(overviewFeatures.map((feature) => feature.properties.tower_id));
    const overviewRouteFeatures = viewMode === 'coverage'
      ? routeFeatures
      : routeFeatures.filter((feature) => {
        if (overviewConnectTowerIds.size) {
          return (
            overviewConnectTowerIds.has(feature.properties.from_tower_id)
            && overviewConnectTowerIds.has(feature.properties.to_tower_id)
          );
        }

        const rank = rankNumber(feature.properties.cluster_install_rank);
        const cutoff = overviewConnectCutoffByCluster.get(feature.properties.cluster_key);

        return rank !== null && cutoff !== undefined && rank <= cutoff;
      });
    const overviewPhaseOneLocalContextFeatures = contextFeatures.filter((feature) => (
      includedTowerIds.has(feature.properties.from_tower_id)
      && includedTowerIds.has(feature.properties.to_tower_id)
    ));
    const overviewPhaseOneConnectorFeatures = phaseOneConnectorEdges.features.filter((feature) => (
      includedTowerIds.has(feature.properties.from_tower_id)
      && includedTowerIds.has(feature.properties.to_tower_id)
    ));
    const overviewContextFeatures = viewMode === 'coverage'
      ? contextFeatures
      : [
        ...overviewPhaseOneLocalContextFeatures,
        ...overviewPhaseOneConnectorFeatures,
      ];
    const overviewInstalledBackboneFeatures = installedBackboneFeatures.filter((feature) => (
      includedTowerIds.has(feature.properties.from_tower_id)
      && includedTowerIds.has(feature.properties.to_tower_id)
    ));

    return {
      collection: {
        type: 'FeatureCollection',
        features: overviewFeatures,
      },
      routes: {
        type: 'FeatureCollection',
        features: overviewRouteFeatures,
      },
      context: {
        type: 'FeatureCollection',
        features: overviewContextFeatures,
      },
      installedBackbone: {
        type: 'FeatureCollection',
        features: overviewInstalledBackboneFeatures,
      },
      seedMqttLinks: viewMode === 'coverage'
        ? overviewSeedMqttLinks
        : {
          type: 'FeatureCollection',
          features: [],
        },
      fitFeatures: overviewRouteFeatures.length
        ? [...overviewFeatures, ...overviewRouteFeatures]
        : overviewFeatures,
    };
  };
  const overviewBounds = {
    type: 'FeatureCollection',
    features: clusterBoundFeatures,
  };
  const overviewSeedMqtt = {
    type: 'FeatureCollection',
    features: seedMqttFeatures,
  };
  const overviewSeedMqttLinks = {
    type: 'FeatureCollection',
    features: seedMqttLinkFeatures,
  };
  const phaseOneConnectorEdges = {
    type: 'FeatureCollection',
    features: payload.phase_one_connector_edges || [],
  };
  const clusterByKey = new Map(payload.clusters.map((cluster) => [cluster.cluster_key, cluster]));
  const buildClusterCollections = (clusterKey, viewMode) => {
    const cluster = clusterByKey.get(clusterKey);
    if (!cluster) return null;

    const maxRank = viewMode === 'coverage' ? null : Number(cluster.connect_max_rank || 0);
    const clusterFeatures = features.filter((feature) => (
      feature.properties.cluster_key === clusterKey
      && (
        maxRank === null
        || feature.properties.installed
        || Number(feature.properties.cluster_install_rank) <= maxRank
      )
    ));
    const includedTowerIds = new Set(clusterFeatures.map((feature) => feature.properties.tower_id));

    return {
      label: cluster.cluster_label,
      collection: {
        type: 'FeatureCollection',
        features: clusterFeatures,
      },
      routes: {
        type: 'FeatureCollection',
        features: routeFeatures.filter((feature) => (
          feature.properties.cluster_key === clusterKey
          && (
            maxRank === null
            || Number(feature.properties.cluster_install_rank) <= maxRank
          )
        )),
      },
      context: {
        type: 'FeatureCollection',
        features: contextFeatures.filter((feature) => (
          (
            feature.properties.from_cluster_key === clusterKey
            || feature.properties.to_cluster_key === clusterKey
          )
          && (
            maxRank === null
            || (
              includedTowerIds.has(feature.properties.from_tower_id)
              && includedTowerIds.has(feature.properties.to_tower_id)
            )
          )
        )),
      },
      installedBackbone: {
        type: 'FeatureCollection',
        features: installedBackboneFeatures.filter((feature) => (
          feature.properties.cluster_key === clusterKey
          && (
            maxRank === null
            || (
              includedTowerIds.has(feature.properties.from_tower_id)
              && includedTowerIds.has(feature.properties.to_tower_id)
            )
          )
        )),
      },
      bounds: {
        type: 'FeatureCollection',
        features: clusterBoundFeatures.filter((feature) => feature.properties.cluster_key === clusterKey),
      },
      seedMqttLinks: viewMode === 'coverage' ? overviewSeedMqttLinks : { type: 'FeatureCollection', features: [] },
      fitFeatures: clusterFeatures.length
        ? [
          ...clusterFeatures,
          ...routeFeatures.filter((feature) => feature.properties.cluster_key === clusterKey),
        ]
        : [],
    };
  };
  let overviewMap = null;
  let overviewOrderMarkers = [];
  let seedMqttMarkers = [];

  window.__installPriorityMaps = {
    payload,
    clusterByKey,
    getOverviewMap: () => overviewMap,
  };
  const clearOverviewOrderMarkers = () => {
    overviewOrderMarkers.forEach((marker) => marker.remove());
    overviewOrderMarkers = [];
  };
  const updateSharedMap = ({ scope = 'overview', clusterKey = '', viewMode = 'connect' } = {}) => {
    if (!overviewMap) return;

    const collections = scope === 'cluster'
      ? buildClusterCollections(clusterKey, viewMode)
      : buildOverviewCollections(viewMode);
    if (!collections) return;

    const nodeSource = overviewMap.getSource('overview-nodes');
    const routeSource = overviewMap.getSource('overview-route-segments');
    const contextSource = overviewMap.getSource('overview-context-segments');
    const seedMqttLinkSource = overviewMap.getSource('overview-seed-mqtt-links');
    const installedBackboneSource = overviewMap.getSource('overview-installed-backbone');
    const boundsSource = overviewMap.getSource('overview-cluster-bounds');

    if (!nodeSource || !routeSource || !contextSource) return;

    if (nodeSource) nodeSource.setData(collections.collection);
    if (routeSource) routeSource.setData(collections.routes);
    if (contextSource) contextSource.setData(collections.context);
    if (seedMqttLinkSource) seedMqttLinkSource.setData(collections.seedMqttLinks);
    if (installedBackboneSource) installedBackboneSource.setData(collections.installedBackbone);
    if (boundsSource) boundsSource.setData(scope === 'cluster' ? collections.bounds : overviewBounds);

    clearOverviewOrderMarkers();
    overviewOrderMarkers = addOrderMarkers(overviewMap, collections.collection, scope === 'cluster' ? 'cluster' : 'overview');

    const stateEl = document.getElementById('map-state');
    if (stateEl) {
      const phaseLabel = viewMode === 'coverage' ? 'Improve coverage' : 'Connect clusters';
      stateEl.textContent = scope === 'cluster'
        ? `${collections.label}: ${phaseLabel}`
        : `Overview: ${phaseLabel}`;
    }

    requestAnimationFrame(() => {
      overviewMap.resize();
      fitToFeatures(overviewMap, collections.fitFeatures);
    });
  };
  const setupOverviewViewTabs = () => {
    const tabs = Array.from(document.querySelectorAll('.overview-view-tab'));
    if (!tabs.length) return;

    tabs.forEach((tab) => {
      tab.addEventListener('click', () => {
        tabs.forEach((candidate) => {
          const selected = candidate === tab;
          candidate.classList.toggle('active', selected);
          candidate.setAttribute('aria-selected', selected ? 'true' : 'false');
        });
        updateSharedMap({ scope: 'overview', viewMode: tab.dataset.overviewView || 'connect' });
      });
    });
  };

  const setupClusterViewTabs = () => {
    document.querySelectorAll('.cluster-view-tabs').forEach((tabList) => {
      const tabs = Array.from(tabList.querySelectorAll('[role="tab"]'));
      const section = tabList.closest('.cluster');
      if (!section || !tabs.length) return;

      const activateTab = (selectedTab, focusTab = false) => {
        tabs.forEach((tab) => {
          const selected = tab === selectedTab;
          tab.classList.toggle('active', selected);
          tab.setAttribute('aria-selected', selected ? 'true' : 'false');
          tab.tabIndex = selected ? 0 : -1;

          const panelId = tab.getAttribute('aria-controls');
          const panel = panelId ? document.getElementById(panelId) : null;
          if (!panel) return;

          panel.hidden = !selected;
          panel.classList.toggle('active', selected);
        });

        updateSharedMap({
          scope: 'cluster',
          clusterKey: selectedTab.dataset.clusterKey || '',
          viewMode: selectedTab.dataset.mapPhase || 'connect',
        });
        if (focusTab) selectedTab.focus();
      };

      tabs.forEach((tab, index) => {
        tab.tabIndex = tab.getAttribute('aria-selected') === 'true' ? 0 : -1;
        tab.addEventListener('click', () => activateTab(tab));
        tab.addEventListener('keydown', (event) => {
          const keyMoves = {
            ArrowLeft: -1,
            ArrowUp: -1,
            ArrowRight: 1,
            ArrowDown: 1,
          };
          let nextIndex = index;

          if (event.key === 'Home') nextIndex = 0;
          if (event.key === 'End') nextIndex = tabs.length - 1;
          if (event.key in keyMoves) nextIndex = (index + keyMoves[event.key] + tabs.length) % tabs.length;
          if (nextIndex === index) return;

          event.preventDefault();
          activateTab(tabs[nextIndex], true);
        });
      });
    });
  };
  const setupSharedMapButtons = () => {
    document.querySelectorAll('.shared-map-button').forEach((button) => {
      button.addEventListener('click', () => {
        updateSharedMap({
          scope: 'cluster',
          clusterKey: button.dataset.clusterKey || '',
          viewMode: button.dataset.mapPhase || 'connect',
        });
        document.getElementById('overview-map')?.scrollIntoView({ behavior: 'smooth', block: 'center' });
      });
    });
  };

  overviewMap = new maplibregl.Map({ ...mapOptions('overview-map', [43.5, 41.8], 6) });
  overviewMap.addControl(new BasemapControl(), 'top-left');
  overviewMap.addControl(new maplibregl.NavigationControl({ visualizePitch: false }), 'top-right');
  overviewMap.addControl(new maplibregl.FullscreenControl(), 'top-right');
  overviewMap.addControl(new maplibregl.AttributionControl({ compact: true }), 'bottom-right');
  runOnStyleReady(overviewMap, () => {
    const initialOverviewCollections = buildOverviewCollections('connect');
    safeOverlayStep('overview bounds', () => addClusterBoundLayers(overviewMap, 'overview-cluster-bounds', overviewBounds, 'overview'));
    safeOverlayStep('overview connectors', () => addContextLayers(overviewMap, 'overview-context-segments', initialOverviewCollections.context, 'overview'));
    safeOverlayStep('overview installed links', () => addInstalledBackboneLayers(overviewMap, 'overview-installed-backbone', initialOverviewCollections.installedBackbone, 'overview'));
    safeOverlayStep('overview seed/mqtt links', () => addSeedMqttLinkLayers(overviewMap, 'overview-seed-mqtt-links', initialOverviewCollections.seedMqttLinks));
    safeOverlayStep('overview nodes', () => addNodeLayers(overviewMap, 'overview-nodes', initialOverviewCollections.collection, 'overview'));
    safeOverlayStep('overview routes', () => addRouteLayers(overviewMap, 'overview-route-segments', initialOverviewCollections.routes, 'overview'));
    safeOverlayStep('overview seed/mqtt markers', () => { seedMqttMarkers = addSeedMqttMarkers(overviewMap, overviewSeedMqtt); });
    safeOverlayStep('overview order markers', () => { overviewOrderMarkers = addOrderMarkers(overviewMap, initialOverviewCollections.collection, 'overview'); });
    requestAnimationFrame(() => { overviewMap.resize(); fitToFeatures(overviewMap, initialOverviewCollections.fitFeatures); });
  });
  setupOverviewViewTabs();
  setupClusterViewTabs();
  setupSharedMapButtons();
  window.__installPriorityMaps.setMapView = updateSharedMap;
}
"""
