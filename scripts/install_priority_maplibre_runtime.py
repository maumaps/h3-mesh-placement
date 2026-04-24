"""Inline MapLibre runtime script for the installer-priority handout."""

from __future__ import annotations


def build_map_script() -> str:
    """Return the inline MapLibre bootstrap script for overview and mini maps."""

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
  const addSeedMqttMarkers = (map, featureCollection) => {
    featureCollection.features.forEach((feature) => {
      const markerEl = document.createElement('div');
      const source = String(feature.properties.source || '').toLowerCase();
      markerEl.className = `order-marker ${source === 'mqtt' ? 'planned' : 'installed'} overview`;
      markerEl.textContent = source === 'mqtt' ? 'm' : 's';
      markerEl.title = `${feature.properties.name} (${feature.properties.country_name || feature.properties.country_code || 'unknown'})`;

      new maplibregl.Marker({ element: markerEl, anchor: 'center' })
        .setLngLat(feature.geometry.coordinates)
        .setPopup(
          new maplibregl.Popup({ offset: 12 }).setHTML(`
            <div class="node-title">${feature.properties.name}</div>
            <div class="node-subtitle">${source === 'mqtt' ? 'MQTT node' : 'Seed node'}</div>
            <div style="margin-top:8px"><strong>Country:</strong> ${feature.properties.country_name || feature.properties.country_code || 'unknown'}</div>
          `)
        )
        .addTo(map);
    });
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

  const overviewFeatureIsInConnectView = (feature) => {
    if (feature.properties.installed) return true;

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
        const rank = rankNumber(feature.properties.cluster_install_rank);
        const cutoff = overviewConnectCutoffByCluster.get(feature.properties.cluster_key);

        return rank !== null && cutoff !== undefined && rank <= cutoff;
      });
    const overviewContextFeatures = viewMode === 'coverage'
      ? contextFeatures
      : phaseOneConnectorEdges.features.filter((feature) => (
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
  const clusterMapTargets = payload.clusters.flatMap((cluster) => ([
    { ...cluster, map_id: cluster.map_id },
    ...(cluster.full_map_id ? [{ ...cluster, map_id: cluster.full_map_id }] : []),
  ]));
  const clusterByMapId = new Map(clusterMapTargets.map((cluster) => [cluster.map_id, cluster]));
  const clusterCollectionsByMapId = new Map(clusterMapTargets.map((cluster) => {
    const container = document.getElementById(cluster.map_id);
    const maxRankText = container ? container.dataset.maxRank : '';
    const maxRank = maxRankText === undefined || maxRankText === '' ? null : Number(maxRankText);
    const clusterFeatures = features.filter((feature) => (
      feature.properties.cluster_key === cluster.cluster_key
      && (
        maxRank === null
        || feature.properties.installed
        || Number(feature.properties.cluster_install_rank) <= maxRank
      )
    ));
    const includedTowerIds = new Set(clusterFeatures.map((feature) => feature.properties.tower_id));

    return [
      cluster.map_id,
      {
        clusterFeatures,
        clusterCollection: {
          type: 'FeatureCollection',
          features: clusterFeatures,
        },
        clusterRoutes: {
          type: 'FeatureCollection',
          features: routeFeatures.filter((feature) => (
            feature.properties.cluster_key === cluster.cluster_key
            && (
              maxRank === null
              || Number(feature.properties.cluster_install_rank) <= maxRank
            )
          )),
        },
        clusterContext: {
          type: 'FeatureCollection',
          features: contextFeatures.filter((feature) => (
            (
              feature.properties.from_cluster_key === cluster.cluster_key
              || feature.properties.to_cluster_key === cluster.cluster_key
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
        clusterBounds: {
          type: 'FeatureCollection',
          features: clusterBoundFeatures.filter((feature) => feature.properties.cluster_key === cluster.cluster_key),
        },
      },
    ];
  }));
  const activeClusterMaps = new Map();
  let overviewMap = null;
  let overviewOrderMarkers = [];

  window.__installPriorityMaps = {
    payload,
    activeClusterMaps,
    clusterByMapId,
    getOverviewMap: () => overviewMap,
    getClusterMap: (mapId) => activeClusterMaps.get(mapId) || null,
  };
  const clearOverviewOrderMarkers = () => {
    overviewOrderMarkers.forEach((marker) => marker.remove());
    overviewOrderMarkers = [];
  };
  const updateOverviewView = (viewMode) => {
    if (!overviewMap) return;

    const collections = buildOverviewCollections(viewMode);
    const nodeSource = overviewMap.getSource('overview-nodes');
    const routeSource = overviewMap.getSource('overview-route-segments');
    const contextSource = overviewMap.getSource('overview-context-segments');
    const seedMqttLinkSource = overviewMap.getSource('overview-seed-mqtt-links');

    if (!nodeSource || !routeSource || !contextSource) return;

    if (nodeSource) nodeSource.setData(collections.collection);
    if (routeSource) routeSource.setData(collections.routes);
    if (contextSource) contextSource.setData(collections.context);
    if (seedMqttLinkSource) seedMqttLinkSource.setData(collections.seedMqttLinks);

    clearOverviewOrderMarkers();
    overviewOrderMarkers = addOrderMarkers(overviewMap, collections.collection, 'overview');
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
        updateOverviewView(tab.dataset.overviewView || 'connect');
      });
    });
  };

  const mountClusterMap = (cluster) => {
    const container = document.getElementById(cluster.map_id);

    if (!container || activeClusterMaps.has(cluster.map_id)) return activeClusterMaps.get(cluster.map_id) || null;

    const collections = clusterCollectionsByMapId.get(cluster.map_id);
    if (!collections) return null;

    const { clusterFeatures, clusterCollection, clusterRoutes, clusterContext, clusterBounds } = collections;
    const clusterMap = new maplibregl.Map({
      ...mapOptions(cluster.map_id, clusterFeatures[0] ? clusterFeatures[0].geometry.coordinates : [43.5, 41.8], 8),
      interactive: true,
    });

    activeClusterMaps.set(cluster.map_id, clusterMap);
    container.dataset.mapMounted = 'true';
    clusterMap.addControl(new maplibregl.FullscreenControl(), 'top-right');
    clusterMap.addControl(new maplibregl.AttributionControl({ compact: true }), 'bottom-right');
    runOnStyleReady(clusterMap, () => {
      safeOverlayStep(`${cluster.map_id} bounds`, () => addClusterBoundLayers(clusterMap, `${cluster.map_id}-cluster-bounds`, clusterBounds, 'cluster'));
      safeOverlayStep(`${cluster.map_id} connectors`, () => addContextLayers(clusterMap, `${cluster.map_id}-context-segments`, clusterContext, 'cluster'));
      safeOverlayStep(`${cluster.map_id} nodes`, () => addNodeLayers(clusterMap, `${cluster.map_id}-nodes`, clusterCollection, 'cluster'));
      safeOverlayStep(`${cluster.map_id} routes`, () => addRouteLayers(clusterMap, `${cluster.map_id}-route-segments`, clusterRoutes, 'cluster'));
      safeOverlayStep(`${cluster.map_id} order markers`, () => addOrderMarkers(clusterMap, clusterCollection, 'cluster'));
      requestAnimationFrame(() => { clusterMap.resize(); fitToFeatures(clusterMap, clusterRoutes.features.length ? [...clusterFeatures, ...clusterRoutes.features] : clusterFeatures); });
    });

    return clusterMap;
  };
  const mountClusterMapsInPanel = (panel) => {
    panel.querySelectorAll('.cluster-map').forEach((container) => {
      const cluster = clusterByMapId.get(container.id);
      if (!cluster) return;

      requestAnimationFrame(() => {
        const clusterMap = mountClusterMap(cluster);
        if (clusterMap) clusterMap.resize();
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
          if (selected) mountClusterMapsInPanel(panel);
        });

        if (focusTab) selectedTab.focus();
        syncVisibleClusterMaps();
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
  const clusterContainerIsFullscreen = (container) => {
    if (!document.fullscreenElement) return false;

    return document.fullscreenElement === container || container.contains(document.fullscreenElement);
  };
  const unmountClusterMap = (cluster) => {
    const container = document.getElementById(cluster.map_id);
    const clusterMap = activeClusterMaps.get(cluster.map_id);

    if (!container || !clusterMap || clusterContainerIsFullscreen(container)) return;

    clusterMap.remove();
    activeClusterMaps.delete(cluster.map_id);
    container.innerHTML = '';
    container.dataset.mapMounted = 'false';
  };
  const clusterContainerIsVisible = (cluster) => {
    const container = document.getElementById(cluster.map_id);
    if (!container) return false;

    return container.getClientRects().length > 0;
  };
  const prefersLazyClusterMaps = () => window.matchMedia('(max-width: 920px)').matches;
  const clusterIsNearViewport = (cluster) => {
    const container = document.getElementById(cluster.map_id);
    if (!container) return false;

    const rect = container.getBoundingClientRect();
    const preloadMargin = 240;

    return rect.bottom >= -preloadMargin && rect.top <= window.innerHeight + preloadMargin;
  };
  const syncVisibleClusterMaps = () => {
    clusterMapTargets.forEach((cluster) => {
      if (!clusterContainerIsVisible(cluster)) {
        unmountClusterMap(cluster);
        return;
      }

      if (!prefersLazyClusterMaps() || clusterIsNearViewport(cluster)) {
        mountClusterMap(cluster);
        return;
      }

      unmountClusterMap(cluster);
    });
  };
  window.__installPriorityMaps.syncVisibleClusterMaps = syncVisibleClusterMaps;
  window.__installPriorityMaps.mountClusterMap = (mapId) => {
    const cluster = clusterByMapId.get(mapId);
    if (!cluster) return null;

    return mountClusterMap(cluster);
  };
  const initializeClusterMaps = () => {
    if (!prefersLazyClusterMaps() || !window.IntersectionObserver) {
      clusterMapTargets.forEach((cluster) => {
        if (clusterContainerIsVisible(cluster)) mountClusterMap(cluster);
      });
      return;
    }

    const mountClusterObserver = new IntersectionObserver((entries) => {
      entries.forEach((entry) => {
        const cluster = clusterByMapId.get(entry.target.id);
        if (!cluster) return;

        if (entry.isIntersecting && clusterContainerIsVisible(cluster)) {
          mountClusterMap(cluster);
          return;
        }

        unmountClusterMap(cluster);
      });
    }, {
      root: null,
      rootMargin: '240px 0px',
      threshold: 0.01,
    });

    clusterMapTargets.forEach((cluster) => {
      const container = document.getElementById(cluster.map_id);
      if (!container) return;
      mountClusterObserver.observe(container);
    });
    syncVisibleClusterMaps();
    setTimeout(syncVisibleClusterMaps, 250);
    window.addEventListener('resize', syncVisibleClusterMaps);
    window.addEventListener('orientationchange', syncVisibleClusterMaps);
    window.addEventListener('focus', syncVisibleClusterMaps);
    window.addEventListener('pageshow', syncVisibleClusterMaps);
    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible') syncVisibleClusterMaps();
    });
    document.addEventListener('fullscreenchange', syncVisibleClusterMaps);
  };

  overviewMap = new maplibregl.Map({ ...mapOptions('overview-map', [43.5, 41.8], 6) });
  overviewMap.addControl(new maplibregl.NavigationControl({ visualizePitch: false }), 'top-right');
  overviewMap.addControl(new maplibregl.FullscreenControl(), 'top-right');
  overviewMap.addControl(new maplibregl.AttributionControl({ compact: true }), 'bottom-right');
  runOnStyleReady(overviewMap, () => {
    const initialOverviewCollections = buildOverviewCollections('connect');
    safeOverlayStep('overview bounds', () => addClusterBoundLayers(overviewMap, 'overview-cluster-bounds', overviewBounds, 'overview'));
    safeOverlayStep('overview connectors', () => addContextLayers(overviewMap, 'overview-context-segments', initialOverviewCollections.context, 'overview'));
    safeOverlayStep('overview seed/mqtt links', () => addSeedMqttLinkLayers(overviewMap, 'overview-seed-mqtt-links', initialOverviewCollections.seedMqttLinks));
    safeOverlayStep('overview nodes', () => addNodeLayers(overviewMap, 'overview-nodes', initialOverviewCollections.collection, 'overview'));
    safeOverlayStep('overview routes', () => addRouteLayers(overviewMap, 'overview-route-segments', initialOverviewCollections.routes, 'overview'));
    safeOverlayStep('overview seed/mqtt markers', () => addSeedMqttMarkers(overviewMap, overviewSeedMqtt));
    safeOverlayStep('overview order markers', () => { overviewOrderMarkers = addOrderMarkers(overviewMap, initialOverviewCollections.collection, 'overview'); });
    requestAnimationFrame(() => { overviewMap.resize(); fitToFeatures(overviewMap, initialOverviewCollections.fitFeatures); });
  });
  setupOverviewViewTabs();
  setupClusterViewTabs();
  initializeClusterMaps();
}
"""
