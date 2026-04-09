# Installer Priority Handout
This document explains the cluster-local installer handout export.
Each sentence starts on a new line for clean diffs.

## Goal
The handout answers a field question rather than a global optimization question.
Instead of one country-wide “next best tower”, it computes one next step per currently installed seed cluster.
This lets one team walk out from Batumi while another team walks out from Tbilisi without waiting for a single shared queue.
The HTML output is the primary field artifact and stays as a single file with an interactive overview map plus mini maps per cluster.
The overview map highlights the current next node in each cluster with an order badge.
Each cluster mini map shows the local install order directly on the nodes and draws a follow line from each step back to its chosen predecessor.
Dashed context lines show the cheapest visible connector between each pair of rollout clusters.
Cluster bounds on the maps are generated as Voronoi cells around all tower points and then merged by `cluster_key`, so the outlines reflect which part of the current tower field is closest to each rollout queue.
The Voronoi cells are clipped to a geodesic buffer around the full point cloud using the widest nearest-neighbor spacing in real meters, so outer edges do not depend on degree padding or Web Mercator assumptions.
Every map includes a fullscreen toggle for field use on smaller screens.

## Installed vs planned
Only `mesh_towers.source = 'seed'` is treated as already installed.
All other sources are treated as planned-but-not-installed towers that still need field work.
The export keeps installed seeds in the table for context, then ranks the planned towers locally inside each seed cluster.
Synthetic labels such as `route #40` are not shown as the primary name in the HTML.
Instead the HTML uses a location-first title built from the nearest place and road, with a smaller typed code such as `Route 40` or `Cluster 70`.

## Graph logic
The export prefers `mesh_visibility_edges_active` only when it covers the full live tower set.
Otherwise it falls back to `mesh_visibility_edges` so the handout does not silently drop reachable towers from planning.
Installed seed towers define the starting backbone.
Seed-only connectivity defines the initial field clusters.
Planned towers are assigned to the nearest seed cluster by shortest visible-path distance with deterministic tie-breaking, but cluster ownership still respects country boundaries.
That means a Georgian tower stays in a Georgian rollout queue and an Armenian tower stays in an Armenian rollout queue even when a cross-border line of sight exists.
Cross-country rollout joins are still allowed as connectors when the visibility graph supports them, but same-country joins are preferred first.
If a tower is disconnected from every installed seed inside its own country, it is still clamped to the nearest installed seed cluster in that country so the handout can show where that blocked island belongs operationally.
That row is then marked as `blocked` until a visible path from any installed seed exists.
Inside each cluster, the rollout order is computed greedily from the current frontier.
A candidate can be chosen only when it already sees a tower that is installed or appears earlier in that cluster’s rollout.

The per-row impact columns mean:
- `impact_people_est`: an estimate of newly reachable people behind this node, based on nearby populated localities attached to the candidate and the towers it unlocks next.
- `impact_tower_count`: how many not-yet-installed towers sit behind this node in the remaining cluster graph.
- `next_unlock_count`: how many towers become newly eligible immediately after installing this node.
- `previous_connections`: the earlier nodes this tower can connect back to right now.
- `next_connections`: the immediate towers this node unlocks on the next step.

The rollout now prefers joining another cluster before maximizing local reach.
If one frontier tower advances toward the cheapest seed-to-seed connector corridor to another cluster and another frontier tower only improves local reach, the connector-progress tower is chosen first.
When explicit per-edge RF loss is unavailable in the live exporter inputs, route-derived edges are used as the best available proxy because those route towers were originally promoted from minimum path-loss corridors.
Once no new cluster connection remains to chase, the queue falls back to estimated newly reachable people.

## Location text
The handout is meant for mountain deployment, so location text avoids postal-address style wording whenever possible.
It prefers the nearest named road and the nearest named place or terrain feature from local OSM data.
Admin context is requested from `https://geocoder.batu.market/v1/reverse` in both English and Russian.
If the hosted geocoder is sparse or unavailable, the export still succeeds and falls back to road/place context and raw coordinates.
Estimated reach also uses nearby populated OSM localities because the current live `mesh_surface_h3_r8` table has drifted away from its geometry/H3 key columns.

## Outputs
Run `make data/out/install_priority.html` to build both deliverables.
The export writes:
- `data/out/install_priority.html`
- `data/out/install_priority.csv`

The HTML file includes a top summary table with one true “next node” per cluster, a MapLibre overview map, and one mini map per cluster.
Solid lines show the recommended rollout path inside each cluster.
The overview map shows every rollout number plus the merged Voronoi outline around each cluster’s current extent.
Dashed gray lines show the cheapest visible connector between rollout clusters.
Those links are context only and do not drive the install order itself.
`blocked` means the tower belongs to that rollout queue, yet there is still no visible path from any installed seed to that tower.
The basemap uses the public OpenFreeMap `liberty` style so the handout does not depend on referrer-gated access to OpenStreetMap standard tiles.
The large cluster tables keep the full data, but the HTML shortens the “Unlocks” preview so the page stays readable on screen.
The CSV file keeps the same flat rows for sharing, filtering, or printing elsewhere.
