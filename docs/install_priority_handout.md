# Installer Priority Handout
This document explains the cluster-local installer handout export.
Each sentence starts on a new line for clean diffs.

## Goal
The handout answers a field question rather than a global optimization question.
Instead of one country-wide “next best tower”, it computes one next step per currently installed seed cluster.
This lets one team walk out from Batumi while another team walks out from Tbilisi without waiting for a single shared queue.
The HTML output is the primary field artifact and stays as a single file with an interactive overview map plus mini maps per cluster.
The overview map highlights the current next node in each cluster with an order badge.
The overview map opens in `Connect clusters` mode so it shows only the rollout prefix needed to connect neighboring queues.
Its `Improve coverage` mode shows the full later plan for hop reduction and local coverage review.
The phase switch is styled as a two-option control above the overview map and repeated inside each cluster detail section.
Each cluster mini map shows the local install order directly on the nodes and draws a follow line from each step back to its chosen predecessor.
By default, each cluster section shows the order prefix through the currently known connector points to neighboring clusters.
Each cluster section has two tabs: `Connect clusters` for that connector-first field route, and `Improve coverage` for the full later queue that reduces hops and fills local coverage.
Dashed context lines show the cheapest visible connector between each pair of rollout clusters.
Cluster bounds on the maps are generated as Voronoi cells around all tower points and then merged by `cluster_key`, so the outlines reflect which part of the current tower field is closest to each rollout queue.
The Voronoi cells are clipped to a geodesic buffer around the full point cloud using the widest nearest-neighbor spacing in real meters, so outer edges do not depend on degree padding or Web Mercator assumptions.
Every map includes a fullscreen toggle for field use on smaller screens.
The HTML is tuned for phones as well as laptops.
The top summary stays a real accessible table, while the long per-cluster detail grid switches to stacked cards on narrow screens so installers do not need to pan across ten columns.
Cluster mini maps intentionally use smaller order badges and point circles than the overview map so the local geometry stays readable on a phone.
Each cluster mini map now fits to its own nodes and local rollout lines only.
Inter-cluster connector lines and full Voronoi bounds are still shown for context, but they do not control the mini-map zoom level anymore.
On phones the maps use compact attribution controls, slightly taller mini-map panels, smaller cluster badges, and heavier local route strokes so the rollout overlays stay visible without zooming.
On narrow screens the cluster mini maps are also mounted lazily and unmounted once they are far offscreen, so mobile Chrome does not have to keep every MapLibre WebGL context alive at once.
That lazy-mount path now resynchronizes on `pageshow`, `focus`, and tab visibility changes as well, which makes forwarded HTML much more reliable inside in-app browsers that background the page before it is fully painted.

## Installed vs planned
Only `mesh_towers.source = 'seed'` is treated as already installed.
All other sources are treated as planned-but-not-installed towers that still need field work.
The export keeps installed seeds in the table for context, then ranks the planned towers locally inside each seed cluster.
Synthetic labels such as `route #40` are not shown as the primary name in the HTML.
Instead the HTML uses a location-first title built from the nearest place and road, with a smaller typed code such as `Route 40` or `Cluster 70`.

## Graph logic
The export prefers `mesh_visibility_edges_active` only when it covers the full live tower set.
Otherwise it falls back to `mesh_visibility_edges` so the handout does not silently drop tower relationships from planning. That table intentionally keeps even long invisible tower-to-tower gaps, because route heuristics and map review need those missing-link diagnostics too.
When the optional routed-geometry backfill has run, those same long invisible edges also carry a pgRouting corridor line in `geom`, so the handout and QGIS can show the actual surface route the planner would use between the two towers.
Installed seed towers define the starting backbone.
Seed-only connectivity defines the initial field clusters.
Planned towers are assigned to the nearest reachable seed cluster by shortest visible-path distance with deterministic tie-breaking.
When a tower has a known local country code and at least one reachable seed cluster in that same country, ownership stays in the same-country rollout queue even if a cross-border seed path is slightly shorter.
After that ownership pass, any same-country island that is not connected to its assigned seed inside the induced cluster is moved to the neighboring cluster that actually provides its visible predecessor chain.
This prevents a map section from showing planned nodes with no local install line back to an already available tower.
Cross-country rollout joins are still allowed as connector summaries when the visibility graph supports them, and they remain the fallback ownership path when no same-country seed cluster is reachable.
If a tower is disconnected from every installed seed, it is still clamped to the geometrically nearest installed seed cluster so the handout can show where that detached island belongs operationally.
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
Run `make data/out/install_priority.html` to build both deliverables from the current database state.
The export writes:
- `data/out/install_priority.html`
- `data/out/install_priority.csv`

Run `make data/out/install_priority_edges_checked` after exporting when you need a fail-fast check that every `primary_previous_tower_id` in the CSV points to an earlier row in the same cluster and has a direct visible edge in the current `mesh_visibility_edges` table.
Run `make data/out/install_priority_reviewed` for the full field-review gate: export exists, predecessor links are visible, bridge/cut-node assertion passes, and the bridge diagnostic TSV exists.

The HTML file includes a top summary table with one true “next node” per cluster, a MapLibre overview map, and one mini map per cluster.
The summary and detail tables now include captions, scoped headers, and labeled map links so screen readers can follow the same rollout data.
On narrow screens the summary table remains horizontally scrollable, while each cluster detail section swaps to mobile cards with the same values and links.
Solid lines show the recommended rollout path inside each cluster.
Dashed local lines on cluster mini maps show additional already-reachable same-cluster predecessors that were not chosen as the primary install edge.
This keeps links such as Komzpa to a later Batumi-cluster tower visible even when another predecessor is used for the ranked route line.
The default `Connect clusters` per-cluster table and mini map stop at the last current connector point.
The `Improve coverage` tab reveals the full list and a full cluster map for later hop-reduction and coverage work.
The overview map has the same `Connect clusters` and `Improve coverage` modes as the cluster detail sections.
Its default connector mode hides later local-improvement rows so the first screen emphasizes how rollout queues meet.
The full coverage mode shows every rollout number plus the merged Voronoi outline around each cluster’s current extent.
Dashed gray lines show the cheapest visible connector between rollout clusters.
Those links are context only and do not drive the install order itself.
The same overview now overlays reachable seed and MQTT import points as `s` and `m` markers.
For those `s`/`m` points, every unique undirected direct visible link from the live visibility graph is drawn as a thin context line so imported backbone candidates can be inspected without changing the rollout queue itself.
On the mini maps, those connector lines and large outer Voronoi edges no longer stretch the viewport away from the local cluster geometry.
`blocked` means the tower belongs to that rollout queue, yet there is still no visible path from any installed seed to that tower.
The basemap uses a simple OpenStreetMap raster tile style so the forwarded handout does not depend on MapLibre vector style, glyph, or sprite loading.
`maplibre-gl.js` and `maplibre-gl.css` are now vendored and inlined into the generated HTML, so a forwarded Telegram file does not need to fetch the MapLibre runtime from a CDN before it can draw the maps.
The map bootstrap now re-attaches overlays even when the basemap style is already cached and isolates per-layer failures, which makes the single HTML file more reliable when it is forwarded directly in mobile in-app browsers such as Telegram.
The same bootstrap now exposes `window.__installPriorityMaps` for phone-side debugging, which makes it possible to inspect which mini maps are currently mounted when a mobile browser drops overlays.
The large cluster tables keep the full data, but the HTML shortens the “Unlocks” preview so the page stays readable on screen.
The CSV file keeps the same flat rows for sharing, filtering, or printing elsewhere.
