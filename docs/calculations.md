# Calculation Steps and Optimizations
This document explains every calculation step in the pipeline, why it exists, and which optimizations keep it tractable.
See `docs/pipeline.md` for the end-to-end Make targets, and `docs/placement_strategies.md` for the “classical algorithm” naming.
See `docs/radio_model.md` for the path loss and clearance model assumptions.

## Hard Constants (and why they are hardcoded)
These values show up across SQL functions, procedures, and Make targets.
They are hardcoded because the pipeline is intended to be reproducible and easy to debug as a single “known-good” configuration.

- **H3 resolution: 8**
  This is the planning granularity for candidate tower locations and all derived metrics.
  It is dense enough to allow meaningful local moves (wiggle) while still being small enough to store and index country-scale surfaces.
- **Maximum LOS link distance: 70 km (`70000` meters)**
  This is the Meshtastic hop/link planning bound used by every LOS and (re)calculation radius.
  Keeping one shared radius means cache invalidations can be localized and consistent.
- **Minimum tower separation: 5 km (`5000` meters)**
  This prevents solutions that place towers nearly on top of each other and makes routing corridors avoid skimming existing infrastructure.
- **Tower height above ground: 28 m**
  This matches the planning assumption in `docs/talks/h3_talk.md` and is used in the Fresnel clearance calculation.
- **Frequency: 868 MHz (`868e6`)**
  This is a planning default used when converting clearance into a path loss estimate.

## Inputs → Normalized Layers
Every raw source is imported as-is, and only then converted into H3-backed layers.
This keeps provenance clear and prevents “pre-clipped” artifacts from hiding data issues.

### OSM extract (`osm_for_mesh_placement`) → Road geometry (`georgia_roads_geom`)
**Where:** `tables/georgia_roads_geom.sql`.
**What:** Filters OSM features down to car-accessible highways (Georgia + Armenia) and stores them as (multi)lines.
**Why:** Candidate towers must be serviceable by road for installation and maintenance.
**Optimization:** Do the highway/access filtering once up front so downstream H3 and routing steps never touch irrelevant OSM tags.

### OSM extract (`osm_for_mesh_placement`) → Boundary (`georgia_boundary`) → Convex hull (`georgia_convex_hull`)
**Where:** `tables/georgia_boundary.sql`, `tables/georgia_convex_hull.sql`.
**What:** Builds an admin-level-2 dissolved polygon (Georgia + Armenia) from the merged extract, then takes its convex hull.
**Why:** The convex hull is used as a computation domain so line-of-sight sampling paths do not fall off the edge of the dataset near borders.
**Optimization:** The hull keeps the domain simple (one polygon) so `h3_polygon_to_cells(...)` stays fast and predictable.

### Forbidden placement polygons (`georgia_unfit_areas`)
**Where:** `tables/georgia_unfit_areas.sql`.
**What:** Dissolves OSM administrative polygons for regions where installation is not allowed (for example, Abkhazia and South Ossetia).
**Why:** It is better to explicitly encode “do not place” areas than to rely on missing road/population data.
**Optimization:** A dissolved polygon + GiST index keeps the “is this cell unfit” test cheap during surface construction.

### Road geometry → Road coverage (`roads_h3_r8`)
**Where:** `tables/roads_h3_r8.sql`.
**What:** Segments road lines, converts segment start points into resolution-8 H3 cells, and aggregates total road length per cell.
**Why:** A boolean `has_road` gate is the simplest “installable” filter for later selection/routing.
**Optimization:** Segmentation makes long ways contribute to every traversed cell without requiring expensive polygon buffering.

### Kontur population polygons → Population per cell (`population_h3_r8`)
**Where:** `tables/population_h3_r8.sql`.
**What:** Aggregates Kontur population into H3 cells (Kontur already provides H3 ids) and intersects with the boundary.
**Why:** Population is the primary coverage objective for greedy placement.
**Optimization:** Prefer the provider’s H3 ids over re-tessellating polygons, which is both slower and easier to get subtly wrong.

### GEBCO raster → Elevation per cell (`gebco_elevation_h3_r8`)
**Where:** `scripts/raster_values_into_h3.sql` driven by the Make target `db/table/gebco_elevation_h3_r8`.
**What:** Imports the `gebco_2024_n90.0_s0.0_w0.0_e90.0.tif` tile, clips it to the convex hull, samples pixel centroids, converts them to H3 cells, and aggregates (average) elevation per cell.
**Why:** Every LOS computation depends on elevation values along the path.
**Optimization:** Import as raster tiles first and let PostGIS do the clipping and centroid extraction inside the database.

## Core Calculation Surface (`mesh_surface_h3_r8`)
**Where:** `tables/mesh_surface_h3_r8.sql`.
This is the main “planning surface” that every procedure reads and incrementally updates.

### Domain and geometry
- `h3` comes from `mesh_surface_domain_h3_r8`.
- `geom` is a stored generated boundary geometry for visualization.
- `centroid_geog` is a stored generated geography centroid for distance and KNN queries.

### Static indicators (computed once per rebuild)
- `ele` is filled from `gebco_elevation_h3_r8`.
- `has_road` is set when a cell exists in `roads_h3_r8`.
- `population` is filled from `population_h3_r8`.
- `is_in_boundaries` is computed by intersecting cell geometry with `georgia_boundary`.
- `is_in_unfit_area` is computed by intersecting cell geometry with `georgia_unfit_areas`.

### Tower state and spacing
- `has_tower` is derived from `mesh_towers` and is updated by routing, wiggle, and greedy steps.
- `distance_to_closest_tower` is computed as the minimum geography distance to any existing tower.
- `min_distance_to_closest_tower` defaults to 5000 meters and exists so spacing constraints can be overridden per cell when needed.
- `can_place_tower` is a generated boolean combining the “static gates” plus the spacing check.

### LOS-derived metrics (computed lazily, then refreshed locally)
- `clearance` and `path_loss` represent the best link from a cell to any nearby tower.
  The greedy loop and routing stages intentionally clear these fields to `null` in a radius around new towers.
  Functions then recompute only the invalidated region.
- `has_reception` is generated from `has_tower` or the presence of valid `clearance` and `path_loss`.
- `visible_tower_count` counts how many individual towers a cell can see within 70 km.
  The pipeline uses it as a hard constraint (`>= 2`) to avoid installing isolated towers.
- `visible_population` is the total population within 70 km that is visible from a candidate cell.
  It is computed per-cell by `mesh_surface_fill_visible_population(...)` and reused by the wiggle stage.
- `visible_uncovered_population` is the greedy objective value.
  It is invalidated (set to `null`) in a “double radius” around each newly added tower so recomputation always sees consistent neighbor coverage.
- `population_70km` is a non-LOS sum used only as a clustering weight for the population seeding stage.

### Indexing (the main performance levers)
- GiST indexes on `geom` and `centroid_geog` make neighborhood lookups and KNN (`<->`) queries usable.
- A BRIN index over the “big numeric columns” keeps sequential scans cheap after `vacuum`/`analyze`.
- A btree index on `distance_to_closest_tower` speeds up eligibility filtering.

## LOS and Path Loss Calculations
These functions are the “physics core” of the pipeline.

### `h3_visibility_clearance_compute(...)` and caching
**Where:** `functions/h3_visibility_clearance.sql`, `tables/mesh_los_cache.sql`.
**What:** Samples elevations along the H3 grid path between two cells, computes the worst Fresnel clearance, and derives a path loss estimate.
**Why:** The greedy placement and routing steps need a consistent “is this link viable” test and a numeric “how good is it” score.
**Optimization:** Results are stored in `mesh_los_cache` keyed by canonicalized `(src, dst, mast heights, frequency)` so repeated reruns do not redo expensive sampling.

### `h3_los_between_cells(a, b)`
**Where:** `functions/h3_los_between_cells.sql`.
**What:** A boolean helper that rejects links beyond 70 km and treats `clearance > 0` as LOS-visible.
**Why:** It is the most common predicate in the pipeline (`visible_tower_count`, visibility edges, candidate filtering).
**Optimization:** Make the hot-path boolean cheap and push detailed metrics into the cached function family.

### `h3_path_loss(...)`
**Where:** `functions/h3_path_loss.sql`.
**What:** Computes free-space path loss plus a single knife-edge diffraction penalty when clearance is negative.
**Why:** Routing needs a scalar edge cost that penalizes bad links even when they remain barely “visible”.
**Optimization:** Keep the model simple enough to run tens of millions of times during cache seeding.

## Diagnostics: Visibility Edges (`mesh_visibility_edges`)
**Where:** `tables/mesh_visibility_edges.sql`, `procedures/mesh_visibility_edges_refresh.sql`.
**What:** Materializes every tower-to-tower pair’s distance and LOS, and computes intra-cluster hop counts.
Each edge stores a `type` label that orders tower sources by stage priority (seed, population, route, bridge, cluster_slim, greedy) so pairs group consistently (for example `seed-route`).
**Why:** It is the debugging view for “is the graph connected” and “which pairs violate the hop budget”.
**Optimization:** Hop counts are computed via pgRouting on a temporary graph of only visible <=70 km edges.

### Routed geometry for invisible edges
**Where:** `tables/mesh_route_graph.sql`, `tables/mesh_route_graph_cache.sql`, `functions/mesh_visibility_invisible_route_geom.sql`.
**What:** When a diagnostic edge is invisible (or spans too many hops), generate a corridor geometry via pgRouting over an adjacency graph of all surface cells.
**Why:** QGIS/debug visualizations stay useful even when LOS does not exist between endpoints.
**Optimization:** Cache routed linework per canonical tower pair in `mesh_route_graph_cache` so refresh runs can reuse it.

## Routing and Placement Procedures (in the order they run)
This section explains what each procedure calculates and which optimization patterns it relies on.

### Population seeding (`mesh_population`)
**Where:** `procedures/mesh_population.sql`.
**What:** Clusters currently-uncovered candidate cells by population and installs one “population” tower per cluster when no existing tower covers it.
**Why:** It adds early anchors in dense areas before routing tries to connect components.
**Optimization:** Uses `population_70km` (non-LOS) weights and keeps clustering out of the greedy loop hot path.

### Cache and graph prep (`fill_mesh_los_cache`)
**Where:** `procedures/fill_mesh_los_cache.sql`.
**What:** Enumerates all eligible tower-or-candidate pairs within 70 km (excluding pairs closer than 5 km), computes missing LOS metrics, and builds a pgRouting graph with `path_loss_db` edge costs.
**Why:** Routing stages should spend time choosing corridors, not repeatedly recomputing LOS.
**Optimization:** Prioritizes missing-pair batches by distance to the nearest currently “problematic” visibility edge so reruns improve the most important gaps first.

### Cluster bridge (`mesh_route_bridge`)
**Where:** `procedures/mesh_route_bridge.sql`.
**What:** Finds a low-loss corridor between the most separated tower clusters and promotes intermediate cells as `route` towers until clusters connect.
**Why:** A connected backbone makes greedy placement avoid wasting towers on isolated islands.
**Optimization:** Works one corridor at a time so each iteration is a transaction with readable `NOTICE` logs.

### Cluster slim (`mesh_route_cluster_slim`)
**Where:** `procedures/mesh_route_cluster_slim.sql`, `tables/mesh_route_cluster_slim_failures.sql`.
**What:** For intra-cluster pairs exceeding 7 hops, routes candidate corridors and promotes intermediate towers so hop counts shrink.
**Why:** The 7-hop budget is a hard design constraint, so the graph must be “tightened” before maximizing population.
**Optimization:** Processes a bounded candidate batch per iteration, reuses existing towers on a corridor, and refreshes visibility diagnostics only when it actually installed new towers.

### Tower wiggle (`mesh_tower_wiggle`)
**Where:** `procedures/mesh_tower_wiggle.sql`, `docs/mesh_tower_wiggle.md`.
**What:** Locally relocates `population`, `route`, `bridge`, and `cluster_slim` towers to nearby road-served cells that keep LOS to their current neighbors while increasing visible population.
**Why:** Routing tends to pick “barely feasible” cells, so a refinement pass improves coverage without breaking connectivity.
**Optimization:** Each call moves at most one tower and recomputes metrics only within a radius around the old/new locations.

### Greedy placement (`mesh_run_greedy_prepare`, `mesh_run_greedy`, `mesh_run_greedy_finalize`)
**Where:** `procedures/mesh_run_greedy_prepare.sql`, `procedures/mesh_run_greedy.sql`, `procedures/mesh_run_greedy_finalize.sql`.
**What:** Repeatedly (a) fills missing RF metrics, (b) prefers a bridging candidate when multiple clusters exist, otherwise (c) selects the max `visible_uncovered_population` candidate and promotes it as a tower.
**Why:** This is the main “maximize covered population under constraints” heuristic from the talk.
**Optimization:** After each new tower, it invalidates only nearby cached fields and uses the localized refresh functions to avoid full-surface recomputation.
