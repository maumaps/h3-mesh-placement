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
- **Maximum LOS link distance: 100 km (`100000` meters)**
  This is the Meshtastic hop/link planning bound used by every LOS and (re)calculation radius.
  Keeping one shared radius means cache invalidations can be localized and consistent.
- **Minimum tower separation: 0 m by default**
  The default allows adjacent H3 placements; generated tower cleanup is handled later by cached-neighbor-set pruning, not by a blind spacing rule.
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
- `min_distance_to_closest_tower` defaults to 0 meters, so adjacent hexes are allowed unless a cell is explicitly overridden later.
- `can_place_tower` is a generated boolean combining the “static gates” plus the spacing check.

### LOS-derived metrics (computed lazily, then refreshed locally)
- `clearance` and `path_loss` represent the best link from a cell to any nearby tower.
  The greedy loop and routing stages intentionally clear these fields to `null` in a radius around new towers.
  Functions then recompute only the invalidated region.
- `has_reception` is generated from `has_tower` or the presence of valid `clearance` and `path_loss`.
- `visible_tower_count` counts how many individual towers a cell can see within 100 km.
  The pipeline uses it as a hard constraint (`>= 2`) to avoid installing isolated towers.
- `visible_population` is the total population within 100 km that is visible from a candidate cell.
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
**What:** A boolean helper that rejects links beyond 100 km and treats `clearance > 0` as LOS-visible.
**Why:** It is the most common predicate in the pipeline (`visible_tower_count`, visibility edges, candidate filtering).
**Optimization:** Make the hot-path boolean cheap and push detailed metrics into the cached function family.

### `h3_path_loss(...)`
**Where:** `functions/h3_path_loss.sql`.
**What:** Computes free-space path loss plus a single knife-edge diffraction penalty when clearance is negative.
**Why:** Routing needs a scalar edge cost that penalizes bad links even when they remain barely “visible”.
**Optimization:** Keep the model simple enough to run tens of millions of times during cache seeding.

## Diagnostics: Visibility Edges (`mesh_visibility_edges`)
**Where:** `tables/mesh_visibility_edges.sql`, `procedures/mesh_visibility_edges_refresh.sql`.
**What:** Materializes every tower-to-tower pair’s distance and LOS-like diagnostic state. Pairs within 100 km get a real LOS calculation; longer pairs are still stored as invisible diagnostic edges so routing and debug views can target long gaps between existing towers. Intra-cluster hop counts are then computed on the visible <=100 km subgraph.
Each edge stores a `type` label that orders tower sources by stage priority (seed, coarse, route, bridge, cluster_slim, greedy, plus legacy population) so pairs group consistently (for example `seed-route`).
**Why:** It is both the debugging view for “is the graph connected” / “which pairs violate the hop budget” and the geometric guide rail for route expansion, because cache seeding and route corridor selection measure distance to currently invisible tower-to-tower gaps.
**Optimization:** The expensive LOS function runs only for pairs within 100 km, while longer tower pairs are kept as pre-marked invisible edges. Hop counts are computed via pgRouting on the temporary graph of only visible <=100 km edges, and connected components are derived from that same graph instead of recalculating tower LOS a second time.

### Routed geometry for invisible edges
**Where:** `tables/mesh_route_graph.sql`, `tables/mesh_route_graph_cache.sql`, `functions/mesh_visibility_invisible_route_geom.sql`, `scripts/mesh_visibility_edges_refresh_route_geom.sql`.
**What:** When a diagnostic edge is invisible (or spans too many hops), generate a corridor geometry via pgRouting over an adjacency graph of all surface cells.
For long invisible tower-to-tower gaps, `mesh_visibility_invisible_route_geom()` runs `pgr_dijkstra(...)` over `mesh_route_graph_edges`, returns the minimum-cost corridor, and caches it in `mesh_route_graph_cache`.
`mesh_visibility_edges_refresh_route_geom()` then rewrites `mesh_visibility_edges.geom` for inter-cluster invisible edges and for intra-cluster edges whose stored `cluster_hops` still exceed the hop budget.
**Why:** QGIS/debug visualizations stay useful even when LOS does not exist between endpoints, and long invisible edges keep a concrete route line that explains where the planner will try to insert intermediate towers.
**Optimization:** Cache routed linework per canonical tower pair in `mesh_route_graph_cache` so refresh runs can reuse it.
`mesh_visibility_edges_refresh_route_geom()` now joins `mesh_route_graph_cache` first and only calls the pgRouting helper for cache misses, which avoids repeated PL/pgSQL overhead on reruns.
With zero separation, the route helper also blocks occupied route nodes by exact tower H3 match instead of scanning route nodes through `ST_DWithin(...)`.
Keep this routed-geometry backfill separate from the normal `db/procedure/mesh_route_refresh_visibility` target, because route geometry should not block every visibility refresh. The pipeline now runs `db/procedure/mesh_visibility_edges_route_priority_geom` automatically before `fill_mesh_los_cache_prepare`, so backfill priority uses routed corridors instead of straight tower-to-tower chords.

## Routing and Placement Procedures (in the order they run)
This section explains what each procedure calculates and which optimization patterns it relies on.

### Coarse backbone seeding (`mesh_coarse_grid`)
**Where:** `procedures/mesh_coarse_grid.sql`.
**What:** Groups fine H3 candidates under a coarser H3 parent, skips every coarse parent that already contains a tower, and installs at most one `coarse` tower per remaining parent.
**Why:** It adds sparse, well-separated anchors before routing without piling new towers into places that already have seed coverage.
**Optimization:** Uses `distance_to_closest_tower` as the primary spacing score, prefers `has_building = true` before other tie-breakers, and falls back to `population_70km`, `building_count`, and stable H3 ordering.

### Population anchor seeding (`mesh_population`)
**Where:** `procedures/mesh_population.sql`.
**What:** Selects a small fixed-k set of serviceable city anchors before route bootstrap and routing.
**Why:** The routing graph needs a few meaningful population/service anchors so bridge placement does not connect clusters only through empty mountain shortcuts.
**Optimization:** Uses `ST_ClusterKMeans(..., k)` with configured `population_anchor_max_count`; no production place names are hardcoded.
Population demand is clustered before coverage filtering, and existing towers are mixed into the same KMeans input as very heavy anchor points (`population_existing_anchor_weight`).
After clustering, clusters that already contain an existing tower anchor are dropped.
This keeps real settlement geometry visible to KMeans without adding duplicate population anchors inside already served islands.
The score interleaves nearby population and building count with `power(ln(1 + population_70km), population_nearby_population_weight) * power(ln(2 + building_count), population_building_weight)`, so cells with both people nearby and plausible buildings/houses win naturally.

### Route bootstrap (`mesh_route_bootstrap`)
**Where:** `tables/mesh_route_bootstrap_pairs.sql`, `scripts/mesh_route_bootstrap.sql`.
**What:** Loads installer-priority CSV points, current placed towers, current placed towers including configured population anchors, manual warmup points, nearest placeable hexes for OSM peaks, and explicit nearest links from disconnected coarse clusters toward other placed towers into a single bootstrap pair set, then seeds `mesh_los_cache` for those pairs before the generic all-pairs cache fill starts.
**Why:** Route planning needs some known rollout-corridor and mountain-top LOS in cache before the huge generic pair search can meaningfully connect clusters.
**Optimization:** Manual points, placed towers, and disconnected coarse-cluster links get lower `bootstrap_rank` than generic installer CSV rows, and OSM peaks are snapped to the nearest placeable H3 cell so the warmup set stays routeable instead of seeding impossible mountain coordinates.

### Cache and graph prep (`fill_mesh_los_cache`)
**Where:** `scripts/fill_mesh_los_cache_prepare.sql`, `scripts/fill_mesh_los_cache_batch.sql`, `scripts/fill_mesh_los_cache_finalize.sql`.
**What:** Enumerates all eligible tower-or-candidate pairs within 100 km, commits one missing-LOS batch in the normal pipeline, and then builds a pgRouting graph with `path_loss_db` edge costs from the currently available cache.
**Why:** Routing stages should spend time choosing corridors, not repeatedly recomputing LOS.
**Optimization:** Orders missing-pair batches by building-bearing endpoints first, then by distance to the nearest currently “problematic” visibility edge, then by total `building_count`, after the smaller `mesh_route_bootstrap` stage has already warmed the cache around installer-priority corridors.
`fill_mesh_los_cache_prepare` now materializes the filtered invisible edges and disconnected towers into small GiST-backed staging tables first, because profiling showed the old correlated subplans spending most of their time in repeated index rescans, tuple allocation/free, and geography-to-geometry conversions.
Inside `h3_visibility_clearance()`, the hot sampling loop now uses the ordinality from `h3_grid_path_cells()` instead of projecting every sample cell through `ST_PointOnSurface(...)` or `ST_LineLocatePoint(...)`, because profiling the batch workers showed those PostGIS geometry conversions dominating CPU time before the actual Fresnel math ran. Elevation lookups in that same loop now come from the narrow `gebco_elevation_h3_r8` table instead of the wide `mesh_surface_h3_r8` table, which removes extra heap traffic from the batch hot path. Endpoint elevations are also derived from that same sampled path, so each LOS pair no longer performs two extra point lookups before scanning the path cells.
Each batch is committed separately so rerunning the manual backfill target resumes instead of discarding earlier work.
Disconnected-cluster distance is now ranked ahead of generic invisible-edge distance, so cache seeding prefers corridors that can attach currently isolated tower groups before spending work on less urgent pairs.
Use `db/procedure/fill_mesh_los_cache_backfill` later when you want to drain more of the queue and rebuild the route graph from a fuller cache.

The operator-facing parallel launcher `db/procedure/fill_mesh_los_cache_parallel` reads `los_batch_limit` and optional `los_parallel_jobs` from `tables/mesh_pipeline_settings.sql`, snapshots the current `mesh_route_missing_pairs` length into `ceil(count(*) / los_batch_limit)` finite jobs, and feeds that list into GNU parallel.
When `los_parallel_jobs` is `0`, the launcher omits `--jobs` and lets GNU parallel use its CPU-count default.
Each GNU parallel job runs exactly one committed `fill_mesh_los_cache_batch.sql` invocation, and the existing `for update skip locked` claim logic guarantees that concurrent jobs still pull disjoint queue slices without any pre-assigned chunk ids.
This makes GNU parallel ETA meaningful for that run, because the job count is fixed at launch time instead of being hidden behind infinite shell loops.
Late-start jobs that find no rows left to claim now exit cleanly instead of failing the whole run, while claimed batches that compute zero cache rows still fail loudly as a real inconsistency.

### Cluster bridge (`mesh_route_bridge`)
**Where:** `procedures/mesh_route_bridge.sql`.
**What:** Finds a low-loss corridor between the closest disconnected tower clusters and promotes intermediate cells as `route` towers until clusters connect.
**Why:** A connected backbone makes greedy placement avoid wasting towers on isolated islands.
**Optimization:** Works one corridor at a time so each iteration is a transaction with readable `NOTICE` logs.
Ranks cluster pairs by the minimum tower-to-tower gap, not centroid spread, so the stage spends time on pairs the current partial route graph can realistically bridge.
Also filters pairs to clusters that share a precomputed connected component in `mesh_route_edge_components`, because `pgr_dijkstra` cannot bridge disjoint route-graph components with the current cached LOS.
Each invocation also caps how many failed cluster-pair attempts it will burn through before returning, so the iterative pipeline keeps moving even when the current route graph is still sparse.
Inside one attempt, routing is anchored on the nearest tower-node pair between the two clusters instead of every tower in both clusters, which keeps each `pgr_dijkstra` call tractable.
After route towers are inserted, local surface visibility/reception refresh is deferred to the later route-refresh stage instead of being recomputed synchronously inside bridge.

### Cluster slim (`mesh_route_cluster_slim`)
**Where:** `procedures/mesh_route_cluster_slim.sql`, `tables/mesh_route_cluster_slim_failures.sql`.
**What:** For intra-cluster pairs exceeding 7 hops, routes candidate corridors and promotes intermediate towers so hop counts shrink.
**Why:** The 7-hop budget is a hard design constraint, so the graph must be “tightened” before maximizing population.
**Optimization:** Processes a bounded candidate batch per iteration and reuses existing towers on a corridor.
Like bridge, it now defers local surface and visibility refresh to the later route-refresh stage instead of recomputing them synchronously inside each cluster-slim iteration.


### Route segment reroute (`mesh_route_segment_reroute`)
**Where:** `procedures/mesh_route_segment_reroute.sql`.

**What:** Optimizes local `endpoint -> route -> route -> endpoint` chains after route contraction.
The pass only touches generated route-like relays whose cached non-population LOS degree is exactly two, so it does not steal towers that have extra external obligations.
For each safe two-relay segment it searches cached LOS for a replacement pair `endpoint -> A -> B -> endpoint`, ranks by building count first and population second, and moves both relay tower IDs together only when the pair improves over the current two H3 cells.
This covers cases that single-node `mesh_tower_wiggle` cannot fix because each existing relay is locally constrained but the pair can improve jointly.

**Data safety:** The pass reads only `mesh_los_cache` for RF feasibility.
It updates `mesh_towers` and invalidates local `mesh_surface_h3_r8` metrics around old and new relay cells, then the pipeline refreshes `mesh_visibility_edges`.

### LOS cache backup (`backup_mesh_los_cache`)
**Where:** `scripts/backup_mesh_los_cache.sh`, `scripts/restore_mesh_los_cache.sh`.

**What:** `make -B db/procedure/backup_mesh_los_cache` writes `data/backups/mesh_los_cache.latest.dump` and a timestamped copy with `pg_dump --format=custom`.
The backup script refuses to overwrite the latest backup when `mesh_los_cache` is missing or empty unless `ALLOW_EMPTY_LOS_CACHE_BACKUP=1` is explicitly set.
The restore script renames any existing `mesh_los_cache` to a timestamped quarantine table before loading the dump, so restore does not destroy the last in-database cache copy.
Use backup before destructive placement experiments or explicit integration tests such as `db/test/mesh_route_integration`; the default `make test` target is not supposed to rebuild cache-adjacent tables.

### Tower wiggle (`mesh_tower_wiggle`)
**Where:** `procedures/mesh_tower_wiggle.sql`, `docs/mesh_tower_wiggle.md`.
**What:** Locally relocates `coarse`, `route`, `bridge`, and `cluster_slim` towers to nearby road-served cells that keep LOS to their current neighbors while increasing visible population.
**Why:** Routing tends to pick “barely feasible” cells, so a refinement pass improves coverage without breaking connectivity.
**Optimization:** Each call moves at most one tower, invalidates nearby cached RF fields, and defers heavy local and global visibility refresh to the later route-refresh stage instead of recomputing them inside every single move.

### Greedy placement (`mesh_run_greedy_prepare`, `mesh_run_greedy`, `mesh_run_greedy_finalize`)
**Where:** `procedures/mesh_run_greedy_prepare.sql`, `procedures/mesh_run_greedy.sql`, `procedures/mesh_run_greedy_finalize.sql`.
`mesh_run_greedy_prepare` only resets tower-derived surface state and nearest-tower distances.
The LOS, visible-tower-count, and reception recomputation happens incrementally inside each greedy iteration so the pipeline can continue without a monolithic pre-pass.
The configured wrapper runs greedy iterations with `statement_timeout=0` so those LOS computations are not canceled mid-run, but the checked-in pipeline config currently skips the stage with `enable_greedy=false`.
**What:** Repeatedly (a) fills missing RF metrics, (b) prefers a bridging candidate when multiple clusters exist, otherwise (c) selects the max `visible_uncovered_population` candidate and promotes it as a tower.
**Why:** This is the main “maximize covered population under constraints” heuristic from the talk.
**Optimization:** The prepare step now uses KNN nearest-tower lookup for per-cell anchor selection, and the loop invalidates only nearby cached fields so later iterations avoid full-surface recomputation.

`fill_mesh_los_cache` keeps `mesh_route_missing_pairs` on disk between committed batches so reruns can resume from the reduced queue instead of starting over.
Before each resume loop, `scripts/fill_mesh_los_cache_queue_indexes.sql` ensures the live queue has both the exact-match `(src_h3, dst_h3)` btree for delete/join tails and the batch-order btree `(building_endpoint_count desc, disconnected_priority, priority, building_count desc, src_h3, dst_h3)` so each committed batch can pull its next slice without a full parallel sort of `mesh_route_missing_pairs`.

### Cluster slim (`mesh_route_cluster_slim`)
**Where:** `procedures/mesh_route_cluster_slim.sql`, `tables/mesh_route_cluster_slim_failures.sql`.
**What:** For intra-cluster pairs exceeding 7 hops, routes candidate corridors and promotes intermediate towers so hop counts shrink.
**Why:** The 7-hop budget is a hard design constraint, so the graph must be “tightened” before maximizing population.
**Optimization:** Processes one corridor per invocation, stores failed pair attempts, and reuses `mesh_route_corridor_between_towers(...)` so the expensive pgRouting graph stays centralized.
Like bridge, it now defers local surface and visibility refresh to the later route-refresh stage instead of recomputing them synchronously inside each cluster-slim iteration.

### Population anchor contraction (`mesh_population_anchor_contract`)
**Where:** `procedures/mesh_population_anchor_contract.sql`.
**What:** Removes soft population anchors after routing when a generated route-like tower preserves their cached non-population LOS neighbor set.
**Why:** Population anchors are demand hints for routing, not mandatory final tower coordinates; keeping them as hard terminals can create small star-shaped route blobs around arbitrary KMeans centroids.
**Optimization:** Uses only `mesh_los_cache` and `population_anchor_contract_distance_m`; `0` means topology-only replacement search.
It does not use local-population or building-count thresholds, and it never calls fresh terrain LOS functions.
Generated route leaves around a contracted anchor are removed only when the chosen replacement also preserves the leaf's non-population visible-neighbor set.

### Generated pair contraction (`mesh_generated_pair_contract`)
**Where:** `procedures/mesh_generated_pair_contract.sql`.
**What:** Replaces close route-like tower pairs with one synthetic H3 when that H3 preserves the combined cached non-population LOS-neighbor set of both towers.
**Why:** Some local route blobs are not population anchors; they are two generated relays around the same local service area.
A distance-only merge is unsafe, but a cached-topology-preserving synthetic replacement can remove the duplicate without breaking route edges.
**Optimization:** Uses `mesh_los_cache` only; `generated_tower_merge_distance_m` bounds pair selection and synthetic H3 search.
