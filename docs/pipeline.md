# H3 Mesh Placement Pipeline
This repository codifies the Meshtastic coverage story from `doc/H3 talk.md` into a reproducible data pipeline.
PostGIS, `h3_pg`, GDAL utilities, and osmium are all mandatory dependencies.

## Data Sources
Georgia roads and borders come from `https://download.geofabrik.de/europe/georgia-latest.osm.pbf`.
The entire extract is streamed through `osmium export` using `osmium.config.json` and copied into the `osm_georgia` table, after which every filter happens inside SQL.
Only OSM highways that allow general car traffic (motorway/trunk/primary/secondary/tertiary, their *_link variants, unclassified, residential, living_street, service, and road) survive the `georgia_roads_geom` stage so downstream candidates stay car-accessible.
Population counts originate from `https://geodata-eu-central-1-kontur-public.s3.eu-central-1.amazonaws.com/kontur_datasets/kontur_population_20231101.gpkg.gz` and are imported wholesale; the SQL stages intersect them with the Georgia boundary so no pre-clipping is needed.
Bathymetry and elevation come from the official GEBCO 2024 GeoTIFF download hosted by BODC (`https://www.bodc.ac.uk/data/open_download/gebco/gebco_2024/geotiff/`). The Makefile downloads the published `gebco_2024_geotiff.zip`, unzips the handful of GeoTIFF tiles, and feeds them directly into `raster2pgsql -t auto` so PostGIS handles tiling.
Initial tower candidates live in `data/in/existing_mesh_nodes.geojson`.
All datasets stay under `data/in` and decompressed or filtered artifacts are mirrored to `data/mid`.

## Layout
The repository mirrors the structure described in the geocint-runner documentation: `functions/` for reusable SQL, `procedures/` for long-running DO blocks, `tables/` for table DDL/DML, `osmium.config.json` for the streaming import, and `data/` split into `in`, `mid`, and `out` subfolders.
Make targets drop marker files under `db/raw`, `db/table`, `db/function`, and `db/procedure`, just like the upstream pipeline.

## Make Targets
`make all` runs the entire workflow: download data, ingest it into PostGIS, convert everything to H3 resolution 8, and execute the greedy placement logic.
Intermediate targets such as `db/table/osm_georgia` or `db/raw/kontur_population` can be run independently to debug single steps.
Every target follows the `target: deps | order_deps ## comment` convention embraced by geocint so the pipeline graph remains explicit.
All thresholds (resolution 8, 60 km link limit, 5 km separation) come from the talk outline and are hardcoded to keep the Makefile knob-free.
`db/table/mesh_surface_h3_r8` now depends on `db/table/mesh_initial_nodes_h3_r8`, so invoking any routing or visibility stage automatically refreshes the seed towers from `data/in/existing_mesh_nodes.geojson` before their downstream tables rebuild.
`db/procedure/mesh_route_cache_graph` seeds the LOS cache for every tower/candidate pair within 70 km (skipping anything closer than the 5 km separation limit) and materializes the pgRouting graph used for bridging.
It now tags every missing pair with a priority derived from the closest blocked visibility edge so each rerun burns down the most urgent million links first.
`db/procedure/mesh_route_bridge` consumes that graph, runs pgRouting to connect the most distant tower clusters, installs any intermediate towers along the best route with `source=route`, and refreshes reception/visibility metrics around every promoted cell.
`db/procedure/mesh_route_refresh_visibility` reruns `scripts/mesh_visibility_edges_refresh.sql` so QGIS/debug layers include the new bridges.
`db/procedure/mesh_tower_wiggle` runs after routing to nudge `route`, `cluster_slim`, and `bridge` towers toward higher visible population while preserving LOS to their current neighbors.
The Make target seeds the dirty queue on the first call, then loops `mesh_tower_wiggle(false)` until no dirty towers remain so every move is its own transaction with clear `NOTICE` logs.
The queue prioritizes towers with the fewest prior wiggles, filters candidates to road-served cells that keep LOS to all visible neighbors, and re-dirties neighbors when a tower moves.
See `docs/mesh_tower_wiggle.md` for the full step-by-step description.

## Database Layout
Everything lives directly in the default PostgreSQL namespace, so table names are globally unique: `osm_georgia`, `kontur_population`, `georgia_boundary`, `mesh_surface_h3_r8`, etc.
The final surface table exposes `h3`, `geom`, `ele`, `has_road`, `population`, `has_tower`, `has_reception`, `is_in_boundaries`, `is_in_unfit_area`, `min_distance_to_closest_tower`, `visible_uncovered_population`, `distance_to_closest_tower`, and the generated `can_place_tower` flag.
Indexes use BRIN where possible so we can vacuum and freeze quickly even as the dataset grows.
`mesh_towers` now also tracks a `recalculation_count` so the wiggle pass can prioritize towers that have moved the least, and `mesh_tower_wiggle_queue` keeps the persistent dirty list between function calls.

## SQL Execution Order
`tables/mesh_pipeline_settings.sql`, `tables/mesh_towers.sql`, and `tables/mesh_greedy_iterations.sql` install the core bookkeeping tables.
`tables/georgia_boundary.sql`, `tables/georgia_convex_hull.sql`, `tables/mesh_surface_domain_h3_r8.sql`, `tables/georgia_roads_geom.sql`, `tables/roads_h3_r8.sql`, `tables/population_h3_r8.sql`, and `scripts/raster_values_into_h3.sql` (for GEBCO) harmonize all imported layers into the H3 resolution 8 domain.
`functions/h3_los_between_cells.sql` defines the helper needed for line-of-sight checks on top of `h3_pg` primitives.
`tables/mesh_surface_h3_r8.sql` builds the final surface table, fills the requested indicator columns, tracks `visible_tower_count` (the number of towers with LOS within 70 km), and sets up constraints plus indexes.
`tables/mesh_visibility_edges.sql` materializes the visibility layer for every tower pair so we can inspect the expected connectivity.
That table also flags whether each edge connects towers from different LOS clusters so downstream priorities can focus on true bridge gaps.
It now stores `cluster_hops`, the minimum number of LOS hops inside a cluster, so long daisy chains are easy to spot in tooling.
`tables/mesh_route_graph_cache.sql` installs a simple cache so already-routed tower pairs can skip pgRouting altogether, and `tables/mesh_route_graph.sql` precomputes the reusable routing graph covering every boundary-approved, placement-eligible cell plus pgRouting-ready adjacency weights of `1 + (population = 0) + (has_road = false) + (source elevation > target elevation)` while truncating the cache whenever the underlying graph changes.
When the refresh script populates the visibility table, any invisible edge that spans towers in different clusters now calls `mesh_visibility_invisible_route_geom()` so the stored geometry follows an actual sequence of adjacent H3 cells instead of a straight chord.
Edges that stay inside a cluster but span at least eight hops borrow the same routed geometries so the map highlights realistic corridors instead of misleading straight segments.
Those fallback routes now reuse the cached graph directly—pgRouting can explore the entire administrative surface, so the diagnostic layer always finds the globally cheapest populated/road-friendly corridor that connects the two towers.
All pgRouting calls now drop intermediate edges whose cells sit within 5 km of any already installed tower (endpoints are exempt), guaranteeing that new corridors respect the spacing rules instead of skimming right next to existing infrastructure.
`procedures/mesh_route_cache_graph.sql` preloads LOS metrics for all tower-or-candidate pairs (excluding pairs that violate the 5 km spacing guard) and stores the routing graph artifacts.
It also orders the million-pair cache fill batches by proximity to existing inter-cluster, non-visible tower edges, which highlights blind spots before long-tail pairs.
`procedures/mesh_route_bridge.sql` consumes that graph, calls the helper function that returns intermediate hexes for the best pgRouting path, and iteratively connects the farthest tower clusters while marking the newly promoted towers with the `route` source.
`procedures/mesh_route_cluster_slim.sql` now runs immediately afterward, scans the refreshed `mesh_visibility_edges` hop counts, and injects additional routing towers along the heaviest intra-cluster paths until every remaining pair stays below seven hops.
Each iteration now pulls up to 256 candidate visibility pairs (instead of 64) so long cluster queues can be evaluated in a single routing sweep.
The sharing score only credits a corridor when it shortens longer, still-viable candidates that reuse the same nodes, so mirror paths or already rejected routes no longer skew prioritization.
Each stored-procedure call processes a single corridor so every iteration becomes its own transaction; the Makefile target now truncates the `mesh_route_cluster_slim_failures` bookkeeping table, loops until the procedure reports that zero new towers were promoted, and you can cap that loop by setting `SLIM_ITERATIONS=<n>` (zero keeps the default “run-until-idle” behavior).
That failure log ensures subsequent iterations skip corridors already marked as completed or permanently blocked during the current run, while the Makefile reset lets a fresh run reconsider everything from scratch once new towers are available.
It gathers a batch of over-limit edges with seed towers on at least one endpoint first, routes every pair in one pgRouting sweep, bans the pairs that still cannot be improved, and then accepts the corridor whose proposed new towers are shared by the most other pairs (breaking ties by hop reduction and average hop length).
Existing towers along a corridor are now reused instead of being filtered out, every routed path is validated to ensure each newly promoted node has actual LOS to the previous hop (otherwise the pair is skipped and logged), and the notice log reports how many towers were newly promoted versus reused plus per-stage timing so long-running (~30 minute) iterations are easier to monitor.
The stage reruns the visibility refresh only when it actually promoted new towers, which keeps slow refreshes off the hot path when a corridor merely reuses existing infrastructure.
`procedures/mesh_run_greedy_prepare.sql` should run immediately afterward; it now lives as a stored procedure so both `make db/procedure/mesh_run_greedy` and tests call the exact same reset logic.
That reset now deletes only towers whose `source` is `greedy` or `bridge`, replays the routing/cluster-slim towers back onto `mesh_surface_h3_r8`, refreshes derived metrics, and keeps every upstream placement available for subsequent greedy iterations.
`procedures/mesh_run_greedy.sql` implements the loop from the talk: fill missing fields, find the best candidate, promote it to a tower, invalidate nearby caches, and repeat until no population remains uncovered.
It only promotes candidates whose `visible_tower_count` is at least two so both bridge and greedy placements always see multiple existing towers.
When several bridge-mode candidates tie on blocked-cluster metrics, the tiebreaker first prefers the option that unlocks the most new road-accessible cells whose `visible_tower_count` is still below two before comparing populations, and the greedy fallback applies the same preference.
`functions/mesh_surface_refresh_reception_metrics.sql` recomputes `clearance` and `path_loss` for the cells whose metrics were invalidated around the most recent tower, so there are no null “holes” in QGIS after an iteration finishes.
`functions/mesh_surface_refresh_visible_tower_counts.sql` recomputes `visible_tower_count` in the 70 km radius around every accepted tower, keeping eligibility filters accurate during the greedy loop.

## Running The Pipeline
Make sure PostgreSQL accepts local socket connections under your current user so the inline `psql` calls succeed.
Execute `make all` to refresh every artifact from downloads to greedy placement.
Each SQL file is idempotent, so you can re-run `psql -f tables/<file>` (or `functions/<file>`, `procedures/<file>`) for debugging without destroying prior work.
The TODO list in `docs/todo.md` tracks missing credentials or tooling gaps noticed during development.
