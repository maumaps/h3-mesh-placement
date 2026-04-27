# H3 Mesh Placement Pipeline
This repository codifies the Meshtastic coverage story from `docs/talks/h3_talk.md` into a reproducible PostGIS pipeline.
The pipeline runs inside PostgreSQL using PostGIS, `h3_pg`, and pgRouting.
`osmium`, `ogr2ogr`, and `raster2pgsql` are used only for importing raw datasets into PostGIS.
See `docs/calculations.md` for a step-by-step description of every calculation and optimization.
See `docs/placement_strategies.md` for the terminology used for each placement stage.
See `docs/radio_model.md` for the radio model assumptions and LongFast defaults.
See `docs/visuals_mapnik.md` for rendering and animation instructions.
See `docs/install_priority_handout.md` for the cluster-local installer handout export.

## Data Sources
Georgia roads come from `https://download.geofabrik.de/europe/georgia-latest.osm.pbf`.
Armenia borders come from `https://download.geofabrik.de/asia/armenia-latest.osm.pbf`.
The extracts are merged at the dump level and streamed through `osmium export` using `osmium.config.json` into the `osm_for_mesh_placement` table.
Only OSM highways that allow general car traffic survive the `georgia_roads_geom` stage.
Population counts originate from `https://geodata-eu-central-1-kontur-public.s3.eu-central-1.amazonaws.com/kontur_datasets/kontur_population_20231101.gpkg.gz`.
Elevation comes from the official GEBCO 2024 GeoTIFF download hosted by BODC (`https://www.bodc.ac.uk/data/open_download/gebco/gebco_2024/geotiff/`).
Only the `gebco_2024_n90.0_s0.0_w0.0_e90.0.tif` tile is imported because it covers Georgia + Armenia while keeping database storage manageable.
Initial tower candidates live in `data/in/existing_mesh_nodes.geojson`.

## Layout
The repository mirrors the geocint-runner structure.
`tables/` contains table DDL/DML for derived layers.
`functions/` contains reusable SQL functions.
`procedures/` contains long-running, stage-level procedures.
`scripts/` contains “glue SQL” that depends on `psql -v` variables.
Make targets write marker files under `db/raw`, `db/table`, `db/function`, `db/procedure`, and `db/test`.
Use `python scripts/check_db_markers.py --fix` if a marker exists but the database object was never created.
Some `db/procedure` markers represent pipeline stage scripts rather than actual Postgres procedures.
The LOS-cache backup itself is stored under `data/backups/` so `make clean` does not delete it with rendered outputs.

## Pipeline Targets
`make all` runs the workflow from downloads through routing with coarse and greedy placement disabled by default.
The single user-editable pipeline configuration file is `tables/mesh_pipeline_settings.sql`.
Edit that file, run `make db/table/mesh_pipeline_settings`, and then restart the affected placement targets.
Stage toggles currently live there as `enable_coarse=false`, `enable_population=true`, `enable_population_anchor_contract=true`, `enable_route_bridge=true`, `enable_cluster_slim=true`, `enable_greedy=false`, and `enable_wiggle=true`.
Use `make db/procedure/mesh_run_greedy_full` after setting `enable_greedy=true` when you want to execute the greedy placement loop.
Use `make db/procedure/mesh_placement_restart` when the database and LOS cache already exist and you only need to replay configured tower placement stages without rebuilding cached tables from stale Make markers.
Use `make db/procedure/mesh_route_cluster_slim_current` when route bridge has already produced a current route graph and you only need to resume cluster slimming without letting Make replay route bridge or base imports.
If that cluster-slim target is interrupted after it has already inserted `cluster_slim` towers, rerun it with `SLIM_RESUME=1` so the wrapper preserves existing towers and resumes from the latest prepared iteration instead of clearing the stage.
Intermediate targets such as `db/table/osm_for_mesh_placement` or `db/table/mesh_surface_h3_r8` can be executed independently to debug single steps.
Use `make data/out/install_priority_reviewed` when you want the installer handout after routing or greedy placement with the field-review invariants checked.

The key stage order is:
- Import sources → merge curated seeds with the optional Liam Cottle snapshot → import `mesh_initial_nodes`.
- Derive boundary/roads/population/buildings/elevation layers → build `mesh_surface_h3_r8`.
- Seed and MQTT towers (`mesh_initial_nodes_h3_r8` → `mesh_towers`).
  MQTT imports are treated as already-installed infrastructure so visibility, routing, and the installer handout use them as graph roots instead of drawing them only as overview markers.
- Apply configured coarse backbone anchors (`mesh_coarse_grid`); the checked-in config currently disables this stage and removes stale `source = 'coarse'` towers on restart.
- Apply configured population anchors (`mesh_population`); the checked-in config clusters the full serviceable demand field, mixes existing towers in as heavy anchors, drops already-anchored clusters, and keeps up to 7 `source = 'population'` anchors.
- Snapshot `data/out/install_priority.csv` into `data/in/install_priority_bootstrap.csv`, merge in `data/in/install_priority_bootstrap_manual.csv`, current placed towers including configured population anchors, nearest placeable OSM peaks, and explicit nearest links from disconnected coarse clusters toward other placed towers, then load bootstrap LOS pairs (`mesh_route_bootstrap_pairs`).
- Seed `mesh_los_cache` from those bootstrap pairs first (`mesh_route_bootstrap`).
- Cache LOS metrics and build routing graph (`fill_mesh_los_cache`).
- Connect clusters (`mesh_route_bridge`) and tighten hop counts (`mesh_route_cluster_slim`), preferring same-country route work before cross-country fallback links, then contract soft population anchors, close generated route pairs whose cached LOS-neighbor roles are preserved, and reroute local two-relay route segments when a better cached-LOS relay pair exists.
- After `mesh_route_bridge` and every later route-mutating stage, run `scripts/assert_mesh_towers_single_los_component.sql` against live `mesh_towers` and cached positive-clearance LOS links.
  The route pipeline must keep all live towers in one LOS-connected component; any disconnected tower fails the stage before the next marker is touched.
- Apply the configured greedy placement loop (`mesh_run_greedy_prepare` + iterative `mesh_run_greedy` + `mesh_run_greedy_finalize`) via `mesh_run_greedy_full`; the checked-in config currently disables this stage and removes stale `source = 'greedy'` towers on restart.
- Apply the configured local routed-tower refinement (`mesh_tower_wiggle`) after routing and greedy cleanup; the checked-in config currently enables it via `enable_wiggle=true`.

The default constants in `tables/mesh_pipeline_settings.sql` are:
- H3 resolution 8 (`h3_res`).
- 100 km maximum real LOS computation distance (`max_los_distance_m`).
- 50,000 LOS pairs per committed batch (`los_batch_limit`).
- GNU parallel's CPU-count default for finite cache-fill runs (`los_parallel_jobs=0`).
- Up to 7 fixed-k population anchors (`population_anchor_max_count`), with `population_anchor_cluster_oversampling` controlling extra KMeans clusters before already-anchored clusters are dropped.
- `mesh_visibility_edges` still keeps longer tower-to-tower diagnostic edges as invisible route targets; only the expensive LOS calculation is capped at 100 km.
- Before `fill_mesh_los_cache_prepare`, `db/procedure/mesh_visibility_edges_route_priority_geom` uses pgRouting to draw fallback corridor geometry for the same invisible or over-hop visibility edges that drive cache priority, so backfill follows routed gaps instead of straight chords.
- No minimum tower separation; adjacent H3 cells are allowed.
See `docs/calculations.md` for where they appear in SQL and why they are shared.

Configured iteration caps:
- `cluster_slim_iterations` controls the cluster-slim loop, with `SLIM_ITERATIONS=<n>` still available as a one-run override.
- `greedy_iterations` controls the greedy loop when `enable_greedy=true`.
- `wiggle_iterations` controls the tower-wiggle loop when `enable_wiggle=true`, with `WIGGLE_ITERATIONS=<n>` still available as a one-run override.
- `wiggle_parallel_workers` controls PostgreSQL parallel workers per heavy wiggle query, with `WIGGLE_PARALLEL_WORKERS=<n>` available for one remote resume run.
  When this is greater than 1, GNU parallel runs several wiggle workers that claim dirty towers with `FOR UPDATE SKIP LOCKED`; graph writes and component checks are still serialized by an advisory lock.
- `population_existing_anchor_weight` controls how strongly current towers pull KMeans clusters away from duplicate population anchors.
- `enable_route_segment_reroute` controls the post-contract local route-pair optimizer; `route_segment_reroute_candidate_limit` caps each endpoint candidate list and `route_segment_reroute_max_moves` bounds one pass.
- `enable_population_anchor_contract` controls the post-route cleanup that removes soft population anchors only when generated towers preserve cached non-population LOS neighbors and the deletion does not increase the live LOS component count; `population_anchor_contract_distance_m=0` makes that replacement search topology-only; positive values re-enable a distance window.
- `enable_generated_pair_contract` controls close route-like pair contraction, where one synthetic H3 must preserve the combined cached non-population LOS-neighbor set and must not increase the live LOS component count.
- `wiggle_candidate_limit` bounds cached marginal-population scoring after all local candidates have already passed the cached LOS-neighbor preservation check.

## Testing
To run the CI-style pipeline offline, launch `TEST_MODE=1 PYTHONPATH=. make -B -j all`.
To run the verification suite, run `make test` or a specific `db/test/<name>` target.
The test targets depend on many database artifacts, so missing markers will cause Make to rebuild prerequisites.
Some tests also execute long-running pipeline stages as part of their setup.
Run tests against a disposable database if you want to keep an existing pipeline state intact.

## Running
Make sure PostgreSQL accepts local socket connections under your current user so the inline `psql` calls succeed.
Set `PGDATABASE`, `PGUSER`, `PGHOST`, and `PGPORT` when the database is not reachable through local libpq defaults; the seed import target uses the same environment values for `ogr2ogr`.
Execute `make all` to refresh every artifact from downloads through routing.
Run `make db/procedure/mesh_run_greedy_full` after setting `enable_greedy=true` to execute greedy placement when you are ready.
The configured greedy wrapper runs with `statement_timeout=0` because LOS and post-placement visibility refresh work can legitimately exceed the default timeout.
`make db/procedure/mesh_placement_restart` also invokes the tower-wiggle wrapper, so setting `enable_wiggle=true` is enough for the safe replay path. Run `make db/procedure/mesh_tower_wiggle_current` when you want to apply wiggle to the current tower set without replaying route inputs.
Run `make data/out/install_priority_reviewed` to build the field handout in HTML and CSV from the current database state, verify predecessor links against `mesh_visibility_edges`, and fail on bridge/cut-node graph findings.
Most SQL files are idempotent rebuild steps, but many `tables/` files intentionally drop and recreate derived tables.
Inspect the file and run the matching Make target when one exists, especially around `mesh_towers`, `mesh_surface_h3_r8`, route graph tables, and cache-adjacent stages.
Any missing credentials or tooling gaps should be recorded in `docs/todo.md`.
