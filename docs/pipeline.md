# H3 Mesh Placement Pipeline
This repository codifies the Meshtastic coverage story from `docs/talks/h3_talk.md` into a reproducible PostGIS pipeline.
The pipeline runs inside PostgreSQL using PostGIS, `h3_pg`, and pgRouting.
`osmium`, `ogr2ogr`, and `raster2pgsql` are used only for importing raw datasets into PostGIS.
See `docs/calculations.md` for a step-by-step description of every calculation and optimization.
See `docs/placement_strategies.md` for the terminology used for each placement stage.
See `docs/radio_model.md` for the radio model assumptions and LongFast defaults.
See `docs/visuals_mapnik.md` for rendering and animation instructions.

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

## Pipeline Targets
`make all` runs the workflow from downloads through routing with greedy placement disabled by default.
Use `make db/procedure/mesh_run_greedy_full` when you want to execute the greedy placement loop.
Intermediate targets such as `db/table/osm_for_mesh_placement` or `db/table/mesh_surface_h3_r8` can be executed independently to debug single steps.

The key stage order is:
- Import sources → derive boundary/roads/population/elevation layers → build `mesh_surface_h3_r8`.
- Seed towers (`mesh_initial_nodes_h3_r8` → `mesh_towers`).
- Optional population anchors (`mesh_population`).
- Cache LOS metrics and build routing graph (`fill_mesh_los_cache`).
- Connect clusters (`mesh_route_bridge`) and tighten hop counts (`mesh_route_cluster_slim`).
- Locally refine routed towers (`mesh_tower_wiggle`).
- Run the greedy placement loop (`mesh_run_greedy_prepare` + iterative `mesh_run_greedy` + `mesh_run_greedy_finalize`) via `mesh_run_greedy_full`.

The hard constants are:
- H3 resolution 8.
- 70 km maximum LOS distance.
- 5 km minimum tower separation.
See `docs/calculations.md` for where they appear in SQL and why they are shared.

Optional debug-only iteration caps:
- `SLIM_ITERATIONS=<n>` limits the Makefile loop for cluster slimming.
- `WIGGLE_ITERATIONS=<n>` limits the Makefile loop for tower wiggle.

## Testing
To run the CI-style pipeline offline, launch `TEST_MODE=1 PYTHONPATH=. make -B -j all`.
To run the verification suite, run `make test` or a specific `db/test/<name>` target.
The test targets depend on many database artifacts, so missing markers will cause Make to rebuild prerequisites.
Some tests also execute long-running pipeline stages as part of their setup.
Run tests against a disposable database if you want to keep an existing pipeline state intact.

## Running
Make sure PostgreSQL accepts local socket connections under your current user so the inline `psql` calls succeed.
Execute `make all` to refresh every artifact from downloads through routing.
Run `make db/procedure/mesh_run_greedy_full` to execute greedy placement when you are ready.
Each SQL file is idempotent, so you can re-run `psql -f tables/<file>` (or `functions/<file>`, `procedures/<file>`) for debugging without destroying prior work.
Any missing credentials or tooling gaps should be recorded in `docs/todo.md`.
