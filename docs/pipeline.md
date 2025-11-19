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

## Database Layout
Everything lives directly in the default PostgreSQL namespace, so table names are globally unique: `osm_georgia`, `kontur_population`, `georgia_boundary`, `mesh_surface_h3_r8`, etc.
The final surface table exposes the columns requested in the talk: `h3`, `geom`, `ele`, `has_road`, `population`, `has_tower`, `has_reception`, `can_place_tower`, `visible_uncovered_population`, `distance_to_closest_tower`.
Indexes use BRIN where possible so we can vacuum and freeze quickly even as the dataset grows.

## SQL Execution Order
`tables/mesh_pipeline_settings.sql`, `tables/mesh_towers.sql`, and `tables/mesh_greedy_iterations.sql` install the core bookkeeping tables.
`tables/georgia_boundary.sql`, `tables/georgia_convex_hull.sql`, `tables/mesh_surface_domain_h3_r8.sql`, `tables/georgia_roads_geom.sql`, `tables/roads_h3_r8.sql`, `tables/population_h3_r8.sql`, and `scripts/raster_values_into_h3.sql` (for GEBCO) harmonize all imported layers into the H3 resolution 8 domain.
`functions/h3_los_between_cells.sql` defines the helper needed for line-of-sight checks on top of `h3_pg` primitives.
`tables/mesh_surface_h3_r8.sql` builds the final surface table, fills the requested indicator columns, and sets up constraints plus indexes.
`tables/mesh_visibility_edges_seed.sql` and `tables/mesh_visibility_edges_active.sql` materialize visibility layers so we can inspect the expected connectivity.
`procedures/mesh_run_greedy.sql` implements the loop from the talk: fill missing fields, find the best candidate, promote it to a tower, invalidate nearby caches, and repeat until no population remains uncovered.

## Running The Pipeline
Make sure PostgreSQL accepts local socket connections under your current user so the inline `psql` calls succeed.
Execute `make all` to refresh every artifact from downloads to greedy placement.
Each SQL file is idempotent, so you can re-run `psql -f tables/<file>` (or `functions/<file>`, `procedures/<file>`) for debugging without destroying prior work.
The TODO list in `docs/todo.md` tracks missing credentials or tooling gaps noticed during development.
