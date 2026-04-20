# H3 Mesh Placement (PostGIS + Radio Model)
This repository is a showcase of H3 + PostGIS + radio propagation modeling for mesh network planning.
It is also a teaching resource with step-by-step notes, reproducible SQL, and visual outputs you can share.

## What you can learn here
- How to build an H3-backed planning surface with PostGIS.
- How to model line of sight, Fresnel clearance, and path loss in SQL.
- How to seed, route, and greedily place towers to maximize population coverage.
- How to visualize radio coverage and propagation with Mapnik.

## Quick start
- Install PostgreSQL with PostGIS, pgRouting, `h3`, and `h3_postgis`.
- Install command-line import/render tools: `osmium`, GDAL/OGR (`ogr2ogr`, `raster2pgsql`), Mapnik, GNU parallel, and ffmpeg.
- Install Python packages used by scripts and tests, including `psycopg2`, `Pillow`, and Mapnik Python bindings.
- Set `PGDATABASE`, `PGUSER`, `PGHOST`, and `PGPORT` when local libpq defaults do not point at the pipeline database.
- Review `docs/pipeline.md` for data sources, safety notes, and pipeline stages.
- Run the full pipeline with `make all`.
- Run the non-destructive verification suite with `make test`; live artifact checks expect the pipeline tables to already exist.

## Where to start reading
- `docs/index.md` is the navigation hub.
- `docs/calculations.md` explains every calculation and optimization.
- `docs/placement_strategies.md` names each stage with classical algorithm analogies.
- `docs/radio_model.md` walks through the LongFast-inspired radio model assumptions.

## Visuals
- `docs/visuals_mapnik.md` explains how to render static maps and the LongFast animation.
- The rendering scripts are read-only and do not modify cached LOS data.
