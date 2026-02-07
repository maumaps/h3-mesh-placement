# H3 Mesh Placement (PostGIS + Radio Model)
This repository is a showcase of H3 + PostGIS + radio propagation modeling for mesh network planning.
It is also a teaching resource with step-by-step notes, reproducible SQL, and visual outputs you can share.

## What you can learn here
- How to build an H3-backed planning surface with PostGIS.
- How to model line of sight, Fresnel clearance, and path loss in SQL.
- How to seed, route, and greedily place towers to maximize population coverage.
- How to visualize radio coverage and propagation with Mapnik.

## Quick start
- Install PostGIS, pgRouting, and the `h3` extension.
- Review `docs/pipeline.md` for data sources and pipeline stages.
- Run the full pipeline with `make all`.
- Run the verification suite with `make test`.

## Where to start reading
- `docs/index.md` is the navigation hub.
- `docs/calculations.md` explains every calculation and optimization.
- `docs/placement_strategies.md` names each stage with classical algorithm analogies.
- `docs/radio_model.md` walks through the LongFast-inspired radio model assumptions.
- `docs/posts/` contains LinkedIn-ready drafts.

## Visuals
- `docs/visuals_mapnik.md` explains how to render static maps and the LongFast animation.
- The rendering scripts are read-only and do not modify cached LOS data.
