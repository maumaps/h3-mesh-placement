# PostGIS Primer
This primer explains the PostGIS patterns used in this repository.
Each sentence starts on a new line for clean diffs.

## Spatial data patterns
We store H3 cells as `h3index` and derive geometry on demand.
We store centroids as geography for distance and KNN queries.
We use GiST for spatial filtering and BRIN to keep scans fast.

## Raster and vector handling
Raster elevation is imported once and sampled into H3 cells.
Vector roads and boundaries are filtered and then projected to H3.
This keeps all downstream computation in H3‑indexed tables.

## Routing patterns
We build a pgRouting graph from LOS‑visible H3 edges.
We cache routing geometry so diagnostics are fast and repeatable.
We keep routing steps idempotent to support iterative debugging.

## Performance notes
Cache and reuse expensive LOS computations whenever possible.
Localize recomputations after each tower placement.
Favor KNN and indexed distance operations over full scans.
