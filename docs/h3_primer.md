# H3 Primer
This primer explains the H3 concepts used in this repository.
Each sentence starts on a new line for clean diffs.

## What H3 gives us
H3 turns geography into a regular hex grid with stable indexing.
That lets us store coverage, population, and visibility data in simple tables.
It also makes routing and neighborhood queries consistent across the pipeline.

## Why resolution 8
Resolution 8 is a walking‑distance scale that balances detail and compute cost.
It is fine-grained enough to allow local tower movement during the wiggle stage.
It is coarse enough to keep country‑scale tables and caches manageable.

## How we use H3 here
We convert roads, population, and elevation into H3 layers at resolution 8.
We build a convex hull and tessellate it into the H3 domain.
We store LOS metrics between H3 cells in a reusable cache.
We place towers on H3 cells and keep all metrics in H3‑keyed tables.

## Practical tips
Always keep H3 resolution explicit in table names.
Always generate geometry from H3 rather than storing raw polygon shapes.
Prefer H3‑native functions for distance, boundary, and path operations.
