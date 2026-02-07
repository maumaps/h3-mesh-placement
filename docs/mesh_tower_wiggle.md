# Tower Wiggle Pass
Population, route, bridge, and cluster-slim towers get a k-means-style refinement before the greedy placement loop runs.
The pass keeps their existing line-of-sight partners intact while nudging each tower toward the most valuable visible population.

`mesh_tower_wiggle(reset_run boolean default false)` processes exactly one tower per call and logs its decisions with `NOTICE`.
Make drives the loop: the first iteration uses `reset_run=true` to seed the dirty queue, every subsequent call omits the reset flag until the function returns `0` (no dirty towers remain).

Towers with `source` in `route`, `cluster_slim`, `bridge`, or `population` sit in `mesh_tower_wiggle_queue` as “dirty”.
The selector picks towers with the smallest `recalculation_count` first and breaks ties by the largest stored `visible_population` (falling back to the cell population).

Each call:
- Collects every tower that currently has direct LOS within 70 km.
- Searches road-served, in-bounds cells within 70 km that keep LOS to all of those neighbors and stay outside the tower spacing guard (default 5 km from other towers).
- Scores candidates by the visible population around them, preferring the existing cell when scores tie so jitter stays intentional.
- Moves the tower to the best-scoring cell, increments its `recalculation_count`, and logs both the pick and the chosen target (or logs that the tower stayed put).

Moves trigger recalculations around both the old and new cells: `has_tower` flags flip, nearest-tower distances refresh for the affected region, LOS counts and RF metrics are recomputed, and the promoted cell gets its `visible_population` refilled.
Any bridge/cluster-slim towers that become direct LOS neighbors of the new position are marked dirty so they get reconsidered in the next pass.
The stage refreshes `mesh_visibility_edges` whenever a move succeeds so downstream diagnostics reflect the latest placements.

## Optimization Notes
The function processes one tower per call so Make can loop it as a sequence of small transactions with readable logs.
Candidate searches are bounded by `ST_DWithin(..., 70000)` and by road/boundary/unfit gates so the planner can use GiST indexes on `centroid_geog`.
Recalculations are localized to the old/new tower neighborhoods, which avoids recomputing RF metrics for the entire country after every small move.
