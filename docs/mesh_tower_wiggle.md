# Tower Wiggle Pass
Population, route, bridge, and cluster-slim towers get a k-means-style refinement before the greedy placement loop runs.
The pass keeps their existing line-of-sight partners intact while nudging each tower toward population it can add without duplicating nearby towers' cached coverage.

`mesh_tower_wiggle(reset_run boolean default false)` processes exactly one tower per call and logs its decisions with `NOTICE`.
Make drives the loop: the first iteration uses `reset_run=true` to seed the dirty queue, every subsequent call omits the reset flag until the function returns `0` (no dirty towers remain).

Towers with `source` in `population`, `route`, `cluster_slim`, or `bridge` sit in `mesh_tower_wiggle_queue` as “dirty”.
The selector picks towers with the smallest `recalculation_count` first and breaks ties by the largest stored `visible_population` (falling back to the cell population).

Each call:
- Collects every tower that currently has direct LOS within 80 km.
- Searches road-served, in-bounds cells within 80 km that keep LOS to all of those neighbors and stay outside the tower spacing guard (default 5 km from other towers).
- Scores LOS-safe candidates by cached marginal population first, then falls back to stored visible/nearby population so adjacent route relays do not chase the same settlement.
- Moves the tower to the best-scoring cell, increments its `recalculation_count`, and logs both the pick and the chosen target (or logs that the tower stayed put).

Moves trigger recalculations around both the old and new cells: `has_tower` flags flip, nearest-tower distances refresh for the affected region, LOS counts and RF metrics are recomputed, and the promoted cell gets its `visible_population` refilled.
Any bridge/cluster-slim towers that become direct LOS neighbors of the new position are marked dirty so they get reconsidered in the next pass.
The stage refreshes `mesh_visibility_edges` whenever a move succeeds so downstream diagnostics reflect the latest placements.

## Optimization Notes
The function processes one tower per call so Make can loop it as a sequence of small transactions with readable logs.
Candidate searches are bounded by `ST_DWithin(..., 80000)` and by road/boundary/unfit gates so the planner can use GiST indexes on `centroid_geog`.
Recalculations are localized to the old/new tower neighborhoods, which avoids recomputing RF metrics for the entire country after every small move.

`wiggle_candidate_limit` limits how many LOS-safe candidates receive cached marginal-population scoring in each tower evaluation.
The candidate must preserve LOS to the tower's current visible neighbors before it can enter that scoring shortlist, so the cap cannot hide valid mountain relay cells behind invalid high-population city cells.
Wiggle reads `mesh_los_cache` for neighbor preservation and cached marginal coverage, then falls back to `mesh_surface_h3_r8.visible_population`, `population_70km`, or local `population`; it must not start fresh terrain LOS or all-population summation inside the interactive refinement loop.

`generated_tower_merge_distance_m` is only a search radius for possible generated-tower merges. Wiggle deletes a nearby route/cluster-slim/bridge/coarse tower only when the merge target has cached LOS to every visible neighbor that the deleted tower had, so close cells with different LOS roles are preserved.
