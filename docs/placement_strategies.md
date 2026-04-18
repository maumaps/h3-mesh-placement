# Placement Strategies
This note names each placement stage with a classical algorithm analogy and ties it to the Make targets.
Each sentence starts on a new line for easy diffs.
See `docs/calculations.md` for the per-stage calculation details and optimization notes.

## Strategy Glossary
- **Coarse backbone seeding (spacing-first anchors)**
  Groups every placeable fine-resolution cell under a coarser H3 parent and installs at most one `coarse` tower per parent, while skipping coarse parents that already contain any tower.
  This primes the network with sparse anchors before routing, prefers cells with buildings, and only then uses spacing/population/building-count tie-breakers within that rooftop-first ordering.
  Target `db/procedure/mesh_coarse_grid` in `procedures/mesh_coarse_grid.sql`.
- **LOS cache and routing graph (all-pairs prep)**
  First seeds `mesh_los_cache` from `data/in/install_priority_bootstrap.csv` via `mesh_route_bootstrap`, and only then precomputes line-of-sight metrics for every tower or placeable candidate within 80 km. Route heuristics still read longer invisible tower-to-tower gaps from `mesh_visibility_edges`, because those gaps define where intermediate routing towers should be explored next.
  Before `fill_mesh_los_cache_prepare`, `db/procedure/mesh_visibility_edges_route_priority_geom` routes the same invisible or over-hop visibility gaps through `mesh_route_graph_edges` with pgRouting and stores the resulting corridor line back into `mesh_visibility_edges.geom`, so cache warmup and backfill prioritize cells along the routed corridor instead of the straight tower chord.
  Builds the weighted pgRouting graph used by every downstream routing step.
  This is the all-pairs visibility preparation step that behaves like a shortest-path precompute over a visibility graph.
  Target `db/procedure/fill_mesh_los_cache` driven by `scripts/fill_mesh_los_cache_prepare.sql`, `scripts/fill_mesh_los_cache_batch.sql`, and `scripts/fill_mesh_los_cache_finalize.sql`.
  The normal pipeline commits one batch and moves on so route stages can start early.
  Manual target `db/procedure/fill_mesh_los_cache_backfill` drains more committed batches later when you want a denser route graph.
  Related table `mesh_route_graph_cache` lives in `tables/mesh_route_graph_cache.sql`.
- **Cluster bridge (Steiner-style gap closing)**
  Connects the closest disconnected tower clusters first by running Dijkstra over the cached graph and installing intermediate towers along the minimum path-loss corridor.
  This behaves like a nearest-gap Steiner heuristic: keep linking components that the current partial graph can realistically reach before spending time on extreme long-shot pairs.
  Target `db/procedure/mesh_route_bridge` in `procedures/mesh_route_bridge.sql`.
- **Cluster slim (hop-span tightening)**
  Replaces long intra-cluster LOS edges with routed corridors until every pair stays within the 7-hop budget.
  The procedure evaluates batches of over-limit pairs, routes them with pgRouting, and accepts the corridor that shortens the most other pairs, similar to k-shortest-path based spanner tightening.
  Each call processes one corridor so the Makefile loops it externally.
  Target `db/procedure/mesh_route_cluster_slim` in `procedures/mesh_route_cluster_slim.sql`.
- **Tower wiggle (hill-climbing / local refinement)**
  Runs a local search on coarse, bridge, route, and cluster-slim towers to nudge them toward cells with higher visible population while preserving LOS to their current neighbors.
  This is the hill-climbing cleanup pass that improves shared routes and seed anchors before greedy coverage placement resumes.
  Target `db/procedure/mesh_tower_wiggle` in `procedures/mesh_tower_wiggle.sql`.
  Detailed walkthrough lives in `docs/mesh_tower_wiggle.md`.
- **Greedy coverage (maximum-coverage heuristic with bridge preference)**
  Prioritizes candidates that unlock the most tower clusters using path-loss scores, then falls back to maximizing visible uncovered population.
  This mirrors the standard greedy set-cover / facility-location heuristic constrained by LOS and minimum-two-neighbor requirements.
  Target `db/procedure/mesh_run_greedy` in `procedures/mesh_run_greedy.sql` with prepare/finalize helpers alongside it.

## Naming Notes
- Use “fill LOS cache” for the all-pairs LOS precompute (`fill_mesh_los_cache`) and “graph cache” for the stored routing geometry (`mesh_route_graph_cache`) to avoid route_cache versus cache_route confusion.
- The routing steps follow the order above: coarse backbone seeding → cache and graph prep → cluster bridge → cluster slim → greedy coverage.
- `mesh_tower_wiggle` remains available as a separate optional post-route refinement pass when you explicitly want local tower recentering.
- Refer to the stages by the classical names in this file when discussing or filing issues to keep terminology consistent.
