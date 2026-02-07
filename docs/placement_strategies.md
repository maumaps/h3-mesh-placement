# Placement Strategies
This note names each placement stage with a classical algorithm analogy and ties it to the Make targets.
Each sentence starts on a new line for easy diffs.
See `docs/calculations.md` for the per-stage calculation details and optimization notes.

## Strategy Glossary
- **Population clustering (k-means seeds)**
  Groups every tower-eligible, tower-invisible hex into k-means clusters weighted by 70 km population sums with a 70 km radius cap and installs the top-population hex from each cluster as a population tower unless the cluster already contains a tower.
  This primes the network with population-driven anchors before any routing or greedy placement while respecting existing infrastructure and skipping cells already covered by current towers.
  Target `db/procedure/mesh_population` in `procedures/mesh_population.sql`.
- **LOS cache and routing graph (all-pairs prep)**
  Precomputes line-of-sight metrics for every tower or placeable candidate within 70 km and writes them into `mesh_los_cache`.
  Builds the weighted pgRouting graph used by every downstream routing step.
  This is the all-pairs visibility preparation step that behaves like a shortest-path precompute over a visibility graph.
  Target `db/procedure/fill_mesh_los_cache` in `procedures/fill_mesh_los_cache.sql`.
  Related table `mesh_route_graph_cache` lives in `tables/mesh_route_graph_cache.sql`.
- **Cluster bridge (Steiner-style gap closing)**
  Connects the most separated tower clusters first by running Dijkstra over the cached graph and installing intermediate towers along the minimum path-loss corridor.
  This mirrors a farthest-first Steiner tree heuristic: keep linking components until the forest becomes one connected cluster.
  Target `db/procedure/mesh_route_bridge` in `procedures/mesh_route_bridge.sql`.
- **Cluster slim (hop-span tightening)**
  Replaces long intra-cluster LOS edges with routed corridors until every pair stays within the 7-hop budget.
  The procedure evaluates batches of over-limit pairs, routes them with pgRouting, and accepts the corridor that shortens the most other pairs, similar to k-shortest-path based spanner tightening.
  Each call processes one corridor so the Makefile loops it externally.
  Target `db/procedure/mesh_route_cluster_slim` in `procedures/mesh_route_cluster_slim.sql`.
- **Tower wiggle (hill-climbing / k-means refinement)**
  Runs a local search on bridge and cluster-slim towers to nudge them toward cells with higher visible population while preserving LOS to their current neighbors.
  This is the hill-climbing cleanup pass that improves shared routes before population-driven placement resumes.
  Target `db/procedure/mesh_tower_wiggle` in `procedures/mesh_tower_wiggle.sql`.
  Detailed walkthrough lives in `docs/mesh_tower_wiggle.md`.
- **Greedy coverage (maximum-coverage heuristic with bridge preference)**
  Prioritizes candidates that unlock the most tower clusters using path-loss scores, then falls back to maximizing visible uncovered population.
  This mirrors the standard greedy set-cover / facility-location heuristic constrained by LOS, separation, and minimum-two-neighbor requirements.
  Target `db/procedure/mesh_run_greedy` in `procedures/mesh_run_greedy.sql` with prepare/finalize helpers alongside it.

## Naming Notes
- Use “fill LOS cache” for the all-pairs LOS precompute (`fill_mesh_los_cache`) and “graph cache” for the stored routing geometry (`mesh_route_graph_cache`) to avoid route_cache versus cache_route confusion.
- The routing steps follow the order above: population clustering → cache and graph prep → cluster bridge → cluster slim → tower wiggle → greedy coverage.
- Refer to the stages by the classical names in this file when discussing or filing issues to keep terminology consistent.
