
## Test isolation follow-up
`db/test/h3_los_between_cells` now shadows `mesh_los_cache` instead of truncating production cache, but the test currently fails one expected-visible fixture (`Poti -> SoNick`) with an empty temporary cache.
Do not restore production-cache truncation; fix the test by seeding the exact cached clearance rows it requires or by moving the fixture fully onto temporary terrain/metrics tables.


## Incident: LOS cache table dropped by unsafe temp test setup
A new SQL test fixture used unqualified destructive table setup for `mesh_los_cache`, `mesh_surface_h3_r8`, and `mesh_towers` before creating temporary tables.
That removed production tables in the local database and invalidated the already-filled LOS cache.
`AGENTS.md` now requires checking SQL fixtures for unqualified destructive statements, `scripts/backup_mesh_los_cache.sh` provides a guarded backup target that refuses empty cache backups, and `scripts/restore_mesh_los_cache.sh` preserves any existing cache by renaming it before restore.
The LOS-cache `drop`/`truncate` patterns were removed from SQL scripts/tests; old SQL tests still contain production `truncate` patterns for non-cache placement tables and should be migrated to isolated `pg_temp` fixtures before being run against valuable local state.

Backbone-first iteration is in progress.
The current live placement restart uses `tables/mesh_pipeline_settings.sql` with coarse and greedy disabled, population anchors enabled with heavy existing-tower KMeans anchors, and route bridge/cluster-slim enabled.
The live tower mix after the safe restart, population-anchor contraction, and current wiggle pass is `35 mqtt + 6 seed + 1 population + 26 route`; use `make db/procedure/mesh_placement_restart` to replay placement stages without rebuilding cached tables.
`make -B db/procedure/mesh_placement_restart` checked all 7 population and 26 route towers through the configured wiggle stage, preserved close generated towers unless a merge target kept the same cached visible-neighbor set, and `make -B db/procedure/mesh_route_refresh_visibility_current` refreshed current visibility diagnostics after the wiggle moves.
Keep using the canonical `existing_mesh_nodes.geojson` output from the curated + manual Liam Cottle merge as the only imported seed source while this iteration is being validated.

mesh_visibility_edges_refresh() takes ~25 minutes on the current dataset; needs optimization to avoid timeouts.
The routed fallback geometry refresh in visibility diagnostics is the expensive tail; keep it split from the core LOS/hop refresh so routing can resume from the faster core stage when needed.
LongFast animation uses cached LOS only; missing cache pairs will appear as no coverage and may need cache warming.
Mapnik installation steps differ by OS and should be tested on macOS and Ubuntu.
Meshtastic LongFast bandwidth differs between the radio settings and site planner tables; confirm which preset matches deployed hardware.
Animation airtime defaults assume a typical payload size; consider capturing real payload sizes for more accurate timing.
Confirm `population_h3_r8` is populated after the in-place repair step; if still empty, re-import `kontur_population` and re-run `db/table/population_h3_r8`.
PostgreSQL reports a collation version mismatch warning (`kom` database); refresh the collation version when convenient.
Mapnik logs an SVG parse error about width/height set to 100% during renders; identify the offending SVG source.
GDAL warns about libavif version mismatch during renders; verify runtime library versions to silence the warning if needed.
`ffmpeg` can fail if a render is interrupted and leaves partial PNGs; if this happens, re-run the renderer to completion before assembling MP4.
Tried `strace -f -e openat` on the render; no `.svg` files were opened directly, so the SVG parse error may come from a system SVG dependency rather than the repo.
The current dataset still needs a full end-to-end runtime check after the H3-keyed table rebuild and the new `mesh_coarse_grid` stage swap.
The installer handout exporter should be re-checked against the refreshed H3-keyed surface before the next field release.
The installer handout still triggers one MapLibre warning about a `null` numeric feature property in Chromium, even though the overlays render correctly after the standalone bootstrap fix.
`db/test/install_priority_py` uses a marker file that can stay up to date even when the underlying Python tests changed, so routine `make` runs may skip fresh test execution unless forced with `make -B`.
`mesh_visibility_edges_refresh()` currently skips 14 towers because their H3 cells have no GEBCO elevation sample; those towers stay in `mesh_towers`, but downstream visibility edges will be incomplete until DEM coverage or a deliberate exclusion rule is added.
`fill_mesh_los_cache` now resumes safely in committed 250k-pair batches, and the main pipeline intentionally uses only one batch before routing.
Use `db/procedure/fill_mesh_los_cache_backfill` later when you want to spend more time thickening the route graph from the cached queue.

- mesh_route_cluster_slim progress notices print malformed `0.0` fragments in psql output; keep the iteration logging but fix the formatting in a later pass.
`db/test/mesh_population` now runs as a direct sparse-anchor smoke test and no longer replays placement in dry-run.
Population-anchor stars are now handled by the applied `mesh_population_anchor_contract` stage: population anchors are soft terminals, while close route-only pairs remain preserved unless cached LOS-neighbor sets prove they are redundant. Use `make db/procedure/mesh_population_anchor_contract_current` to re-run only that cleanup on current towers without replaying route stages.
