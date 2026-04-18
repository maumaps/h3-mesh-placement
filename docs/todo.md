Backbone-first iteration is in progress.
The H3-keyed tables were rebuilt, but the refreshed placement artifacts still need a clean run through `mesh_coarse_grid` and downstream routing stages on the repaired dataset.
Keep using the canonical `existing_mesh_nodes.geojson` output from the curated + manual Liam Cottle merge as the only imported seed source while this iteration is being validated.
The current live restart is now past coarse seeding with `43 mqtt + 6 seed + 62 coarse`; continue from this exact baseline into refreshed LOS cache fill and routed placement instead of restarting earlier stages.

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
`mesh_visibility_edges_refresh()` currently skips 21 towers because their H3 cells have no GEBCO elevation sample; those towers stay in `mesh_towers`, but downstream visibility edges will be incomplete until DEM coverage or a deliberate exclusion rule is added.
`fill_mesh_los_cache` now resumes safely in committed 250k-pair batches, and the main pipeline intentionally uses only one batch before routing.
Use `db/procedure/fill_mesh_los_cache_backfill` later when you want to spend more time thickening the route graph from the cached queue.

- mesh_route_cluster_slim progress notices print malformed `0.0` fragments in psql output; keep the iteration logging but fix the formatting in a later pass.
