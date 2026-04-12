Population clustering (`call mesh_population()`) timed out twice (5 min and 15 min) on the current dataset; no `population` towers were inserted.
Needs investigation/optimization before running end-to-end.
mesh_visibility_edges_refresh() takes ~25 minutes on the current dataset; needs optimization to avoid timeouts.
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
The current live `kom` database schema has drift from the checked-in SQL for `mesh_towers` and `mesh_surface_h3_r8`.
The installer handout exporter now works around that drift, but the database should still be refreshed and aligned with the repository definition when convenient.
The handout's `impact_people_est` currently relies on nearby populated OSM localities because the live `mesh_surface_h3_r8` table no longer exposes the geometry/H3 key needed to attach true stored `visible_population` to towers.
The live DB also lacks the deployed H3 visibility helper and keyed LOS cache columns the checked-in SQL expects, so the installer handout currently uses route-derived corridor preference as the RF-loss proxy when ranking cluster connectors.
The installer handout still triggers one MapLibre warning about a `null` numeric feature property in Chromium, even though the overlays render correctly after the standalone bootstrap fix.
`db/test/install_priority_py` uses a marker file that can stay up to date even when the underlying Python tests changed, so routine `make` runs may skip fresh test execution unless forced with `make -B`.
