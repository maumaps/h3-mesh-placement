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
