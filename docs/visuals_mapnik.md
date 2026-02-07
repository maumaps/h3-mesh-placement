# Mapnik Visuals and LongFast Animation
This document explains how to render static maps and the LongFast animation.
Each sentence starts on a new line for clean diffs.

## Guarantees
The rendering scripts are read‑only and do not recompute LOS caches.
They only query `mesh_los_cache`, `mesh_towers`, and related tables.
This keeps expensive cached visibility data intact.
The scripts open read‑only transactions to protect the cache.
Treat `mesh_los_cache` as precious and avoid dropping or rebuilding it.
The animation schedules transmissions using airtime plus small randomized jitter.
Only one new transmitter begins per frame.
Additional towers can transmit concurrently if they do not hear any on‑air transmission.
Missing cache pairs will appear as gaps in reception.
The airtime calculation uses the standard LoRa airtime formula.
Cells are marked as received after a transmission finishes.
Population counters are cumulative and count each cell once.

## Dependencies
- Mapnik with the PostGIS datasource enabled.
- Python packages: `mapnik`, `psycopg2`, and `pillow` for legend overlays.
- `ffmpeg` for MP4 assembly and GIF export.
- ImageMagick `convert` when rasterizing SVG wordmarks.

## LongFast defaults references
- Meshtastic radio settings preset table lists LongFast as SF11, CR 4/5, BW 250 kHz.
- `https://meshtastic.org/docs/overview/radio-settings/`
- Meshtastic site planner table lists LongFast with BW 125 kHz and SF11, CR 4/5.
- `https://meshtastic.org/docs/software/site-planner/`
- Use the values that match your hardware and adjust script flags accordingly.

## Static map rendering
Run a static render with:
`python scripts/render_mapnik.py --output data/out/visuals/mesh_surface.png`
The default style file is `mapnik/styles/mesh_style.xml`.
The map renders in UTM zone 38N for accurate local proportions across Georgia + Armenia.
Adjust colors and opacity in the style file to match your brand palette.
The default style defines multiple signal tiers for receive and transmit layers.
Static renders focus on population density and tower locations.
Route lines are intentionally omitted to keep the map legible.
Use `PGHOST`, `PGDATABASE`, `PGUSER`, and `PGPASSWORD` to configure the DB connection.
Set `PGDATABASE` to the pipeline database before running renders.
The static render buckets population by quantiles for balanced color ramps.

## LongFast animation rendering
Run the animation frame generator with:
`python scripts/render_longfast_animation.py --output-dir data/out/visuals/longfast`
The default frame rate is 24 fps and the default size is 1920x1080.
Frames are written as `frame_0000.png`, `frame_0001.png`, and so on.
The `data/out/` directory is git‑ignored for large outputs.
Frames include a bottom-left label panel rendered with Pillow to keep text readable against dense map features.
Frame 0 is intentionally idle so the transmission starts after the first tick.
The renderer reuses the last map render when the visual state is unchanged to speed up long sequences.
For quick legend previews, render a short sequence with `--max-frames`.
Use `--wordmark-path` and `--wordmark-height` to change the wordmark overlay.
Pass `--no-video` to skip MP4 assembly.
Greedy placement is disabled by default for visuals.
Run `make db/procedure/mesh_run_greedy_full` if you want greedy towers included.
Active transmitters render with a warm glow so the broadcast origin is obvious.
Pending handoffs render subtle links from on‑air transmitters to towers that have heard but not yet transmitted.
Past links remain visible with lower opacity so the propagation trail stays on screen.
The legend includes node role colors plus the glow meaning.
Signal tiers use an “illumination” palette (soft yellow → orange) for the coverage fills.
The renderer interpolates extra tier stops for smoother gradients while keeping a compact legend.
The wordmark renders in the bottom-right corner of every frame.

Assemble into a GIF with:
`ffmpeg -framerate 24 -i data/out/visuals/longfast/frame_%04d.png -vf "scale=1920:-1:flags=lanczos" -loop 0 data/out/visuals/longfast.gif`

Assemble into an MP4 with:
`ffmpeg -framerate 24 -i data/out/visuals/longfast/frame_%04d.png -vf "scale=1920:-1:flags=lanczos,format=yuv420p" data/out/visuals/longfast.mp4`

## Configuration knobs
- `--seed-name` selects the initial tower by name from `mesh_initial_nodes_h3_r8`.
- The default seed name is `Komzpa`.
- `--hop-limit` caps propagation depth (default 12).
- `--bandwidth-khz`, `--spreading-factor`, `--coding-rate`, and `--payload-bytes` control airtime.
- The default payload size is 160 bytes and can be overridden per run.
- Use `--bandwidth-khz 125` if your LongFast preset matches the site planner table.
- `--coding-rate` expects the denominator of the 4/x ratio, for example `5` for 4/5.
- `--tier-thresholds-db` lets you set custom path loss tiers.
- Provide six thresholds in ascending order to control the base tier anchors.
- Lower path loss values are treated as stronger signal tiers.
- The hop breakdown in the label uses the first hop a cell was reached.
- If no thresholds are provided, the script derives quantile-based tiers from cached path loss values.
- Use `--max-frames` to render a preview subset without changing any caches.

## Useful SQL snippets
The scripts run read‑only queries similar to the ones below.
These are included so you can debug without touching caches.

```sql
-- Fetch seed tower by name.
select h3, name
from mesh_initial_nodes_h3_r8
where name ilike '%Komzpa%';

-- Load tower adjacency using cached LOS.
select
    src_h3,
    dst_h3,
    path_loss_db
from mesh_los_cache
where clearance > 0
  and distance_m <= 70000
  and mast_height_src = 28
  and mast_height_dst = 28
  and frequency_hz = 868000000;

-- Load visible cells for a tower using cached LOS.
select
    src_h3 as tower_h3,
    dst_h3 as cell_h3,
    path_loss_db
from mesh_los_cache
where src_h3 = :tower_h3
  and clearance > 0
  and distance_m <= 70000
  and mast_height_src = 28
  and mast_height_dst = 28
  and frequency_hz = 868000000;
```
