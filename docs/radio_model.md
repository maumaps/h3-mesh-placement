# Radio Model Notes (LongFast‑inspired)
This note explains the simplified radio model used in the pipeline.
Each sentence starts on a new line for clean diffs.

## What we model
We model a single link between two H3 cells at a time.
We compute a Fresnel clearance value along the H3 grid path.
We convert that clearance into a path loss score for routing.

## Effective Earth radius
The LOS calculation applies a 4/3 Earth radius to model atmospheric refraction.
This bends the ray slightly and improves long‑range clearance realism.
See `functions/h3_visibility_clearance.sql` for the exact formula and constants.

## Fresnel clearance and visibility
A link is considered visible when the minimum Fresnel clearance is positive.
We treat negative clearance as diffraction and apply a penalty in path loss.
The boolean helper lives in `functions/h3_los_between_cells.sql`.

## Path loss model
We compute free‑space path loss and add a single knife‑edge diffraction penalty.
This is intentionally simple to keep the cache and routing steps tractable.
The model is implemented in `functions/h3_path_loss.sql`.
The LongFast animation uses this model through cached values only.
Keep `mesh_los_cache` intact to avoid multi‑day recomputation.

## LongFast defaults
The demo uses a LongFast‑style preset as a planning default.
The Meshtastic radio settings page lists LongFast as SF11, CR 4/5, and 250 kHz bandwidth.
The Meshtastic site planner table lists LongFast with 125 kHz bandwidth and SF11, CR 4/5.
Use the preset your hardware actually runs, and override parameters in the render script if needed.
References are in `docs/visuals_mapnik.md` along with the animation configuration.
The planning defaults for LOS are 28 m mast height and 868 MHz frequency.
The hard distance cut is 70 km to match the Meshtastic planning radius.
