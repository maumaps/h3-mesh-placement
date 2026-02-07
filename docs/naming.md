# Naming and Terminology
This file clarifies naming conventions and avoids ambiguous terms.
Each sentence starts on a new line for clean diffs.

## Canonical terms
- Use **fill LOS cache** for the LOS precompute stage (`fill_mesh_los_cache`).
- Use **graph cache** for the stored routed geometry (`mesh_route_graph_cache`).
- Use **tower wiggle** for the local improvement pass (`mesh_tower_wiggle`).
- Use **surface** for the main planning table (`mesh_surface_h3_r8`).

## H3 naming
- Use `_h3` when multiple resolutions are stored in one table.
- Use `_h3_r8` when resolution 8 is fixed and explicit.

## Radio model
- Use **clearance** for the Fresnel clearance value in meters.
- Use **path loss** for the dB value used for routing weights.
- Use **LongFast preset** to refer to the planning defaults for SF, BW, and CR.
