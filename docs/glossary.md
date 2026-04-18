# Glossary
Each sentence starts on a new line for clean diffs.

## H3 and geometry
- **H3 index** is a hex cell identifier from the H3 grid.
- **Resolution 8** is the chosen planning scale for candidate towers and coverage.
- **Cell boundary** is the polygon outline of an H3 cell.
- **Centroid** is the center point used for distance and KNN queries.

## Radio model
- **Line of sight (LOS)** means the link has positive Fresnel clearance.
- **Fresnel clearance** is the minimum distance between terrain and the first Fresnel zone along the link.
- **Path loss** is the modeled attenuation in dB for a link, including diffraction.
- **Effective Earth radius** applies a k‑factor that bends the ray and changes clearance.
- **LongFast** is a Meshtastic radio preset used as a planning default.

## Pipeline stages
- **Coarse backbone seeding** adds one spacing-first anchor per eligible coarse H3 parent before routing.
- **Seed refresh** is the manual Liam Cottle snapshot merge that rebuilds the canonical seed GeoJSON before planning imports.
- **Cache graph** is the all-pairs LOS precompute step.
- **Graph cache** stores routing linework for re-use.
- **Cluster bridge** connects farthest tower components via the routing graph.
- **Cluster slim** reduces hop counts within connected clusters.
- **Tower wiggle** is the local refinement pass before greedy placement.
- **Greedy placement** selects towers that maximize uncovered population.
- **Building-first ordering** means cells with `has_building = true` rank ahead of bare cells before stage-specific metrics break ties.
