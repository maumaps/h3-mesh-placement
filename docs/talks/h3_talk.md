This is a rough talk outline and scratchpad.
For the implemented pipeline and its calculation details, see `docs/pipeline.md` and `docs/calculations.md`.

Hi! I'm Darafei, from PostGIS PSC, 
I also run consulting company Maumap, we make maps. AI-enabled decision making systems, web maps, UX research, data pipelines.

I want to show an example of how to work with h3

h3 is by Uber, postgres bindings are h3_pg, thanks Zacharias Knudsen for it, we also implemented stuff there.

I got another hobby, Meshtastic. LoRA based recievers and transmitters.
Let's imagine how to create a country coverage for it.
We know:
 - 7 hop limit
 - 70 km max distance of link
 - Can install nodes where roads are
 - Height is 28m above ground
 - We don't want nodes closer than ~5km of one another
 - Want to cover most population not yet covered

Create table with initial points.
Poti, SoNick, Jvari, Komzpa, Feria 2, Tbilisi hackerspace, Gudauri, Gyumri, Yerevan.

Indicators we'll consider:
 - initial points
 - limit to Georgia
 - roads
 - population
 - elevation 

Import data.
We have come to several agreements internally that help: if table has h3 hexagons of many resolutions, we postfix it with \_h3. If the resolution is known and fixed, we postfix it with \_h3_r8. This helps data management later.
Typical schema is h3 | value float [ | geom | resolution | indicator name ].
Picking resolution: r8 is "walking distance scale", r11 is "building-level modelling"

Points
- Import initial points geojson using ogr2ogr.
- Convert points to h3 counts.

Lines and polygons.
- Import osm using osmium.
- Convert roads into h3 cells.
- Convert country outline into coverage.
- Convert country convex hull into coverage - for visibility data

H3
- Import kontur population using ogr.
- Format table to have h3.

Raster
- Import gebco using raster2pgsql.
- Two ways to convert raster
- min/max values

Visibility function
- takes two h3 cells
- returns Fresnel clearance in meters, path loss in dB, plus the simple boolean visibility helper
- draws 3D line
- checks intersections
    - planar 
    - height
        - except start and end cell
        - if intermediate cell is higher than max(start and end) - invisible and adds diffraction loss
        - some simplifications can be done because h3 is convex
- checks distance < 70km
- optional: noise model - many people along the line will cause noise; proper wave calculation 


Create sql file that draws connectivity.
Check our model.
Poti sees Feria 2 and SoNick.
Komzpa sees Feria 2.
Tbilisi sees nobody.
Jvari visibility will be measured separately.


Create final calculation surface:
h3 | geom | ele | has_road | population | has_tower | clearance | path_loss | has_reception | is_in_boundaries | is_in_unfit_area | min_distance_to_closest_tower | can_place_tower | visible_uncovered_population | distance_to_closest_tower

fill:
 - h3 - all h3 indexes in the convex hull outline
 - geom - geometry outline --- generated column
 - elevation - from gebco for all existing
 - has_road - from osm
 - population - from kontur population
 - has_tower - from initial points data
 - distance_to_closest_tower = ST_Distance()
 - is_in_boundaries - intersects the Georgia administrative border polygon
 - is_in_unfit_area - intersects the polygons we manually marked as off-limits
 - min_distance_to_closest_tower = 5000 (meters) unless we override a cell explicitly
 - can_place_tower is generated: has_road and is_in_boundaries and not has_tower and not is_in_unfit_area and distance_to_closest_tower >= min_distance_to_closest_tower (LOS requirements stay in the placement code so the surface can visualize the whole search space)
 - placement logic still insists on `visible_tower_count >= 2` before picking candidates, so the greedy tables never install isolated towers even though the surface column remains true ahead of time
    

(create some indexes - brin for all columns?)

Now, greedy loop.
 - fill nulls:
     - clearance & path_loss = best result from `h3_visibility_metrics()` against towers within 70km where reception cache is missing
     - can_place_node = true where has_reception and has_road and can_place_node is null
     - visible_uncovered_population = (sum (population) where visibility(a,b) and distance_to_closest_tower < 70km) where can_place_node and visible_uncovered_population is null
 - connect clusters before hunting population:
     - call `mesh_tower_clusters()` to label current towers per connected component (using LOS edges)
     - for every eligible candidate cell compute the best path loss to each cluster (using `h3_visibility_metrics()` against every tower), treat clusters that already have LOS as zero-cost, require at least one LOS link overall, and pick the cell that minimizes the average path loss (ties go to the candidate closest to the remaining blocked clusters); this automatically drags the network toward the middle without preselecting a target pair
- pick best candidate that has max(visible_uncovered_population) > 0 when no cluster-bridging option is available
    - add it to new masts table with next id (bridge selections mark their `source` as `bridge`, population-driven ones stay `greedy`)
    - set has_tower = true (the generated `can_place_tower` flips off automatically once spacing and visibility caches refresh)
    - set clearance/path_loss = null in 70km
    - set visible_uncovered_population = null in 2x70km
    - set distance_to_closest_tower = distance() in 2x70km
 - vacuum
     - freeze
     - maintain brin index


Picture of result:
 - nodes with id numbers
 - ST_MakeLine (a, b) where visible(a,b)

~~~~~~~~~

Part 2: Dithering







----
- [ ] h3 talk
	- [ ] MapCSS
	- [ ] normalization
	- [ ] Isochrone
		- [ ] osrm http
	- [ ] antimeridian
	- [ ] dithering
		- [ ] main idea
		- [ ] percentage
		- [ ] constraints
	- [x] raster to h3
		- [x] intersection
		- [x] rasterize h3 cells
	- [ ] linear to h3
		- [ ] ST_DumpSegments(ST_Segmentize(geom, h3 length divided by 2)
	- [ ] tile zoom level to h3
	- [x] site selection
		- [x] greedy site selection
		- [ ] kmeans
	- [ ] hands on
		- [ ] solar panels
		- [ ] population dither
	- [ ] generate overviews
	- [x] picking resolutions
		- [x] resolution 8 - walking distance - 400m
		- [x] resolution 11 - 30m - block/building level modelling
	- [ ] vacuum
		- [ ] freeze bits
		- [ ] Subtopic 2
