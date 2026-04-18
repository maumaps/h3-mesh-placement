all: db/procedure/mesh_run_greedy ## [FINAL] Build pipeline through routing (greedy disabled)

test: db/test/seed_nodes_py db/test/pipeline_regressions_py db/test/georgia_roads_geom db/test/georgia_unfit_areas db/test/population_h3_r8 db/test/mesh_surface_building_fields db/test/h3_los_between_cells db/test/mesh_surface_visible_towers db/test/mesh_surface_refresh_reception_metrics db/test/mesh_surface_refresh_visible_tower_counts db/test/mesh_visibility_edges db/test/mesh_visibility_edges_type db/test/mesh_visibility_invisible_route_geom db/test/mesh_run_greedy_prepare db/test/fill_mesh_los_cache_priority db/test/mesh_route_bootstrap_pairs db/test/mesh_route_corridor_between_towers db/test/mesh_route db/test/mesh_route_cluster_slim db/test/mesh_coarse_grid db/test/mesh_tower_wiggle db/test/install_priority_py ## [FINAL] Run verification suite

clean: ## [FINAL] Remove intermediate data and build markers
	@if [ -n "$(filter clean,$(MAKECMDGOALS))" ]; then rm -rf data/mid data/out db; fi

data: ## Ensure data staging directory exists
	mkdir -p data

data/in: | data ## Ensure raw data input directory exists
	mkdir -p data/in

data/in/osm: | data/in ## Ensure raw OSM input directory exists
	mkdir -p data/in/osm

data/in/population: | data/in ## Ensure raw population input directory exists
	mkdir -p data/in/population

data/in/meshtastic: | data/in ## Ensure raw Meshtastic input directory exists
	mkdir -p data/in/meshtastic

data/in/gebco: | data/in ## Ensure raw GEBCO input directory exists
	mkdir -p data/in/gebco

data/mid: | data ## Ensure intermediate data directory exists
	mkdir -p data/mid

data/mid/population: | data/mid ## Ensure intermediate population directory exists
	mkdir -p data/mid/population

data/mid/osm: | data/mid ## Ensure intermediate OSM directory exists
	mkdir -p data/mid/osm

data/mid/gebco: | data/mid ## Ensure intermediate GEBCO directory exists
	mkdir -p data/mid/gebco

data/out: | data ## Ensure output directory exists
	mkdir -p data/out

db: ## Ensure database artifacts directory exists
	mkdir -p db

db/raw: | db ## Ensure raw database marker directory exists
	mkdir -p db/raw

db/table: | db ## Ensure table marker directory exists
	mkdir -p db/table

db/table/h3_visibility_metrics: tables/h3_visibility_metrics.sql | db/table ## Install composite LOS metric type for scalar batch helpers
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/h3_visibility_metrics.sql
	touch db/table/h3_visibility_metrics

db/function: | db ## Ensure function marker directory exists
	mkdir -p db/function

db/procedure: | db ## Ensure procedure marker directory exists
	mkdir -p db/procedure

db/test: | db ## Ensure test marker directory exists
	mkdir -p db/test

data/in/osm/georgia-latest.osm.pbf: | data/in/osm ## Download Georgia OSM extract
	curl -L --retry 3 --continue-at - -o data/in/osm/georgia-latest.osm.pbf https://download.geofabrik.de/europe/georgia-latest.osm.pbf

data/in/osm/armenia-latest.osm.pbf: | data/in/osm ## Download Armenia OSM extract
	curl -L --retry 3 --continue-at - -o data/in/osm/armenia-latest.osm.pbf https://download.geofabrik.de/asia/armenia-latest.osm.pbf

data/mid/osm/osm_for_mesh_placement.osm.pbf: data/in/osm/georgia-latest.osm.pbf data/in/osm/armenia-latest.osm.pbf | data/mid/osm ## Merge Georgia and Armenia OSM extracts for mesh placement
	osmium merge data/in/osm/georgia-latest.osm.pbf data/in/osm/armenia-latest.osm.pbf -o data/mid/osm/osm_for_mesh_placement.osm.pbf -f pbf

data/in/population/kontur_population_20231101.gpkg.gz: | data/in/population ## Download Kontur population archive
	rm -f data/in/population/kontur_population_20231101.gpkg.gz
	curl -L --retry 3 --continue-at - -o data/in/population/kontur_population_20231101.gpkg.gz https://geodata-eu-central-1-kontur-public.s3.eu-central-1.amazonaws.com/kontur_datasets/kontur_population_20231101.gpkg.gz

data/mid/population/kontur_population_20231101.gpkg: data/in/population/kontur_population_20231101.gpkg.gz | data/mid/population ## Decompress Kontur population geopackage
	rm -f data/mid/population/kontur_population_20231101.gpkg
	gunzip -c data/in/population/kontur_population_20231101.gpkg.gz > data/mid/population/kontur_population_20231101.gpkg

data/in/gebco/gebco_2024_geotiff.zip: | data/in/gebco ## Download GEBCO 2024 GeoTIFF archive
	curl -L --retry 3 --continue-at - -o data/in/gebco/gebco_2024_geotiff.zip https://www.bodc.ac.uk/data/open_download/gebco/gebco_2024/geotiff/

data/mid/gebco/gebco_2024_geotiffs_unzip: data/in/gebco/gebco_2024_geotiff.zip | data/mid/gebco ## Unpack GEBCO rasters
	rm -f data/mid/gebco/*.tif
	unzip -o data/in/gebco/gebco_2024_geotiff.zip -d data/mid/gebco
	rm -f data/mid/gebco/*.pdf
	touch data/mid/gebco/gebco_2024_geotiffs_unzip

db/table/postgis_extension: | db/table ## Ensure required Postgres extensions exist
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "create extension if not exists postgis;"
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "create extension if not exists h3;"
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "create extension if not exists h3_postgis;"
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "create extension if not exists hstore;"
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "create extension if not exists pgrouting;"
	touch db/table/postgis_extension

db/table/mesh_pipeline_settings: tables/mesh_pipeline_settings.sql db/table/postgis_extension | db/table ## Store pipeline constants
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_pipeline_settings.sql
	touch db/table/mesh_pipeline_settings

db/table/mesh_towers: tables/mesh_towers.sql db/table/mesh_pipeline_settings | db/table ## Create mesh_towers registry
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_towers.sql
	touch db/table/mesh_towers

db/table/mesh_los_cache: tables/mesh_los_cache.sql db/table/postgis_extension | db/table ## Store cached LOS evaluations
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_los_cache.sql
	touch db/table/mesh_los_cache

db/table/mesh_route_bootstrap_pairs: tables/mesh_route_bootstrap_pairs.sql data/in/install_priority_bootstrap.csv data/in/install_priority_bootstrap_manual.csv db/table/mesh_surface_h3_r8 db/table/mesh_towers db/table/osm_for_mesh_placement db/table/georgia_boundary | db/table ## Load install-priority, current-tower, manual, and peak bootstrap LOS pairs
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_route_bootstrap_pairs.sql
	touch db/table/mesh_route_bootstrap_pairs

db/procedure/mesh_route_bootstrap: scripts/mesh_route_bootstrap.sql db/table/mesh_route_bootstrap_pairs db/table/mesh_los_cache db/function/h3_visibility_clearance | db/procedure ## Seed LOS cache from install-priority bootstrap pairs
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_route_bootstrap.sql
	touch db/procedure/mesh_route_bootstrap

db/table/mesh_greedy_iterations: tables/mesh_greedy_iterations.sql db/table/postgis_extension | db/table ## Create greedy iterations log table
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_greedy_iterations.sql
	touch db/table/mesh_greedy_iterations

data/in/existing_mesh_nodes.geojson: data/in/existing_mesh_nodes_curated.geojson scripts/merge_seed_nodes.py | data/in ## Build canonical seed GeoJSON from curated and optional Meshtastic inputs
	python scripts/merge_seed_nodes.py --curated-geojson data/in/existing_mesh_nodes_curated.geojson --raw-json data/in/meshtastic_liamcottle_nodes_region.json --output-geojson data/in/existing_mesh_nodes.geojson

data/in/meshtastic_liamcottle_nodes_region.json: | data/in/meshtastic ## Download raw Meshtastic node snapshot for manual seed refresh
	curl -L --retry 3 --output data/in/meshtastic_liamcottle_nodes_region.json https://meshtastic.liamcottle.net/api/v1/nodes

data/in/existing_mesh_nodes_refresh: data/in/meshtastic_liamcottle_nodes_region.json data/in/existing_mesh_nodes_curated.geojson scripts/merge_seed_nodes.py | data/in ## Refresh canonical seed GeoJSON using the latest Meshtastic snapshot
	python scripts/merge_seed_nodes.py --curated-geojson data/in/existing_mesh_nodes_curated.geojson --raw-json data/in/meshtastic_liamcottle_nodes_region.json --output-geojson data/in/existing_mesh_nodes.geojson
	touch data/in/existing_mesh_nodes_refresh

data/in/install_priority_bootstrap_refresh: data/out/install_priority.csv | data/in ## Refresh committed installer-priority bootstrap snapshot from latest export
	cp data/out/install_priority.csv data/in/install_priority_bootstrap.csv
	touch data/in/install_priority_bootstrap_refresh

db/raw/initial_nodes: data/in/existing_mesh_nodes.geojson db/table/mesh_towers | db/raw ## Import canonical seed tower locations
	ogr2ogr -f PostgreSQL "PG:dbname=kom user=kom host=/var/run/postgresql port=5432" data/in/existing_mesh_nodes.geojson -nln mesh_initial_nodes -nlt POINT -lco GEOMETRY_NAME=geom -overwrite -a_srs EPSG:4326
	@if psql --no-psqlrc --set=ON_ERROR_STOP=1 -Atc "select to_regclass('mesh_initial_nodes')" | grep -q mesh_initial_nodes; then \
		touch db/raw/initial_nodes; \
	else \
		echo "mesh_initial_nodes table missing after import; remove marker and retry"; \
		exit 1; \
	fi

db/table/osm_for_mesh_placement: data/mid/osm/osm_for_mesh_placement.osm.pbf db/table/postgis_extension | db/table ## Import merged Georgia + Armenia OSM extract
	@if psql --no-psqlrc --set=ON_ERROR_STOP=1 -Atc "select to_regclass('osm_for_mesh_placement')" | grep -q osm_for_mesh_placement; then \
		echo "osm_for_mesh_placement already exists, skipping import"; \
	else \
		osmium export -i sparse_mem_array -c osmium.config.json -f pg data/mid/osm/osm_for_mesh_placement.osm.pbf -v --progress | psql --no-psqlrc --set=ON_ERROR_STOP=1 -1 -c "create table osm_for_mesh_placement(geog geography, osm_type text, osm_id bigint, version int, osm_user text, ts timestamptz, way_nodes bigint[], tags jsonb); alter table osm_for_mesh_placement alter geog set storage external, alter osm_type set storage main, alter osm_user set storage main, alter way_nodes set storage external, alter tags set storage external, set (fillfactor=100); copy osm_for_mesh_placement from stdin freeze;"; \
	fi
	@if psql --no-psqlrc --set=ON_ERROR_STOP=1 -Atc "select to_regclass('osm_for_mesh_placement')" | grep -q osm_for_mesh_placement; then \
		touch db/table/osm_for_mesh_placement; \
	else \
		echo "osm_for_mesh_placement table missing after import; remove marker and retry"; \
		exit 1; \
	fi

db/raw/kontur_population: data/mid/population/kontur_population_20231101.gpkg db/table/postgis_extension | db/raw ## Import Kontur population
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c 'do $$body$$begin if to_regclass('"'kontur_population'"') is null then raise notice '"'kontur_population missing, importing'"'; end if; end$$body$$;'
	@if psql --no-psqlrc --set=ON_ERROR_STOP=1 -Atc "select to_regclass('kontur_population')" | grep -q kontur_population; then \
		echo "kontur_population already exists, skipping import"; \
	else \
		ogr2ogr -f PostgreSQL PG:\"\" data/mid/population/kontur_population_20231101.gpkg -nln kontur_population -nlt MULTIPOLYGON -lco GEOMETRY_NAME=geom -overwrite -t_srs EPSG:4326; \
	fi
	@if psql --no-psqlrc --set=ON_ERROR_STOP=1 -Atc "select to_regclass('kontur_population')" | grep -q kontur_population; then \
		touch db/raw/kontur_population; \
	else \
		echo "kontur_population table missing after import; remove marker and retry"; \
		exit 1; \
	fi

db/raw/gebco_elevation: data/mid/gebco/gebco_2024_geotiffs_unzip db/table/georgia_convex_hull db/table/postgis_extension | db/raw ## Import GEBCO raster tile covering Georgia + Armenia extent
	@if psql --no-psqlrc --set=ON_ERROR_STOP=1 -Atc "select to_regclass('gebco_elevation')" | grep -q gebco_elevation; then \
		echo "gebco_elevation already exists, skipping import"; \
	else \
		psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "drop table if exists gebco_elevation_all cascade;"; \
		raster2pgsql -I -C -M -s 4326 -t auto data/mid/gebco/gebco_2024_n90.0_s0.0_w0.0_e90.0.tif gebco_elevation_all | psql --no-psqlrc --set=ON_ERROR_STOP=1 -q; \
		psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "create table gebco_elevation as select * from gebco_elevation_all where ST_Intersects(rast, (select geom from georgia_convex_hull));"; \
		psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "drop table if exists gebco_elevation_all;"; \
	fi
	@if psql --no-psqlrc --set=ON_ERROR_STOP=1 -Atc "select to_regclass('gebco_elevation')" | grep -q gebco_elevation; then \
		touch db/raw/gebco_elevation; \
	else \
		echo "gebco_elevation table missing after import; remove marker and retry"; \
		exit 1; \
	fi

db/table/georgia_boundary: tables/georgia_boundary.sql db/table/osm_for_mesh_placement | db/table ## Build Georgia + Armenia boundary table
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/georgia_boundary.sql
	touch db/table/georgia_boundary

db/table/georgia_unfit_areas: tables/georgia_unfit_areas.sql db/table/osm_for_mesh_placement | db/table ## Build forbidden placement polygons
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/georgia_unfit_areas.sql
	touch db/table/georgia_unfit_areas

db/table/georgia_convex_hull: tables/georgia_convex_hull.sql db/table/georgia_boundary | db/table ## Build convex hull for Georgia
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/georgia_convex_hull.sql
	touch db/table/georgia_convex_hull

db/table/mesh_initial_nodes_h3_r8: tables/mesh_initial_nodes_h3_r8.sql db/raw/initial_nodes db/table/mesh_towers db/table/georgia_boundary | db/table ## Convert seed nodes to H3
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_initial_nodes_h3_r8.sql
	touch db/table/mesh_initial_nodes_h3_r8

db/table/mesh_surface_domain_h3_r8: tables/mesh_surface_domain_h3_r8.sql db/table/georgia_convex_hull | db/table ## Build convex hull H3 domain
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_surface_domain_h3_r8.sql
	touch db/table/mesh_surface_domain_h3_r8

db/table/georgia_roads_geom: tables/georgia_roads_geom.sql db/table/osm_for_mesh_placement | db/table ## Extract Georgia + Armenia roads geometries
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/georgia_roads_geom.sql
	touch db/table/georgia_roads_geom

db/table/roads_h3_r8: tables/roads_h3_r8.sql db/table/georgia_roads_geom | db/table ## Convert roads into H3 coverage
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/roads_h3_r8.sql
	touch db/table/roads_h3_r8

db/table/population_h3_r8: tables/population_h3_r8.sql db/table/georgia_boundary db/table/roads_h3_r8 | db/table ## Aggregate population into H3 without rebuild
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/population_h3_r8.sql
	touch db/table/population_h3_r8

db/table/buildings_h3_r8: tables/buildings_h3_r8.sql db/table/osm_for_mesh_placement | db/table ## Aggregate building counts into planning H3 cells
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/buildings_h3_r8.sql
	touch db/table/buildings_h3_r8

db/table/gebco_elevation_h3_r8: scripts/raster_values_into_h3.sql db/raw/gebco_elevation db/table/georgia_convex_hull | db/table ## Sample GEBCO values per H3
	psql --no-psqlrc --set=ON_ERROR_STOP=1 \
		-v table_name=gebco_elevation \
		-v table_name_h3=gebco_elevation_h3_r8 \
		-v item_name=ele \
		-v aggr_func=avg \
		-v resolution=8 \
		-v clip_table=georgia_convex_hull \
		-f scripts/raster_values_into_h3.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "create index if not exists gebco_elevation_h3_r8_h3_idx on gebco_elevation_h3_r8 using btree (h3) include (ele);"
	touch db/table/gebco_elevation_h3_r8

db/function/h3_path_loss: functions/h3_path_loss.sql | db/function ## Install RF path-loss helper
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/h3_path_loss.sql
	touch db/function/h3_path_loss

db/function/h3_visibility_clearance: functions/h3_visibility_clearance.sql db/table/h3_visibility_metrics db/function/h3_path_loss db/table/mesh_los_cache | db/function ## Install Fresnel-clearance helper with cache support
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/h3_visibility_clearance.sql
	touch db/function/h3_visibility_clearance

db/function/h3_los_between_cells: functions/h3_los_between_cells.sql db/table/gebco_elevation_h3_r8 db/table/mesh_los_cache | db/function ## Install LOS helper
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/h3_los_between_cells.sql
	touch db/function/h3_los_between_cells

db/function/mesh_surface_fill_visible_population: functions/mesh_surface_fill_visible_population.sql db/function/h3_los_between_cells | db/function ## Fill visible population for a single candidate cell
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/mesh_surface_fill_visible_population.sql
	touch db/function/mesh_surface_fill_visible_population

db/function/mesh_surface_refresh_reception_metrics: functions/mesh_surface_refresh_reception_metrics.sql db/function/h3_los_between_cells db/table/mesh_surface_h3_r8 db/table/mesh_towers | db/function ## Refresh reception metrics near a new tower
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/mesh_surface_refresh_reception_metrics.sql
	touch db/function/mesh_surface_refresh_reception_metrics

db/function/mesh_surface_refresh_visible_tower_counts: functions/mesh_surface_refresh_visible_tower_counts.sql db/function/h3_los_between_cells db/table/mesh_surface_h3_r8 db/table/mesh_towers | db/function ## Refresh visible tower counts near a new tower
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/mesh_surface_refresh_visible_tower_counts.sql
	touch db/function/mesh_surface_refresh_visible_tower_counts

db/function/mesh_visibility_invisible_route_geom: functions/mesh_visibility_invisible_route_geom.sql db/table/mesh_surface_h3_r8 db/table/mesh_route_graph db/table/mesh_route_graph_cache | db/function ## Build routing helper for invisible visibility edges
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/mesh_visibility_invisible_route_geom.sql
	touch db/function/mesh_visibility_invisible_route_geom

db/function/mesh_route_corridor_between_towers: functions/mesh_route_corridor_between_towers.sql db/table/mesh_surface_h3_r8 db/table/mesh_towers | db/function ## Produce pgRouting corridors between specific tower pairs
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f functions/mesh_route_corridor_between_towers.sql
	touch db/function/mesh_route_corridor_between_towers

db/test/seed_nodes_py: tests/test_seed_nodes.py scripts/merge_seed_nodes.py | db/test ## Run canonical seed merge unit tests
	python -m unittest -q tests/test_seed_nodes.py
	touch db/test/seed_nodes_py

db/test/pipeline_regressions_py: tests/test_pipeline_regressions.py Makefile procedures/fill_mesh_los_cache.sql docs/calculations.md docs/placement_strategies.md | db/test ## Run pipeline regression unit tests
	python -m unittest -q tests/test_pipeline_regressions.py
	touch db/test/pipeline_regressions_py

db/test/georgia_roads_geom: tests/georgia_roads_geom.sql db/table/georgia_roads_geom | db/test ## Verify only car-capable highways stay in roads layer
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/georgia_roads_geom.sql
	touch db/test/georgia_roads_geom

db/test/georgia_unfit_areas: tests/georgia_unfit_areas.sql db/table/georgia_unfit_areas | db/test ## Verify unfit area polygons include expected regions
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/georgia_unfit_areas.sql
	touch db/test/georgia_unfit_areas

db/test/population_h3_r8: tests/population_h3_r8.sql db/table/population_h3_r8 db/raw/kontur_population | db/test ## Check population table stays a 1:1 Kontur H3 cast
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/population_h3_r8.sql
	touch db/test/population_h3_r8

db/test/h3_los_between_cells: tests/h3_los_between_cells.sql db/function/h3_los_between_cells db/table/mesh_initial_nodes_h3_r8 | db/test ## Validate LOS results for seed nodes
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/h3_los_between_cells.sql
	touch db/test/h3_los_between_cells

db/test/mesh_surface_visible_towers: tests/mesh_surface_visible_towers.sql db/table/mesh_surface_h3_r8 db/function/h3_los_between_cells db/table/mesh_towers | db/test ## Confirm visible_tower_count matches sampled LOS tower counts
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_surface_visible_towers.sql
	touch db/test/mesh_surface_visible_towers

db/test/mesh_surface_refresh_reception_metrics: tests/mesh_surface_refresh_reception_metrics.sql db/function/mesh_surface_refresh_reception_metrics db/table/mesh_surface_h3_r8 db/table/mesh_towers | db/test ## Ensure localized reception refresh restores clearance and path loss
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_surface_refresh_reception_metrics.sql
	touch db/test/mesh_surface_refresh_reception_metrics

db/test/mesh_surface_refresh_visible_tower_counts: tests/mesh_surface_refresh_visible_tower_counts.sql db/function/mesh_surface_refresh_visible_tower_counts db/table/mesh_surface_h3_r8 db/table/mesh_towers | db/test ## Ensure localized visible tower refresh repopulates counts
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_surface_refresh_visible_tower_counts.sql
	touch db/test/mesh_surface_refresh_visible_tower_counts

db/test/mesh_surface_building_fields: tests/mesh_surface_building_fields.sql db/table/mesh_surface_h3_r8 db/table/buildings_h3_r8 | db/test ## Ensure building_count and has_building stay aligned on the surface
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_surface_building_fields.sql
	touch db/test/mesh_surface_building_fields

db/table/mesh_surface_h3_r8: tables/mesh_surface_h3_r8.sql db/table/mesh_surface_domain_h3_r8 db/table/roads_h3_r8 db/table/population_h3_r8 db/table/buildings_h3_r8 db/table/mesh_initial_nodes_h3_r8 db/table/mesh_towers db/table/georgia_unfit_areas db/table/gebco_elevation_h3_r8 db/function/h3_los_between_cells | db/table ## Populate mesh_surface_h3_r8 table
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_surface_h3_r8.sql
	touch db/table/mesh_surface_h3_r8

db/table/mesh_route_graph_cache: tables/mesh_route_graph_cache.sql | db/table ## Cache precomputed routing geometries per tower pair
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_route_graph_cache.sql
	touch db/table/mesh_route_graph_cache

db/table/mesh_route_graph: tables/mesh_route_graph.sql db/table/mesh_surface_h3_r8 db/table/mesh_route_graph_cache | db/table ## Precompute routing graph for invisible edge fallback
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_route_graph.sql
	touch db/table/mesh_route_graph

db/table/mesh_visibility_edges: tables/mesh_visibility_edges.sql scripts/mesh_visibility_edges_refresh.sql db/table/mesh_towers db/table/mesh_surface_h3_r8 db/table/mesh_route_graph db/table/mesh_route_graph_cache db/function/h3_los_between_cells db/procedure/mesh_visibility_edges_refresh | db/table ## Materialize visibility diagnostics for seed and active towers
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_visibility_edges.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql
	touch db/table/mesh_visibility_edges

db/test/mesh_visibility_edges: tests/mesh_visibility_edges.sql db/table/mesh_visibility_edges db/table/mesh_initial_nodes_h3_r8 db/function/h3_los_between_cells | db/test ## Validate seed visibility diagnostics stay accurate
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_visibility_edges.sql
	touch db/test/mesh_visibility_edges

db/test/mesh_visibility_edges_type: tests/mesh_visibility_edges_type.sql db/procedure/mesh_visibility_edges_refresh | db/test ## Validate visibility edge type labels stay canonical
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_visibility_edges_type.sql
	touch db/test/mesh_visibility_edges_type

db/test/mesh_visibility_invisible_route_geom: tests/mesh_visibility_invisible_route_geom.sql db/function/mesh_visibility_invisible_route_geom db/table/mesh_surface_h3_r8 db/table/mesh_route_graph db/table/mesh_route_graph_cache | db/test ## Ensure invisible visibility edges gain routed geometries
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_visibility_invisible_route_geom.sql
	touch db/test/mesh_visibility_invisible_route_geom

db/test/mesh_run_greedy_prepare: tests/mesh_run_greedy_prepare.sql procedures/mesh_run_greedy_prepare.sql db/table/mesh_surface_h3_r8 db/table/mesh_towers db/table/mesh_greedy_iterations | db/test ## Ensure greedy preparation preserves routed towers
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy_prepare.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_run_greedy_prepare.sql
	touch db/test/mesh_run_greedy_prepare

db/test/fill_mesh_los_cache_priority: tests/fill_mesh_los_cache_priority.sql db/table/mesh_surface_h3_r8 db/table/mesh_towers db/table/mesh_los_cache db/table/mesh_visibility_edges | db/test ## Validate LOS cache prioritizes nearest invisible edges
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/fill_mesh_los_cache_priority.sql
	touch db/test/fill_mesh_los_cache_priority

db/test/mesh_route_bootstrap_pairs: tests/mesh_route_bootstrap_pairs.sql db/table/mesh_route_bootstrap_pairs | db/test ## Validate install-priority bootstrap pairs load into the routing pipeline
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_route_bootstrap_pairs.sql
	touch db/test/mesh_route_bootstrap_pairs

db/test/mesh_route_corridor_between_towers: tests/mesh_route_corridor_between_towers.sql db/function/mesh_route_corridor_between_towers | db/test ## Validate corridor extraction between tower pairs
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_route_corridor_between_towers.sql
	touch db/test/mesh_route_corridor_between_towers


db/test/mesh_route: tests/mesh_route.sql db/procedure/fill_mesh_los_cache procedures/mesh_route_bridge.sql db/table/mesh_surface_h3_r8 db/table/mesh_towers db/table/mesh_los_cache | db/test ## Validate mesh_route cache/bridge stages
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_bridge.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_route.sql
	touch db/test/mesh_route

db/test/mesh_route_cluster_slim: tests/mesh_route_cluster_slim.sql procedures/mesh_route_cluster_slim.sql db/table/mesh_route_cluster_slim_failures | db/test ## Verify cluster slim iterates once and prefers seed endpoints
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_cluster_slim.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_route_cluster_slim.sql
	touch db/test/mesh_route_cluster_slim

db/test/mesh_coarse_grid: tests/mesh_coarse_grid.sql procedures/mesh_coarse_grid.sql db/table/mesh_surface_h3_r8 db/table/mesh_towers | db/test ## Verify coarse-grid seeding skips occupied parents and prefers building cells
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_coarse_grid.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_coarse_grid.sql
	touch db/test/mesh_coarse_grid

db/test/mesh_tower_wiggle: tests/mesh_tower_wiggle.sql procedures/mesh_tower_wiggle.sql db/function/h3_los_between_cells db/function/mesh_surface_fill_visible_population db/function/mesh_surface_refresh_reception_metrics db/function/mesh_surface_refresh_visible_tower_counts db/procedure/mesh_visibility_edges_refresh | db/test ## Validate bridge/cluster-slim wiggle pass relocates toward higher visible population
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_tower_wiggle.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tests/mesh_tower_wiggle.sql
	touch db/test/mesh_tower_wiggle

db/test/install_priority_py: tests/test_install_priority.py tests/test_install_priority_render.py scripts/export_install_priority.py scripts/install_priority_cluster_bounds.py scripts/install_priority_cluster_helpers.py scripts/install_priority_connectors.py scripts/install_priority_enrichment.py scripts/install_priority_geocoder.py scripts/install_priority_graph.py scripts/install_priority_graph_support.py scripts/install_priority_lib.py scripts/install_priority_map_payload.py scripts/install_priority_maplibre.py scripts/install_priority_points.py scripts/install_priority_render.py scripts/install_priority_sources.py | db/test ## Run installer-priority Python unit tests
	python -m unittest discover -s tests -p 'test_install_priority*.py'
	touch db/test/install_priority_py

db/procedure/mesh_coarse_grid: procedures/mesh_coarse_grid.sql db/table/mesh_surface_h3_r8 db/table/mesh_towers db/function/h3_los_between_cells | db/procedure ## Install coarse-grid towers ahead of routing stages
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_coarse_grid.sql
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "call mesh_coarse_grid();"
	touch db/procedure/mesh_coarse_grid

db/procedure/fill_mesh_los_cache_prepare: scripts/fill_mesh_los_cache_prepare.sql db/table/mesh_surface_h3_r8 db/table/mesh_towers db/table/mesh_los_cache db/procedure/mesh_visibility_edges_route_priority_geom db/procedure/mesh_route_bootstrap db/procedure/mesh_coarse_grid | db/procedure ## Build route candidate and missing-pair staging for LOS cache fill
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/fill_mesh_los_cache_prepare.sql
	touch db/procedure/fill_mesh_los_cache_prepare

db/procedure/fill_mesh_los_cache_batch_once: scripts/fill_mesh_los_cache_batch.sql db/procedure/fill_mesh_los_cache_prepare db/function/h3_visibility_clearance db/table/mesh_los_cache | db/procedure ## Commit one LOS batch from the prepared missing-pair queue
	PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -v batch_limit=50000 -f scripts/fill_mesh_los_cache_batch.sql
	touch db/procedure/fill_mesh_los_cache_batch_once

db/procedure/fill_mesh_los_cache_batches: scripts/fill_mesh_los_cache_batch.sql db/procedure/fill_mesh_los_cache_prepare db/function/h3_visibility_clearance db/table/mesh_los_cache | db/procedure ## Drain missing LOS pairs in committed batches so reruns can resume
	bash -lc 'set -euo pipefail; while [ "$$(PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select case when exists (select 1 from mesh_route_missing_pairs limit 1) then 1 else 0 end")" -eq 1 ]; do PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -v batch_limit=50000 -f scripts/fill_mesh_los_cache_batch.sql; done'
	touch db/procedure/fill_mesh_los_cache_batches


db/procedure/fill_mesh_los_cache_parallel: scripts/fill_mesh_los_cache_parallel.sh scripts/fill_mesh_los_cache_parallel_job.sh scripts/fill_mesh_los_cache_batch.sql scripts/fill_mesh_los_cache_queue_indexes.sql | db/procedure ## Launch a finite eight-way GNU parallel run over the current LOS queue snapshot
	bash scripts/fill_mesh_los_cache_parallel.sh
	touch db/procedure/fill_mesh_los_cache_parallel

db/procedure/fill_mesh_los_cache_finalize: scripts/fill_mesh_los_cache_finalize.sql db/procedure/fill_mesh_los_cache_batch_once | db/procedure ## Build the route graph from the currently cached LOS pairs
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/fill_mesh_los_cache_finalize.sql
	touch db/procedure/fill_mesh_los_cache_ready
	touch db/procedure/fill_mesh_los_cache_finalize

db/procedure/fill_mesh_los_cache_ready: | db/procedure ## Mark that the route graph matches the currently materialized LOS cache
	test -f db/procedure/fill_mesh_los_cache_ready

db/procedure/fill_mesh_los_cache: db/procedure/fill_mesh_los_cache_finalize | db/procedure ## Seed one committed LOS batch and build a first routing graph
	touch db/procedure/fill_mesh_los_cache_ready
	touch db/procedure/fill_mesh_los_cache

db/procedure/fill_mesh_los_cache_backfill: scripts/fill_mesh_los_cache_prepare.sql scripts/fill_mesh_los_cache_batch.sql scripts/fill_mesh_los_cache_finalize.sql db/function/h3_visibility_clearance db/table/mesh_los_cache | db/procedure ## Backfill more LOS pairs in committed batches and refresh the route graph
	bash -lc 'set -euo pipefail; if [ "$$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select case when to_regclass('\''mesh_route_missing_pairs'\'') is null then 0 else 1 end")" -eq 0 ]; then psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/fill_mesh_los_cache_prepare.sql; fi; psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/fill_mesh_los_cache_queue_indexes.sql; while [ "$$(PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select case when exists (select 1 from mesh_route_missing_pairs limit 1) then 1 else 0 end")" -eq 1 ]; do PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -v batch_limit=50000 -f scripts/fill_mesh_los_cache_batch.sql; done'
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/fill_mesh_los_cache_finalize.sql
	touch db/procedure/fill_mesh_los_cache_ready
	touch db/procedure/fill_mesh_los_cache_backfill

db/procedure/fill_mesh_los_cache_resume: scripts/fill_mesh_los_cache_batch.sql scripts/fill_mesh_los_cache_finalize.sql db/procedure/fill_mesh_los_cache_backfill | db/procedure ## Backward-compatible alias for manual LOS cache backfill
	touch db/procedure/fill_mesh_los_cache_resume

db/procedure/mesh_route_bridge: procedures/mesh_route_bridge.sql db/procedure/fill_mesh_los_cache_ready db/function/mesh_surface_refresh_reception_metrics db/function/mesh_surface_refresh_visible_tower_counts | db/procedure ## Bridge farthest tower clusters via pgRouting
	bash -lc 'set -euo pipefail; PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_bridge.sql'
	touch db/procedure/mesh_route_bridge

db/procedure/mesh_tower_wiggle: procedures/mesh_tower_wiggle.sql db/procedure/mesh_route db/procedure/mesh_route_cluster_slim db/procedure/mesh_visibility_edges_refresh db/function/mesh_surface_fill_visible_population db/function/mesh_surface_refresh_reception_metrics db/function/mesh_surface_refresh_visible_tower_counts db/table/mesh_surface_h3_r8 db/table/mesh_towers db/table/mesh_visibility_edges | db/procedure ## Recenter population, route, bridge, and cluster-slim towers toward denser visible population
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_tower_wiggle.sql
	bash -lc 'set -euo pipefail; iter=0; max_iters=$${WIGGLE_ITERATIONS:-0}; reset=true; while :; do iter=$$((iter+1)); echo ">> Wiggle iteration $$iter"; moved=$$(psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select mesh_tower_wiggle($$reset);"); reset=false; moved=$${moved:-0}; if [ "$$moved" -eq 0 ]; then echo ">> Wiggle converged after $$((iter-1)) iteration(s)"; break; fi; if [ "$$max_iters" -gt 0 ] && [ "$$iter" -ge "$$max_iters" ]; then echo ">> Wiggle hit iteration cap $$max_iters"; break; fi; done'
	touch db/procedure/mesh_tower_wiggle

db/procedure/mesh_visibility_edges_refresh: procedures/mesh_visibility_edges_refresh.sql db/table/mesh_towers db/table/mesh_surface_h3_r8 db/table/gebco_elevation_h3_r8 db/function/h3_los_between_cells | db/procedure ## Install core visibility refresh procedure
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_visibility_edges_refresh.sql
	touch db/procedure/mesh_visibility_edges_refresh

db/procedure/mesh_visibility_edges_refresh_route_geom: procedures/mesh_visibility_edges_refresh_route_geom.sql db/table/mesh_visibility_edges db/procedure/mesh_visibility_edges_refresh db/function/mesh_visibility_invisible_route_geom | db/procedure ## Install routed-geometry backfill for visibility diagnostics
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_visibility_edges_refresh_route_geom.sql
	touch db/procedure/mesh_visibility_edges_refresh_route_geom

db/procedure/mesh_visibility_edges_route_priority_geom: scripts/mesh_visibility_edges_refresh_route_priority_geom.sql db/table/mesh_visibility_edges db/function/mesh_visibility_invisible_route_geom | db/procedure ## Backfill routed geometry for inter-cluster priority gaps before LOS cache staging
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh_route_priority_geom.sql
	touch db/procedure/mesh_visibility_edges_route_priority_geom


db/table/mesh_route_cluster_slim_failures: tables/mesh_route_cluster_slim_failures.sql | db/table ## Track cluster slim outcomes between iterations
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f tables/mesh_route_cluster_slim_failures.sql
	touch db/table/mesh_route_cluster_slim_failures

db/procedure/mesh_route_cluster_slim: procedures/mesh_route_cluster_slim.sql db/table/mesh_route_cluster_slim_failures db/procedure/mesh_route_bridge db/procedure/fill_mesh_los_cache_ready db/function/mesh_route_corridor_between_towers db/function/mesh_surface_refresh_reception_metrics db/function/mesh_surface_refresh_visible_tower_counts db/procedure/mesh_visibility_edges_refresh | db/procedure ## Shorten long intra-cluster hops with routing towers
	bash -lc 'set -euo pipefail; PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_cluster_slim.sql'
	bash -lc 'set -euo pipefail; max_iters=$${SLIM_ITERATIONS:-0}; iter=0; while :; do iter=$$((iter+1)); echo ">> Cluster slim iteration $$iter"; promoted=$$(PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "call mesh_route_cluster_slim($$iter, null);"); promoted=$${promoted:-0}; if [ "$$promoted" -eq 0 ]; then echo ">> Cluster slim converged after $$((iter-1)) iteration(s)"; break; fi; if [ "$$max_iters" -gt 0 ] && [ "$$iter" -ge "$$max_iters" ]; then echo ">> Cluster slim hit iteration cap $$max_iters"; break; fi; done'
	touch db/procedure/mesh_route_cluster_slim

db/procedure/mesh_route_refresh_visibility: scripts/mesh_visibility_edges_refresh.sql db/table/mesh_visibility_edges db/procedure/mesh_route_cluster_slim db/procedure/mesh_visibility_edges_refresh | db/procedure ## Rebuild core visibility diagnostics after routing stages
	psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql
	touch db/procedure/mesh_route_refresh_visibility

db/procedure/mesh_route: db/procedure/mesh_route_refresh_visibility | db/procedure ## Build PG routing bridges between tower clusters
	touch db/procedure/mesh_route

db/procedure/mesh_run_greedy: procedures/mesh_run_greedy_prepare.sql procedures/mesh_run_greedy.sql procedures/mesh_run_greedy_finalize.sql scripts/mesh_visibility_edges_refresh.sql db/procedure/mesh_route db/table/mesh_visibility_edges db/table/mesh_surface_h3_r8 db/table/mesh_greedy_iterations db/function/mesh_surface_refresh_reception_metrics db/function/mesh_surface_refresh_visible_tower_counts db/function/mesh_surface_fill_visible_population db/table/mesh_initial_nodes_h3_r8 | db/procedure ## Skip greedy placement loop (use mesh_run_greedy_full)
	echo ">> Greedy placement disabled for now (use db/procedure/mesh_run_greedy_full)"
	touch db/procedure/mesh_run_greedy

db/procedure/mesh_run_greedy_full: procedures/mesh_run_greedy_prepare.sql procedures/mesh_run_greedy.sql procedures/mesh_run_greedy_finalize.sql scripts/mesh_visibility_edges_refresh.sql db/procedure/mesh_route db/table/mesh_visibility_edges db/table/mesh_surface_h3_r8 db/table/mesh_greedy_iterations db/function/mesh_surface_refresh_reception_metrics db/function/mesh_surface_refresh_visible_tower_counts db/function/mesh_surface_fill_visible_population db/table/mesh_initial_nodes_h3_r8 | db/procedure ## Execute greedy placement loop (explicit)
	PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy_prepare.sql
	PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -c "call mesh_run_greedy_prepare();"
	bash -lc 'set -euo pipefail; for iter in $$(seq 1 100); do echo ">> Greedy iteration $$iter"; PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy.sql; PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql; done'
	PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy_finalize.sql
	PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql
	touch db/procedure/mesh_run_greedy_full

data/out/visuals: | data/out ## Ensure visuals output directory exists
	mkdir -p data/out/visuals

data/out/install_priority.html: scripts/export_install_priority.py scripts/install_priority_cluster_bounds.py scripts/install_priority_cluster_helpers.py scripts/install_priority_connectors.py scripts/install_priority_enrichment.py scripts/install_priority_geocoder.py scripts/install_priority_graph.py scripts/install_priority_graph_support.py scripts/install_priority_lib.py scripts/install_priority_map_payload.py scripts/install_priority_maplibre.py scripts/install_priority_points.py scripts/install_priority_render.py scripts/install_priority_sources.py db/table/mesh_towers db/table/mesh_visibility_edges db/table/mesh_initial_nodes_h3_r8 | data/out ## Export installer-priority HTML handout and CSV table
	python scripts/export_install_priority.py --csv-output data/out/install_priority.csv --html-output data/out/install_priority.html

data/out/install_priority.csv: data/out/install_priority.html | data/out ## Ensure installer-priority CSV exists after export
	test -f data/out/install_priority.csv

data/out/visuals/mesh_surface.png: scripts/render_mapnik.py mapnik/styles/mesh_style.xml db/procedure/mesh_route_bridge | data/out/visuals ## Render static mesh surface map
	python scripts/render_mapnik.py --output data/out/visuals/mesh_surface.png

data/out/visuals/longfast: scripts/render_longfast_animation.py scripts/longfast_animation_lib.py mapnik/styles/mesh_style.xml db/procedure/mesh_tower_wiggle | data/out/visuals ## Render LongFast animation frames
	python scripts/render_longfast_animation.py --output-dir data/out/visuals/longfast --no-video

data/out/visuals/longfast.mp4: data/out/visuals/longfast | data/out/visuals ## Assemble LongFast video
	ffmpeg -y -framerate 24 -i data/out/visuals/longfast/frame_%04d.png -vf "scale=1920:-1:flags=lanczos,format=yuv420p" data/out/visuals/longfast.mp4

visuals: data/out/visuals/mesh_surface.png data/out/visuals/longfast data/out/visuals/longfast.mp4 ## Render static map and LongFast animation frames
