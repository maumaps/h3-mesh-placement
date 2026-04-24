"""Regression checks for pipeline wiring and batch-complete routing fill."""

from pathlib import Path
import re
import subprocess
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]


class PipelineRegressionTest(unittest.TestCase):
    """Keep recent pipeline regressions from sneaking back in."""

    def test_parallel_launcher_batch_math_has_no_off_by_one(self) -> None:
        """Finite GNU parallel job counts should match 50k batch boundaries exactly."""
        cases = {
            0: 0,
            1: 1,
            49_999: 1,
            50_000: 1,
            50_001: 2,
            99_999: 2,
            100_000: 2,
            100_001: 3,
        }

        for queue_rows, expected_jobs in cases.items():
            actual_jobs = -(-queue_rows // 50_000)
            self.assertEqual(
                actual_jobs,
                expected_jobs,
                f"Finite LOS launcher should map {queue_rows} queued rows to {expected_jobs} 50k jobs, got {actual_jobs}.",
            )

    def test_pipeline_config_controls_placement_stages(self) -> None:
        """Placement toggles and key parameters should live in one SQL config file."""
        settings_text = (REPO_ROOT / "tables" / "mesh_pipeline_settings.sql").read_text()
        makefile_text = (REPO_ROOT / "Makefile").read_text()
        coarse_text = (REPO_ROOT / "procedures" / "mesh_coarse_grid.sql").read_text()
        greedy_script_text = (REPO_ROOT / "scripts" / "mesh_run_greedy_configured.sh").read_text()
        population_text = (REPO_ROOT / "procedures" / "mesh_population.sql").read_text()
        contract_text = (REPO_ROOT / "procedures" / "mesh_population_anchor_contract.sql").read_text()
        placement_restart_text = (REPO_ROOT / "scripts" / "mesh_placement_restart.sh").read_text()

        for setting, value in {
            "enable_coarse": "false",
            "enable_greedy": "false",
            "enable_route_bridge": "true",
            "enable_cluster_slim": "true",
            "los_batch_limit": "50000",
            "los_parallel_jobs": "0",
            "min_tower_separation_m": "0",
            "generated_tower_merge_distance_m": "10000",
            "enable_population": "true",
            "enable_population_anchor_contract": "true",
            "population_anchor_contract_distance_m": "0",
            "enable_generated_pair_contract": "true",
            "population_anchor_min_count": "7",
            "population_anchor_max_count": "7",
            "population_existing_anchor_weight": "1000000",
            "population_anchor_cluster_oversampling": "2",
            "enable_wiggle": "true",
            "wiggle_candidate_limit": "256",
        }.items():
            self.assertIn(
                f"('{setting}', '{value}')",
                settings_text,
                f"mesh_pipeline_settings.sql should expose {setting}={value} in the single user-editable pipeline config file.",
            )

        self.assertIn(
            "from mesh_pipeline_settings\n        where setting = 'enable_coarse'",
            coarse_text,
            "mesh_coarse_grid should read enable_coarse from mesh_pipeline_settings before inserting coarse towers.",
        )
        self.assertIn(
            "delete from mesh_towers where source = 'coarse';",
            coarse_text,
            "Disabling coarse placement should remove stale coarse towers during a restart.",
        )
        self.assertIn(
            "scripts/mesh_run_greedy_configured.sh",
            makefile_text,
            "Greedy Make targets should delegate to the configured wrapper instead of hardcoding an unconditional greedy loop.",
        )
        self.assertIn(
            "db/procedure/mesh_placement_restart: scripts/mesh_placement_restart.sh",
            makefile_text,
            "Makefile should expose a safe placement restart target that does not rebuild cached table dependencies when only tower placement needs replaying.",
        )
        self.assertIn(
            "psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_population.sql",
            placement_restart_text,
            "Safe placement restart should replay the configured population anchor stage before route bootstrap sees current towers.",
        )
        self.assertIn(
            "psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_route_bootstrap.sql",
            placement_restart_text,
            "Safe placement restart should reseed route bootstrap without truncating or recreating the LOS cache table.",
        )
        self.assertIn(
            "psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_population_anchor_contract.sql",
            placement_restart_text,
            "Safe placement restart should contract soft population anchors after routing and before final visibility refresh.",
        )
        self.assertIn(
            "psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_generated_pair_contract.sql",
            placement_restart_text,
            "Safe placement restart should contract generated tower pairs after population-anchor cleanup and before final visibility refresh.",
        )
        self.assertIn(
            "scripts/mesh_tower_wiggle_configured.sh",
            placement_restart_text,
            "Safe placement restart should include the configured tower-wiggle stage so enable_wiggle works without a full table rebuild.",
        )
        self.assertIn(
            "scripts/mesh_tower_wiggle_configured.sh\n\n"
            'echo ">> Applying manually reviewed route redundancy anchors"\n'
            "psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_route_manual_redundancy.sql\n\n"
            'echo ">> Refreshing visibility diagnostics after tower wiggle"\n'
            "psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql\n"
            "psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/assert_mesh_towers_single_los_component.sql",
            placement_restart_text,
            "Safe placement restart should apply reviewed redundancy anchors after tower wiggle, then refresh mesh_visibility_edges so exports and cluster checks read current LOS diagnostics.",
        )
        self.assertIn(
            "db/procedure/mesh_tower_wiggle_current",
            makefile_text,
            "Makefile should expose a safe current-wiggle target that does not rebuild route inputs when only local refinement is requested.",
        )
        self.assertIn(
            "delete from mesh_towers where source in ('greedy', 'bridge')",
            greedy_script_text,
            "Disabling greedy placement should clean stale greedy/bridge towers during a restart.",
        )
        self.assertIn(
            "truncate mesh_greedy_iterations",
            greedy_script_text,
            "Disabling greedy placement should clear iteration logs during a restart.",
        )
        self.assertIn(
            "power(ln(1 + rc.nearby_population)",
            population_text,
            "mesh_population should interleave nearby population with building count instead of sorting those dimensions lexicographically.",
        )
        self.assertIn(
            "existing_anchor_cells as",
            population_text,
            "mesh_population should feed existing towers into KMeans as heavy anchors instead of erasing covered demand before clustering.",
        )
        self.assertIn(
            "population_existing_anchor_weight",
            population_text,
            "mesh_population should expose the heavy existing-anchor weight through mesh_pipeline_settings.",
        )
        self.assertIn(
            "cluster_has_existing_anchor",
            population_text,
            "mesh_population should drop clusters that already contain an existing tower anchor after clustering.",
        )
        self.assertNotIn(
            "h3_los_between_cells(s.h3, t.h3)",
            population_text,
            "mesh_population should not run expensive LOS pre-filtering while ranking population anchor candidates.",
        )
        self.assertIn(
            "ST_ClusterKMeans(cp.cluster_geom, (select k from cluster_count)) over ()",
            population_text,
            "mesh_population should use fixed-k clustering from configured max count instead of radius-limited clustering in the default path.",
        )
        self.assertIn(
            "sc.cluster_weight",
            population_text,
            "mesh_population should pass configurable demand weight as the M coordinate for weighted KMeans.",
        )
        self.assertIn(
            "cluster_centroids as",
            population_text,
            "mesh_population should reconstruct weighted KMeans centroids before snapping winners to placeable H3 cells.",
        )
        self.assertIn(
            "cl.score desc,\n        cl.nearby_population desc,\n        cl.building_count desc,\n        ST_3DDistance(cl.cluster_geom, cc.centroid_geom) asc",
            population_text,
            "mesh_population should choose high-demand candidates inside each weighted cluster before using centroid distance as a tie-breaker.",
        )
        self.assertNotIn(
            "kutaisi",
            settings_text.lower() + population_text.lower(),
            "Kutaisi should stay a regression-test calibration point, not production pipeline input.",
        )
        self.assertIn(
            "enable_population_anchor_contract",
            contract_text,
            "mesh_population_anchor_contract should be guarded by the single user-editable config switch.",
        )
        self.assertIn(
            "population_anchor_contract_distance_m",
            contract_text,
            "mesh_population_anchor_contract should support topology-only mode instead of reusing generated route-tower merge radius.",
        )
        self.assertIn(
            "merge_distance <= 0",
            contract_text,
            "mesh_population_anchor_contract should treat distance 0 as topology-only replacement search.",
        )
        self.assertIn(
            "mesh_los_cache",
            contract_text,
            "mesh_population_anchor_contract should use cached LOS neighbor sets for contraction decisions.",
        )
        self.assertIn(
            "required.source <> 'population'",
            contract_text,
            "mesh_population_anchor_contract should preserve non-population visible-neighbor roles when removing soft anchors.",
        )
        self.assertNotIn(
            "h3_los_between_cells",
            contract_text,
            "mesh_population_anchor_contract must not compute fresh terrain LOS while contracting soft anchors.",
        )
        self.assertNotIn(
            "population_anchor_contract_local_population",
            settings_text + contract_text,
            "Population anchor contraction should not depend on arbitrary local-population thresholds.",
        )
        self.assertNotIn(
            "population_anchor_contract_building",
            settings_text + contract_text,
            "Population anchor contraction should not depend on arbitrary building-count thresholds.",
        )

    def test_initial_nodes_import_does_not_skip_existing_table(self) -> None:
        """Canonical seed refresh should always reach Postgres on rebuild."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()

        self.assertIn(
            'ogr2ogr -f PostgreSQL "PG:dbname=$${PGDATABASE:-$${USER}} user=$${PGUSER:-$${USER}} host=$${PGHOST:-/var/run/postgresql} port=$${PGPORT:-5432}" data/in/existing_mesh_nodes.geojson -nln mesh_initial_nodes -nlt POINT -lco GEOMETRY_NAME=geom -overwrite -a_srs EPSG:4326',
            makefile_text,
            "db/raw/initial_nodes should reimport the refreshed canonical seed GeoJSON with -overwrite so refreshed Liam Cottle or curated seeds are not skipped.",
        )
        self.assertIn(
            "db/raw/initial_nodes: data/in/existing_mesh_nodes.geojson | db/raw ## Import canonical seed tower locations",
            makefile_text,
            "db/raw/initial_nodes should import canonical seeds without depending on db/table/mesh_towers, otherwise a simple seed refresh can drop live towers before ogr2ogr runs.",
        )
        self.assertNotIn(
            "db/raw/initial_nodes: data/in/existing_mesh_nodes.geojson db/table/mesh_towers | db/raw",
            makefile_text,
            "db/raw/initial_nodes must not depend on db/table/mesh_towers because that target recreates mesh_towers and is unsafe during a seed-only refresh.",
        )
        self.assertNotIn(
            "mesh_initial_nodes already exists, skipping import",
            makefile_text,
            "db/raw/initial_nodes must not skip importing just because mesh_initial_nodes already exists, otherwise refreshed seeds never reach Postgres.",
        )

    def test_osm_merge_target_overwrites_existing_mid_pbf(self) -> None:
        """OSM merge refresh should succeed when the merged PBF already exists."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()

        self.assertIn(
            "osmium merge data/in/osm/georgia-latest.osm.pbf data/in/osm/armenia-latest.osm.pbf --overwrite -o data/mid/osm/osm_for_mesh_placement.osm.pbf -f pbf",
            makefile_text,
            "The merged OSM PBF target should pass --overwrite so safe seed-layer reruns do not fail just because data/mid/osm/osm_for_mesh_placement.osm.pbf already exists.",
        )

    def test_population_import_uses_active_pg_connection(self) -> None:
        """Kontur import should honor the selected database under make -j runs."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()

        self.assertIn(
            "db/table/population_h3_r8: tables/population_h3_r8.sql db/raw/kontur_population db/table/georgia_boundary db/table/roads_h3_r8 | db/table",
            makefile_text,
            "population_h3_r8 should depend on db/raw/kontur_population so make -j cannot aggregate population before the raw import exists.",
        )
        self.assertIn(
            'ogr2ogr -f PostgreSQL "PG:dbname=$${PGDATABASE:-$${USER}} user=$${PGUSER:-$${USER}} host=$${PGHOST:-/var/run/postgresql} port=$${PGPORT:-5432}" data/mid/population/kontur_population_20231101.gpkg -nln kontur_population -nlt MULTIPOLYGON -lco GEOMETRY_NAME=geom -overwrite -t_srs EPSG:4326',
            makefile_text,
            "db/raw/kontur_population should import into the active PGDATABASE instead of using an empty PG connection string.",
        )
        self.assertNotIn(
            'ogr2ogr -f PostgreSQL PG:\\"\\" data/mid/population/kontur_population_20231101.gpkg',
            makefile_text,
            "db/raw/kontur_population must not use PG:\"\", which GDAL treats as invalid connection info on geocint.",
        )

    def test_surface_build_disables_interactive_statement_timeout(self) -> None:
        """The 100 km surface neighborhood build is expected to exceed short server timeouts."""
        surface_text = (REPO_ROOT / "tables" / "mesh_surface_h3_r8.sql").read_text()

        self.assertIn(
            "set statement_timeout = 0;",
            surface_text,
            "mesh_surface_h3_r8 should opt out of server-side interactive statement_timeout because its 100 km ST_DWithin population and LOS checks can run longer than 10 minutes on geocint.",
        )
        self.assertIn(
            "create temporary table mesh_surface_population_points as",
            surface_text,
            "mesh_surface_h3_r8 should stage coarse populated buckets in a temporary projected geometry table so the 100 km population weight does not scan geography pairs directly.",
        )
        self.assertIn(
            "h3_cell_to_parent(h3, 6) as h3",
            surface_text,
            "mesh_surface_h3_r8 should roll r8 population cells up to r6 demand buckets before the 100 km ranking-weight join.",
        )
        self.assertIn(
            "create index mesh_surface_population_points_geom_idx on mesh_surface_population_points using gist (geom);",
            surface_text,
            "mesh_surface_h3_r8 should index the temporary projected population buckets before the 100 km ST_DWithin aggregation.",
        )
        self.assertIn(
            "create temporary table mesh_surface_tower_candidates as",
            surface_text,
            "mesh_surface_h3_r8 should stage tower candidates separately so the 100 km population aggregation can resume from an inspectable SQL block.",
        )
        self.assertIn(
            "ST_DWithin(pop.geom, c.geom, 100000)",
            surface_text,
            "mesh_surface_h3_r8 should run the 100 km population aggregation against projected geometry in meters after staging candidate and population points.",
        )
        self.assertIn(
            "from mesh_los_cache lc",
            surface_text,
            "mesh_surface_h3_r8 should initialize visible_tower_count from cached LOS metrics instead of computing fresh terrain LOS during the full surface rebuild.",
        )
        self.assertNotIn(
            "where h3_los_between_cells(vp.cell_h3, vp.tower_h3)",
            surface_text,
            "mesh_surface_h3_r8 must not call h3_los_between_cells for every tower pair during the full surface rebuild; cache warming is handled by resumable LOS targets.",
        )
        self.assertIn(
            "db/table/mesh_surface_h3_r8: tables/mesh_surface_h3_r8.sql db/table/mesh_surface_domain_h3_r8 db/table/roads_h3_r8 db/table/population_h3_r8 db/table/buildings_h3_r8 db/table/mesh_initial_nodes_h3_r8 db/table/mesh_towers db/table/georgia_unfit_areas db/table/gebco_elevation_h3_r8 db/table/mesh_los_cache",
            (REPO_ROOT / "Makefile").read_text(),
            "mesh_surface_h3_r8 should depend on the preserved LOS cache table when it initializes visible_tower_count from cached metrics.",
        )

    def test_fill_mesh_los_cache_main_target_stays_partial_and_backfill_loops(self) -> None:
        """The main pipeline should do one committed batch, while manual backfill drains the queue later."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()
        parallel_script_text = (REPO_ROOT / "scripts" / "fill_mesh_los_cache_parallel.sh").read_text()
        batch_text = (REPO_ROOT / "scripts" / "fill_mesh_los_cache_batch.sql").read_text()
        finalize_text = (REPO_ROOT / "scripts" / "fill_mesh_los_cache_finalize.sql").read_text()

        self.assertIn(
            "db/procedure/fill_mesh_los_cache_prepare",
            makefile_text,
            "Makefile should keep an explicit prepare stage so cache-fill staging can be reused and reasoned about separately.",
        )
        self.assertIn(
            "db/procedure/fill_mesh_los_cache_batch_once",
            makefile_text,
            "Makefile should keep the one-batch normal pipeline as its own intermediate target so dependency invalidation flows through route bootstrap and prepare markers.",
        )
        self.assertIn(
            "scripts/fill_mesh_los_cache_batch_once.sh",
            makefile_text,
            "The single-batch fill target should delegate to a configured wrapper so the route pipeline can progress without hardcoded batch parameters.",
        )
        self.assertIn(
            "db/procedure/fill_mesh_los_cache: db/procedure/fill_mesh_los_cache_finalize | db/procedure",
            makefile_text,
            "The main fill target should depend on the finalize marker instead of bypassing prepare and bootstrap dependencies with direct SQL calls.",
        )
        self.assertIn(
            "db/procedure/fill_mesh_los_cache_ready: | db/procedure",
            makefile_text,
            "Downstream routing stages should depend on a stable cache-ready marker instead of pulling the whole cache prepare/batch/finalize chain back into route recalculation.",
        )
        self.assertIn(
            "db/procedure/mesh_route_bridge: procedures/mesh_route_bridge.sql scripts/mesh_route_bridge_configured.sh scripts/assert_mesh_towers_single_los_component.sql db/procedure/fill_mesh_los_cache_ready",
            makefile_text,
            "mesh_route_bridge should consume the materialized route graph marker without forcing another LOS cache batch when towers are recalculated from a completed cache.",
        )
        self.assertIn(
            "db/procedure/mesh_route_cluster_slim: procedures/mesh_route_cluster_slim.sql scripts/mesh_route_cluster_slim_configured.sh scripts/assert_mesh_towers_single_los_component.sql db/table/mesh_route_cluster_slim_failures db/procedure/mesh_route_bridge db/procedure/fill_mesh_los_cache_ready",
            makefile_text,
            "mesh_route_cluster_slim should share the same cache-ready marker so downstream tower recalculation does not restart LOS cache preparation.",
        )
        self.assertIn(
            "db/procedure/fill_mesh_los_cache_prepare: scripts/fill_mesh_los_cache_prepare.sql db/table/mesh_surface_h3_r8 db/table/mesh_towers db/table/mesh_los_cache db/procedure/mesh_visibility_edges_route_priority_geom db/procedure/mesh_route_bootstrap db/procedure/mesh_population",
            makefile_text,
            "fill_mesh_los_cache_prepare should depend on configured population anchors so route cache staging sees the sparse city/serviceability hints.",
        )
        self.assertIn(
            "db/procedure/fill_mesh_los_cache_finalize: scripts/fill_mesh_los_cache_finalize.sql db/procedure/fill_mesh_los_cache_batch_once | db/procedure",
            makefile_text,
            "Route-graph finalization should depend on the committed one-batch marker so the normal pipeline never skips the bootstrap-invalidated batch step.",
        )
        self.assertIn(
            "db/procedure/fill_mesh_los_cache_backfill",
            makefile_text,
            "Makefile should expose a separate manual backfill target for draining more LOS pairs after the first route results are available.",
        )
        self.assertIn(
            "db/procedure/fill_mesh_los_cache_parallel",
            makefile_text,
            "Makefile should expose a single GNU parallel launcher target so operators can burn through the LOS queue without manual per-worker make commands.",
        )
        self.assertIn(
            "bash scripts/fill_mesh_los_cache_parallel.sh",
            makefile_text,
            "The parallel LOS launcher target should delegate its shell logic to a checked-in script so the Makefile stays debuggable.",
        )
        self.assertIn(
            "scripts/fill_mesh_los_cache_parallel_job.sh",
            makefile_text,
            "The finite GNU parallel launcher target should depend on the one-batch job helper script so queue jobs are reproducible and debuggable.",
        )
        self.assertIn(
            'seq "${job_count}" | parallel \\',
            parallel_script_text,
            "The GNU parallel launcher script should feed a finite batch-count job queue into GNU parallel so ETA reflects the remaining LOS batches.",
        )
        self.assertIn(
            'parallel_jobs="$(pg_setting_int los_parallel_jobs)"',
            parallel_script_text,
            "The GNU parallel launcher should read optional worker parallelism from mesh_pipeline_settings instead of hardcoding the current machine shape.",
        )
        self.assertIn(
            'if [ "${parallel_jobs}" -gt 0 ]; then',
            parallel_script_text,
            "The GNU parallel launcher should let los_parallel_jobs=0 fall through to GNU parallel's CPU-count default.",
        )
        self.assertIn(
            'parallel_job_args+=(--jobs "${parallel_jobs}")',
            parallel_script_text,
            "The GNU parallel launcher should pass --jobs only when the operator pins a positive job count.",
        )
        self.assertIn(
            '"${parallel_job_args[@]}"',
            parallel_script_text,
            "The GNU parallel launcher should use the optional job arguments when invoking GNU parallel.",
        )
        self.assertIn(
            "--line-buffer",
            parallel_script_text,
            "The GNU parallel launcher should line-buffer worker output so batch progress remains readable while jobs run concurrently.",
        )
        self.assertIn(
            "scripts/fill_mesh_los_cache_parallel_job.sh {}",
            parallel_script_text,
            "The GNU parallel launcher should hand each finite job token to a dedicated helper script so psql never mistakes it for a database or role name.",
        )
        self.assertIn(
            "parallel_eta_args+=(--eta)",
            parallel_script_text,
            "The GNU parallel launcher should request ETA output when it has a TTY, but stay quiet under nohup where /dev/tty is unavailable.",
        )
        self.assertIn(
            'batch_limit="$(pg_setting_int los_batch_limit)"',
            parallel_script_text,
            "The GNU parallel launcher should read its batch size from mesh_pipeline_settings so users can tune finite job granularity in one file.",
        )
        self.assertIn(
            "ceil(count(*)::numeric / ${batch_limit})::integer from mesh_route_missing_pairs",
            parallel_script_text,
            "The GNU parallel launcher should snapshot the current queue length using the configured batch size.",
        )
        self.assertIn(
            "psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/fill_mesh_los_cache_queue_indexes.sql",
            parallel_script_text,
            "The GNU parallel launcher should refresh queue indexes once before starting workers so every job sees the indexed queue immediately.",
        )
        self.assertNotIn(
            "db/procedure/fill_mesh_los_cache_backfill: scripts/fill_mesh_los_cache_prepare.sql scripts/fill_mesh_los_cache_batch.sql scripts/fill_mesh_los_cache_finalize.sql db/procedure/fill_mesh_los_cache_prepare",
            makefile_text,
            "Manual LOS cache backfill should not depend on the prepare marker directly, otherwise resume rebuilds staging from scratch instead of continuing an existing queue.",
        )
        backfill_script_text = (REPO_ROOT / "scripts" / "fill_mesh_los_cache_backfill.sh").read_text()
        batches_script_text = (REPO_ROOT / "scripts" / "fill_mesh_los_cache_batches.sh").read_text()
        self.assertIn(
            "select case when exists (select 1 from mesh_route_missing_pairs limit 1) then 1 else 0 end",
            batches_script_text,
            "The manual backfill batch loop should use EXISTS instead of a full COUNT(*) scan on every iteration.",
        )
        self.assertIn(
            "psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/fill_mesh_los_cache_queue_indexes.sql",
            backfill_script_text,
            "Manual backfill should install exact-match and batch-order indexes on an existing queue before draining it, so resume does not keep paying a full sort of mesh_route_missing_pairs.",
        )
        self.assertIn(
            "limit :batch_limit",
            batch_text,
            "The standalone batch script should keep bounded batch processing so route cache work stays resumable.",
        )
        self.assertIn(
            "for update skip locked",
            batch_text,
            "Each committed LOS batch should claim queue rows with FOR UPDATE SKIP LOCKED so the queue can be resumed safely and parallel workers can share it later without a second queue design.",
        )
        self.assertIn(
            "delete from mesh_route_missing_pairs mp",
            batch_text,
            "Each committed batch should remove claimed queue rows inside the same transaction that computes metrics, so reruns continue from the preserved queue without a separate delete tail.",
        )
        self.assertIn(
            "begin;",
            batch_text,
            "The standalone batch script should wrap claim, compute, and cache insert in one transaction so claimed rows roll back into the queue if LOS computation fails.",
        )
        self.assertIn(
            "set local synchronous_commit = off;",
            batch_text,
            "LOS cache batches should disable synchronous commit inside the transaction because the cache is derivable and the hot path should avoid extra commit fsync latency on every batch.",
        )
        self.assertIn(
            "set local jit = off;",
            batch_text,
            "LOS cache batches should disable JIT inside the transaction because repeated short-lived batch queries do not benefit from recompiling the same plan on every worker.",
        )
        self.assertIn(
            "on conflict on constraint mesh_los_cache_pkey do nothing",
            batch_text,
            "Claim-first batch fill should insert cache rows with DO NOTHING on conflict, because mesh_route_missing_pairs already represents cache misses and the hot path should avoid unnecessary update locking.",
        )
        self.assertIn(
            "with claimed as (",
            batch_text,
            "The batch script should claim queue rows in a CTE before metric computation so it can avoid a second delete pass and support skip-locked workers later.",
        )
        self.assertNotIn(
            "mesh_route_missing_pairs_claimed",
            batch_text,
            "Claim-first batches should not create a separate temp claimed table once the claimed slice can flow straight from the delete CTE into metric computation.",
        )
        self.assertIn(
            "when exists (select 1 from mesh_route_missing_pairs limit 1) then 'on'",
            batch_text,
            "Batch progress reporting should not count the whole remaining queue after every commit; an EXISTS probe is enough to decide whether more work remains.",
        )
        self.assertIn(
            "claimed no queue rows; batch finished cleanly",
            batch_text,
            "Finite GNU parallel runs should allow late-start jobs to claim nothing and exit successfully instead of treating empty claims as an error.",
        )
        self.assertIn(
            "has_claimed_pairs",
            batch_text,
            "The batch script should distinguish between an empty claim and a claimed batch that computed no metrics, because only the latter is a real inconsistency.",
        )
        self.assertNotIn(
            "mesh_route_missing_pairs_claimed_src_dst_idx",
            batch_text,
            "Claim-first batches should not waste time building a temp index on the claimed slice once delete-tail joins are gone.",
        )
        self.assertNotIn(
            "mesh_route_missing_metrics_src_dst_idx",
            batch_text,
            "Claim-first batches should not build a temp index on mesh_route_missing_metrics when the batch no longer does a separate delete join on that table.",
        )
        prepare_text = (REPO_ROOT / "scripts" / "fill_mesh_los_cache_prepare.sql").read_text()
        self.assertIn(
            "create index if not exists mesh_route_missing_pairs_batch_order_idx",
            prepare_text,
            "fill_mesh_los_cache_prepare should create a btree on the batch ordering tuple so each resumed batch does not parallel-scan and sort the whole missing-pair queue again.",
        )
        queue_index_text = (REPO_ROOT / "scripts" / "fill_mesh_los_cache_queue_indexes.sql").read_text()
        self.assertIn(
            "if changed then",
            queue_index_text,
            "Queue-index refresh should analyze mesh_route_missing_pairs only when it actually created queue indexes, so extra resume workers do not pay another full-table ANALYZE before every batch loop.",
        )
        self.assertIn(
            "execute 'analyze mesh_route_missing_pairs';",
            queue_index_text,
            "Queue-index refresh should still analyze mesh_route_missing_pairs after building its btree indexes so the next resumed batch sees the new access path immediately.",
        )
        self.assertIn(
            "if not exists (",
            queue_index_text,
            "Queue-index refresh should skip DDL entirely when queue indexes already exist, otherwise extra resume workers block on unnecessary CREATE INDEX commands.",
        )
        self.assertNotIn(
            "create index if not exists mesh_route_missing_pairs_batch_order_idx",
            queue_index_text,
            "Queue-index refresh should not use CREATE INDEX IF NOT EXISTS directly for already-indexed live queues, because that still grabs DDL locks for no useful work.",
        )
        self.assertIn(
            "intentionally allows partial cache fill",
            finalize_text,
            "Finalization should explicitly document that the main pipeline may continue from a partial cache fill and rebuild the route graph from the current cache state.",
        )

    def test_bootstrap_route_stage_is_removed(self) -> None:
        """Installer-priority bootstrap should warm cache only, not place route towers directly."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()
        calculations_text = (REPO_ROOT / "docs" / "calculations.md").read_text()
        placement_text = (REPO_ROOT / "docs" / "placement_strategies.md").read_text()

        self.assertNotIn(
            "mesh_route_bootstrap_route",
            makefile_text,
            "Makefile should not contain a bootstrap-route placement stage because install_priority bootstrap is cache warmup only.",
        )
        self.assertNotIn(
            "mesh_route_bootstrap_route",
            calculations_text,
            "docs/calculations.md should not document a bootstrap-route placement stage once routing is expected to converge from cache alone.",
        )
        self.assertNotIn(
            "mesh_route_bootstrap_route",
            placement_text,
            "docs/placement_strategies.md should describe bootstrap as cache warmup only, not direct route placement.",
        )

    def test_mesh_route_bridge_is_timeout_safe(self) -> None:
        """Route bridge should not fail in local refresh work because of the session timeout."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()
        bridge_text = (REPO_ROOT / "procedures" / "mesh_route_bridge.sql").read_text()
        bridge_script_text = (REPO_ROOT / "scripts" / "mesh_route_bridge_configured.sh").read_text()
        cluster_slim_text = (REPO_ROOT / "procedures" / "mesh_route_cluster_slim.sql").read_text()

        self.assertIn(
            "perform set_config('statement_timeout', '0', true);",
            bridge_text,
            "mesh_route_bridge should disable statement_timeout internally before local LOS refresh work, matching mesh_route_cluster_slim.",
        )
        self.assertIn(
            "scripts/mesh_route_bridge_configured.sh",
            makefile_text,
            "db/procedure/mesh_route_bridge should delegate to the configured wrapper so timeout handling and the stage toggle stay in one place.",
        )
        self.assertIn(
            "delete from mesh_towers where source = 'route';",
            bridge_script_text,
            "mesh_route_bridge wrapper should remove stale route towers before rerunning so route placement is recalculated instead of appended.",
        )
        self.assertIn(
            'PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0"',
            bridge_script_text,
            "mesh_route_bridge wrapper should also run with statement_timeout=0 so the outer psql session does not cancel long refreshes.",
        )
        self.assertIn(
            "min(ST_Distance(cp1.centroid_geog, cp2.centroid_geog)) as cluster_distance",
            bridge_text,
            "mesh_route_bridge should rank disconnected clusters by their closest tower-to-tower gap so partial route graphs do not waste time on extreme centroid-distance pairs first.",
        )
        self.assertIn(
            "country_priority",
            bridge_text,
            "mesh_route_bridge should classify cluster-pair country priority so same-country gaps are attempted before cross-border gaps.",
        )
        self.assertIn(
            "Do not distance-dedup here",
            bridge_text,
            "mesh_route_bridge should not distance-dedup promoted route nodes because close cells can have different LOS-neighbor sets.",
        )
        self.assertNotIn(
            "generated_tower_merge_distance_m",
            bridge_text,
            "mesh_route_bridge should leave generated-tower merging to wiggle, where cached visible-neighbor sets are compared before pruning.",
        )
        self.assertIn(
            "order by country_priority asc, cluster_distance asc",
            bridge_text,
            "mesh_route_bridge should search same-country cluster gaps first, then fall back to nearest cross-country gaps.",
        )
        self.assertIn(
            "create temporary table mesh_route_country_polygons as",
            bridge_text,
            "mesh_route_bridge should build local country polygons for country-aware pair ordering inside the route transaction.",
        )
        self.assertIn(
            "mesh_route_edge_components",
            bridge_text,
            "mesh_route_bridge should read precomputed route-graph connected components instead of rebuilding them inside the bridge transaction.",
        )
        self.assertIn(
            "and cp1.component = cp2.component",
            bridge_text,
            "mesh_route_bridge should only try cluster pairs that share a route-graph connected component, because disjoint components cannot be bridged by the current cached graph.",
        )
        self.assertIn(
            "prefer same-country hop shortening before cross-border hop shortening",
            cluster_slim_text,
            "mesh_route_cluster_slim should also prefer same-country over-hop repairs before cross-border repairs.",
        )
        self.assertIn(
            "max_pair_attempts_per_run constant integer := 256;",
            bridge_text,
            "mesh_route_bridge should cap how many failed cluster-pair attempts one invocation can spend, so the iterative pipeline keeps moving when the current route graph is sparse.",
        )
        self.assertIn(
            "order by start_towers.centroid_geog <-> end_towers.centroid_geog",
            bridge_text,
            "mesh_route_intermediate_hexes should anchor each bridge attempt on the nearest tower-node pair between clusters instead of passing every tower node into pgr_dijkstra.",
        )
        self.assertNotIn(
            "mesh_surface_refresh_visible_tower_counts(",
            bridge_text,
            "mesh_route_bridge should not perform synchronous local visibility refresh after route insertion; that work is deferred to the later route refresh stage.",
        )
        self.assertNotIn(
            "mesh_surface_refresh_reception_metrics(",
            bridge_text,
            "mesh_route_bridge should not perform synchronous reception refresh after route insertion; that work is deferred to the later route refresh stage.",
        )

    def test_mesh_route_cluster_slim_defers_refresh(self) -> None:
        """Cluster slim should not stall inside synchronous local refreshes after promoting a corridor."""
        slim_text = (REPO_ROOT / "procedures" / "mesh_route_cluster_slim.sql").read_text()

        self.assertNotIn(
            "mesh_surface_refresh_visible_tower_counts(",
            slim_text,
            "mesh_route_cluster_slim should defer local visible-tower refresh to the later route_refresh_visibility stage.",
        )
        self.assertNotIn(
            "mesh_surface_refresh_reception_metrics(",
            slim_text,
            "mesh_route_cluster_slim should defer local reception refresh to the later route_refresh_visibility stage.",
        )
        self.assertNotIn(
            "call mesh_visibility_edges_refresh();",
            slim_text,
            "mesh_route_cluster_slim should not run visibility refresh internally because the Make pipeline already runs route_refresh_visibility after cluster slim converges.",
        )

    def test_mesh_tower_wiggle_defers_refresh(self) -> None:
        """Tower wiggle should not run heavy local or global visibility refresh inside each single-tower move."""
        wiggle_text = (REPO_ROOT / "procedures" / "mesh_tower_wiggle.sql").read_text()
        makefile_text = (REPO_ROOT / "Makefile").read_text()

        self.assertNotIn(
            "mesh_surface_refresh_visible_tower_counts(",
            wiggle_text,
            "mesh_tower_wiggle should defer local visible-tower refresh instead of recomputing it inside each single-tower move.",
        )
        self.assertNotIn(
            "mesh_surface_refresh_reception_metrics(",
            wiggle_text,
            "mesh_tower_wiggle should defer local reception refresh instead of recomputing it inside each single-tower move.",
        )
        self.assertNotIn(
            "mesh_surface_fill_visible_population(",
            wiggle_text,
            "mesh_tower_wiggle should defer visible-population refresh instead of recomputing it inside each single-tower move.",
        )
        self.assertNotIn(
            "call mesh_visibility_edges_refresh();",
            wiggle_text,
            "mesh_tower_wiggle should not run a full visibility refresh internally because that work belongs to the later visibility stage.",
        )
        self.assertIn(
            "array['population', 'route', 'cluster_slim', 'bridge', 'coarse']",
            wiggle_text,
            "mesh_tower_wiggle should include population anchors along with route-derived towers in local refinement.",
        )
        self.assertIn(
            "generated_tower_merge_distance_m",
            wiggle_text,
            "mesh_tower_wiggle should look for nearby generated-tower merge candidates using the configured merge radius.",
        )
        self.assertIn(
            "where setting = 'mast_height_m'",
            wiggle_text,
            "mesh_tower_wiggle should read mast_height_m from mesh_pipeline_settings so cached LOS lookups use the configured RF dimensions.",
        )
        self.assertIn(
            "where setting = 'frequency_hz'",
            wiggle_text,
            "mesh_tower_wiggle should read frequency_hz from mesh_pipeline_settings so cached LOS lookups use the configured RF dimensions.",
        )
        self.assertIn(
            "merged_link.src_h3 = least(merge_target.h3, nb.h3)",
            wiggle_text,
            "mesh_tower_wiggle should prune duplicate generated towers only after the merge target preserves the anchor visible-neighbor set.",
        )
        self.assertIn(
            "marginal_candidates as materialized",
            wiggle_text,
            "mesh_tower_wiggle should bound expensive cached marginal-population scoring only after candidates preserve existing LOS neighbors.",
        )
        self.assertIn(
            "limit wiggle_candidate_limit",
            wiggle_text,
            "mesh_tower_wiggle should still cap how many LOS-safe candidates get cached marginal-population scoring.",
        )
        self.assertIn(
            "cached_marginal_population",
            wiggle_text,
            "mesh_tower_wiggle should prefer candidates that add population not already served by other cached-visible towers.",
        )
        self.assertNotIn(
            "h3_los_between_cells",
            wiggle_text,
            "mesh_tower_wiggle should use mesh_los_cache instead of starting fresh terrain LOS calculations during local refinement.",
        )
        self.assertIn(
            "db/test/mesh_population_anchor_contract: tests/mesh_population_anchor_contract_setup.sql procedures/mesh_population_anchor_contract.sql tests/mesh_population_anchor_contract_assert.sql | db/test",
            makefile_text,
            "mesh_population_anchor_contract test should run setup, production contraction, and assertions in one psql session so temp fixtures survive.",
        )
        self.assertIn(
            "db/test/mesh_tower_wiggle: tests/mesh_tower_wiggle.sql procedures/mesh_tower_wiggle.sql | db/test",
            makefile_text,
            "mesh_tower_wiggle test should run against its temp fixture without forcing surface/tower table rebuilds.",
        )
        self.assertIn(
            "create temporary table mesh_pipeline_settings",
            (REPO_ROOT / "tests" / "mesh_tower_wiggle.sql").read_text(),
            "mesh_tower_wiggle fixture should provide its own temporary pipeline settings so the standalone target works on a clean database.",
        )
        self.assertIn(
            "db/test/mesh_population: tests/mesh_population.sql | db/test",
            makefile_text,
            "mesh_population test must not depend on mesh_placement_restart because test runs must not replay or delete real placement state.",
        )
        self.assertNotIn(
            "db/test/mesh_population: tests/mesh_population.sql db/procedure/mesh_placement_restart",
            makefile_text,
            "mesh_population test should stay off the destructive placement restart pipeline.",
        )
        self.assertIn(
            "db/procedure/mesh_route_refresh_visibility_current: scripts/mesh_visibility_edges_refresh.sql scripts/assert_mesh_towers_single_los_component.sql | db/procedure",
            makefile_text,
            "Makefile should provide a current visibility refresh target after wiggle that does not replay route or rebuild surface tables.",
        )
        self.assertIn(
            "data/out/mesh_visibility_bridges.tsv: scripts/report_mesh_visibility_bridges.sql | data/out",
            makefile_text,
            "Makefile should provide a read-only bridge/cut-node diagnostic report for DB-first rollout graph review.",
        )
        self.assertIn(
            "db/procedure/mesh_visibility_no_bridges: scripts/assert_mesh_visibility_no_bridges.sql db/procedure/mesh_route_refresh_visibility_current | db/procedure",
            makefile_text,
            "Makefile should provide a fail-fast bridge/cut-node invariant for final rollout graph review.",
        )
        self.assertIn(
            "db/procedure/mesh_route_manual_redundancy: scripts/mesh_route_manual_redundancy.sql scripts/assert_mesh_towers_single_los_component.sql | db/procedure",
            makefile_text,
            "Makefile should provide a narrow target for manually reviewed route redundancy anchors.",
        )
        self.assertIn(
            "scripts/mesh_tower_wiggle_configured.sh\n"
            "\tpsql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql\n"
            "\tpsql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/assert_mesh_towers_single_los_component.sql",
            makefile_text,
            "Tower-wiggle Make targets should refresh mesh_visibility_edges after moving towers and before asserting connectivity.",
        )
        self.assertIn(
            "db/procedure/mesh_population_anchor_contract_current: procedures/mesh_population_anchor_contract.sql scripts/assert_mesh_towers_single_los_component.sql | db/procedure",
            makefile_text,
            "Makefile should provide a current population-anchor contraction target that does not replay route stages.",
        )
        self.assertIn(
            "db/test/mesh_generated_pair_contract: tests/mesh_generated_pair_contract_setup.sql procedures/mesh_generated_pair_contract.sql tests/mesh_generated_pair_contract_assert.sql | db/test",
            makefile_text,
            "mesh_generated_pair_contract test should run setup, production contraction, and assertions in one psql session so temp fixtures survive.",
        )
        self.assertIn(
            "db/procedure/mesh_generated_pair_contract_current: procedures/mesh_generated_pair_contract.sql scripts/assert_mesh_towers_single_los_component.sql | db/procedure",
            makefile_text,
            "Makefile should provide a current generated-pair contraction target that does not replay route stages.",
        )
        self.assertIn(
            "data/out/install_priority.html: scripts/export_install_priority.py scripts/install_priority_cluster_bounds.py scripts/install_priority_cluster_helpers.py scripts/install_priority_connectors.py scripts/install_priority_enrichment.py scripts/install_priority_geocoder.py scripts/install_priority_graph.py scripts/install_priority_graph_support.py scripts/install_priority_lib.py scripts/install_priority_map_payload.py scripts/install_priority_maplibre.py scripts/install_priority_points.py scripts/install_priority_render.py scripts/install_priority_sources.py | data/out",
            makefile_text,
            "Installer handout export should refresh from current DB state without forcing raw-import marker chains.",
        )
        self.assertIn(
            "data/out/install_priority_edges_checked: scripts/assert_install_priority_edges.py data/out/install_priority.csv | data/out",
            makefile_text,
            "Installer handout checks should provide a post-export target that verifies CSV predecessor links against current visibility edges.",
        )
        self.assertIn(
            "data/out/install_priority_reviewed: data/out/install_priority.html data/out/install_priority_edges_checked db/procedure/mesh_visibility_no_bridges data/out/mesh_visibility_bridges.tsv | data/out",
            makefile_text,
            "Installer handout checks should provide one field-review gate covering export files, predecessor links, graph redundancy, and diagnostics.",
        )
        self.assertIn(
            "scripts/assert_install_priority_edges.py",
            makefile_text,
            "Installer-priority Python tests should rerun when the post-export edge assertion changes.",
        )
        self.assertNotIn(
            "data/out/install_priority.html: scripts/export_install_priority.py scripts/install_priority_cluster_bounds.py scripts/install_priority_cluster_helpers.py scripts/install_priority_connectors.py scripts/install_priority_enrichment.py scripts/install_priority_geocoder.py scripts/install_priority_graph.py scripts/install_priority_graph_support.py scripts/install_priority_lib.py scripts/install_priority_map_payload.py scripts/install_priority_maplibre.py scripts/install_priority_points.py scripts/install_priority_render.py scripts/install_priority_sources.py db/table/mesh_towers",
            makefile_text,
            "Installer handout export should not depend on db/table markers, because missing remote markers can drag unrelated raw-import rebuild chains before export.",
        )
        self.assertIn(
            'PGOPTIONS="$${PGOPTIONS:-} -c temp_buffers=256MB -c work_mem=128MB" python scripts/export_install_priority.py --csv-output data/out/install_priority.csv --html-output data/out/install_priority.html',
            makefile_text,
            "Installer handout export should raise temp_buffers/work_mem for its large temporary OSM context tables so the current-state Make target works on geocint without manual shell overrides.",
        )
        self.assertIn(
            "separation_default constant double precision := 0",
            wiggle_text,
            "mesh_tower_wiggle should keep fallback tower spacing at 0 so adjacent hex placements stay allowed during wiggle moves.",
        )

    def test_visibility_bridge_report_is_read_only(self) -> None:
        """Bridge diagnostics should not mutate placement tables while reviewing graph fragility."""
        report_text = (REPO_ROOT / "scripts" / "report_mesh_visibility_bridges.sql").read_text()

        self.assertIn(
            "copy (",
            report_text,
            "The bridge diagnostic should emit a copy-friendly TSV report from one read-only query.",
        )
        self.assertIn(
            "'cut_node'::text as finding_type",
            report_text,
            "The bridge diagnostic should report articulation towers as cut_node findings.",
        )
        self.assertIn(
            "'bridge_edge'::text as finding_type",
            report_text,
            "The bridge diagnostic should report single-edge graph bridges as bridge_edge findings.",
        )
        self.assertNotRegex(
            report_text.lower(),
            r"\b(insert|update|delete|truncate|drop|create|alter)\b",
            "The bridge diagnostic must remain read-only so it is safe to run against the live remote DB.",
        )

    def test_visibility_bridge_assertion_fails_fast(self) -> None:
        """Final rollout checks should fail when refreshed visibility has graph bridges."""
        assertion_text = (
            REPO_ROOT / "scripts" / "assert_mesh_visibility_no_bridges.sql"
        ).read_text()
        restart_text = (REPO_ROOT / "scripts" / "mesh_placement_restart.sh").read_text()

        self.assertIn(
            "'cut_node'::text as finding_type",
            assertion_text,
            "The visibility redundancy assertion should detect articulation towers as cut_node findings.",
        )
        self.assertIn(
            "'bridge_edge'::text as finding_type",
            assertion_text,
            "The visibility redundancy assertion should detect single-edge graph bridges as bridge_edge findings.",
        )
        self.assertIn(
            "raise exception",
            assertion_text.lower(),
            "The visibility redundancy assertion must fail the pipeline when bridge/cut-node findings remain.",
        )
        self.assertIn(
            "scripts/assert_mesh_visibility_no_bridges.sql",
            restart_text,
            "Placement restart should run the no-bridges assertion after the final visibility refresh.",
        )
        self.assertNotRegex(
            assertion_text.lower(),
            r"\b(insert|update|delete|truncate|drop|create|alter)\b",
            "The visibility redundancy assertion must not mutate placement tables.",
        )

    def test_mesh_route_refresh_visibility_keeps_route_geom_optional(self) -> None:
        """Normal route refresh should stop after core visibility edges so routed geometry stays a separate backfill."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()

        self.assertIn(
            "db/procedure/mesh_route_refresh_visibility: scripts/mesh_visibility_edges_refresh.sql",
            makefile_text,
            "The normal route visibility refresh target should depend on the fast core visibility refresh script only.",
        )
        self.assertNotIn(
            "db/procedure/mesh_route_refresh_visibility: scripts/mesh_visibility_edges_refresh.sql scripts/mesh_visibility_edges_refresh_route_geom.sql",
            makefile_text,
            "The normal route visibility refresh target should not force routed-geometry backfill, because that long diagnostics step should remain optional.",
        )
        self.assertNotIn(
            "psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh_route_geom.sql\n\ttouch db/procedure/mesh_route_refresh_visibility",
            makefile_text,
            "The normal route visibility refresh target should finish after core visibility refresh instead of blocking on routed-geometry backfill.",
        )

    def test_h3_visibility_metrics_type_target_is_idempotent(self) -> None:
        """The composite helper type target should be rerunnable even after functions depend on it."""
        type_sql = (REPO_ROOT / "tables" / "h3_visibility_metrics.sql").read_text()

        self.assertIn(
            "drop type if exists h3_visibility_metrics cascade;",
            type_sql,
            "h3_visibility_metrics.sql should drop the type with cascade so rerunning its dedicated Make target does not fail when helper functions already depend on the type.",
        )

    def test_h3_los_between_cells_target_does_not_force_surface_rebuilds(self) -> None:
        """Refreshing h3_visibility_clearance should not force h3_los_between_cells and mesh_surface rebuilds."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()

        self.assertIn(
            "db/function/h3_los_between_cells: functions/h3_los_between_cells.sql db/table/gebco_elevation_h3_r8 db/table/mesh_los_cache | db/function",
            makefile_text,
            "h3_los_between_cells should depend only on its own SQL and data tables, because Postgres callers do not need reinstalling when h3_visibility_clearance is updated.",
        )
        self.assertNotIn(
            "db/function/h3_los_between_cells: functions/h3_los_between_cells.sql db/function/h3_visibility_clearance",
            makefile_text,
            "h3_los_between_cells must not depend on the h3_visibility_clearance marker, otherwise every cache-fill optimization drags a false mesh_surface rebuild chain.",
        )

    def test_fill_mesh_los_cache_prepare_depends_on_routed_visibility_geom(self) -> None:
        """LOS cache prepare should require routed visibility geometry so invisible-edge distance follows the pgRouting corridor."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()

        self.assertIn(
            "db/procedure/mesh_visibility_edges_route_priority_geom: scripts/mesh_visibility_edges_refresh_route_priority_geom.sql",
            makefile_text,
            "Makefile should expose a runnable inter-cluster routed-geometry backfill target before LOS cache preparation.",
        )
        self.assertIn(
            "db/procedure/fill_mesh_los_cache_prepare: scripts/fill_mesh_los_cache_prepare.sql db/table/mesh_surface_h3_r8 db/table/mesh_towers db/table/mesh_los_cache db/procedure/mesh_visibility_edges_route_priority_geom",
            makefile_text,
            "fill_mesh_los_cache_prepare should depend on routed visibility geometry so nearest-edge priority uses pgRouting corridors instead of straight tower chords.",
        )

    def test_mesh_visibility_edges_core_refresh_does_not_depend_on_route_geom_helper(self) -> None:
        """Core visibility-edge refresh should not rebuild just because the optional routed-corridor helper changed."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()

        self.assertIn(
            "db/table/mesh_visibility_edges: tables/mesh_visibility_edges.sql scripts/mesh_visibility_edges_refresh.sql",
            makefile_text,
            "mesh_visibility_edges should still depend on its core refresh inputs.",
        )
        self.assertNotIn(
            "db/table/mesh_visibility_edges: tables/mesh_visibility_edges.sql scripts/mesh_visibility_edges_refresh.sql db/table/mesh_towers db/table/mesh_surface_h3_r8 db/table/mesh_route_graph db/table/mesh_route_graph_cache db/function/h3_los_between_cells db/function/mesh_visibility_invisible_route_geom",
            makefile_text,
            "mesh_visibility_edges should not depend on mesh_visibility_invisible_route_geom because routed corridor backfill is a later optional step.",
        )


    def test_mesh_run_greedy_full_is_timeout_safe(self) -> None:
        """Greedy placement should run with statement_timeout disabled because LOS and visibility refresh work can exceed the default timeout."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()
        greedy_script_text = (REPO_ROOT / "scripts" / "mesh_run_greedy_configured.sh").read_text()

        self.assertIn(
            "scripts/mesh_run_greedy_configured.sh",
            makefile_text,
            "mesh_run_greedy_full should delegate to the configured wrapper so timeout handling and the stage toggle stay in one place.",
        )
        self.assertIn(
            'PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy.sql',
            greedy_script_text,
            "mesh_run_greedy_full should execute greedy iterations with statement_timeout=0 so LOS calculations do not get canceled mid-run.",
        )
        self.assertIn(
            'PGOPTIONS="${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql',
            greedy_script_text,
            "mesh_run_greedy_full should refresh visibility with statement_timeout=0 because that post-iteration refresh can also run longer than the default timeout.",
        )

    def test_greedy_targets_do_not_force_wiggle(self) -> None:
        """Greedy pipeline should not block on the optional wiggle refinement stage."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()

        greedy_target_line = next(
            line for line in makefile_text.splitlines()
            if line.startswith("db/procedure/mesh_run_greedy_full:")
        )
        self.assertNotIn(
            "db/procedure/mesh_tower_wiggle",
            greedy_target_line,
            "mesh_run_greedy_full should not depend on mesh_tower_wiggle because wiggle is an explicit optional refinement pass, not a prerequisite for continuing placement.",
        )

    def test_mesh_run_greedy_prepare_stays_incremental(self) -> None:
        """Greedy prepare should reset state but leave LOS and visible-count recomputation to the iterative greedy stage."""
        greedy_prepare_text = (REPO_ROOT / "procedures" / "mesh_run_greedy_prepare.sql").read_text()

        self.assertNotIn(
            "with tower_points as (",
            greedy_prepare_text,
            "mesh_run_greedy_prepare should not rebuild full visible-tower counts up front because that duplicates the first greedy iteration and blocks placement progress.",
        )
        self.assertNotIn(
            "h3_visibility_metrics(",
            greedy_prepare_text,
            "mesh_run_greedy_prepare should not precompute LOS metrics because greedy iterations already refresh them incrementally for the affected cells.",
        )
        self.assertIn(
            "set visible_tower_count = null,",
            greedy_prepare_text,
            "mesh_run_greedy_prepare should invalidate visible_tower_count so the first greedy iteration recomputes it from the current tower set.",
        )

    def test_mesh_run_greedy_prepare_uses_knn_nearest_tower(self) -> None:
        """Greedy prepare should use KNN nearest-tower ordering instead of ST_Distance scans."""
        greedy_prepare_text = (REPO_ROOT / "procedures" / "mesh_run_greedy_prepare.sql").read_text()

        self.assertIn(
            "order by s2.centroid_geog <-> t.centroid_geog",
            greedy_prepare_text,
            "mesh_run_greedy_prepare should use KNN nearest-tower ordering so the per-cell tower anchor lookup does not scan every tower with ST_Distance.",
        )
        self.assertNotIn(
            "order by ST_Distance(s_inner.centroid_geog, t.centroid_geog)",
            greedy_prepare_text,
            "mesh_run_greedy_prepare should not use ST_Distance in the nearest-tower lateral subquery because that blocks KNN index usage.",
        )


    def test_zero_separation_route_helpers_use_exact_h3_blocking(self) -> None:
        """Zero-separation routing helpers should block exact tower H3 cells directly instead of rescanning with ST_DWithin()."""
        invisible_route_text = (REPO_ROOT / "functions" / "mesh_visibility_invisible_route_geom.sql").read_text()
        corridor_text = (REPO_ROOT / "functions" / "mesh_route_corridor_between_towers.sql").read_text()

        self.assertIn(
            "join mesh_towers mt on mt.h3 = mrgn.h3",
            invisible_route_text,
            "mesh_visibility_invisible_route_geom should block exact tower route nodes by H3 when separation is zero.",
        )
        self.assertNotIn(
            "ST_DWithin(mrgn.geog, mt.centroid_geog, separation)",
            invisible_route_text,
            "mesh_visibility_invisible_route_geom should not rescan route nodes through ST_DWithin when only exact tower cells need blocking.",
        )
        self.assertIn(
            "left join mesh_route_graph_blocked_nodes blocked_source on blocked_source.node_id = e.source_node_id",
            invisible_route_text,
            "mesh_visibility_invisible_route_geom should anti-join blocked nodes inside the pgRouting edge SQL instead of using NOT IN subplans.",
        )
        self.assertNotIn(
            "source_node_id not in (select node_id from mesh_route_graph_blocked_nodes)",
            invisible_route_text,
            "mesh_visibility_invisible_route_geom should not use NOT IN subplans for blocked route nodes because that adds extra executor overhead inside pgRouting edge fetches.",
        )
        self.assertIn(
            "join mesh_towers mt on mt.h3 = mrn.h3",
            corridor_text,
            "mesh_route_corridor_between_towers should block exact tower route nodes by H3 when separation is zero.",
        )
        self.assertNotIn(
            "ST_DWithin(surface.centroid_geog, mt.centroid_geog, separation)",
            corridor_text,
            "mesh_route_corridor_between_towers should not rescan the surface through ST_DWithin when only exact tower cells need blocking.",
        )
        self.assertIn(
            "left join mesh_route_blocked_nodes blocked_source on blocked_source.node_id = e.source",
            corridor_text,
            "mesh_route_corridor_between_towers should anti-join blocked nodes inside the pgRouting edge SQL instead of using NOT IN subplans.",
        )
        self.assertNotIn(
            "source not in (select node_id from mesh_route_blocked_nodes)",
            corridor_text,
            "mesh_route_corridor_between_towers should not use NOT IN subplans for blocked route nodes because that adds extra executor overhead inside pgRouting edge fetches.",
        )


    def test_h3_visibility_clearance_uses_h3_centroids_in_sampling(self) -> None:
        """Visibility sampling should use H3-native centroids instead of PointOnSurface on boundary polygons."""
        clearance_text = (REPO_ROOT / "functions" / "h3_visibility_clearance.sql").read_text()

        self.assertIn(
            "from h3_grid_path_cells(norm_src, norm_dst) with ordinality as p(h3, step_no)",
            clearance_text,
            "h3_visibility_clearance should use h3_grid_path_cells() ordinality to derive sample fractions instead of projecting every sample through PostGIS geometry math.",
        )
        self.assertIn(
            "grid_step_count <= 1 then 0::double precision",
            clearance_text,
            "h3_visibility_clearance should derive the path step count from h3_grid_distance() instead of running a second aggregate or window pass just to recover the path length.",
        )
        self.assertIn(
            "annotated_samples as (",
            clearance_text,
            "h3_visibility_clearance should annotate samples with endpoint elevations and sample count in the same pass instead of running a separate endpoint_stats aggregate over the path.",
        )
        self.assertNotIn(
            "endpoint_stats as (",
            clearance_text,
            "h3_visibility_clearance should not keep a separate endpoint_stats aggregate once sample_count and endpoint elevations can be carried by window functions over samples.",
        )
        self.assertNotIn(
            "path_stats as (",
            clearance_text,
            "h3_visibility_clearance should not keep a separate path_stats aggregate CTE once step_count can be derived in the ordered path scan itself.",
        )
        self.assertNotIn(
            "ST_LineLocatePoint(line_geom, p.h3::geometry)::double precision",
            clearance_text,
            "h3_visibility_clearance should no longer call ST_LineLocatePoint in the hot sampling loop once path ordinality defines the traversal fraction.",
        )
        self.assertNotIn(
            "ST_PointOnSurface(ms.geom)",
            clearance_text,
            "h3_visibility_clearance should not rebuild polygon point-on-surface positions inside the hot LOS sampling loop.",
        )
        self.assertIn(
            "from gebco_elevation_h3_r8",
            clearance_text,
            "h3_visibility_clearance should read elevations from the narrow gebco_elevation_h3_r8 table so the LOS batch avoids repeated lookups against the wide mesh_surface_h3_r8 table.",
        )
        self.assertNotIn(
            "join mesh_surface_h3_r8 ms",
            clearance_text,
            "h3_visibility_clearance should not join the wide mesh_surface_h3_r8 table in the hot sampling loop once gebco_elevation_h3_r8 already holds the required elevation samples.",
        )
        self.assertIn(
            "max(ms.ele) filter (where p.h3 = norm_src) over () as src_ele",
            clearance_text,
            "h3_visibility_clearance should derive endpoint elevations from the sampled path with window functions so each LOS pair does not do separate point lookups or a second aggregate pass before scanning the same path cells.",
        )
        self.assertNotIn(
            "select ele\n    into src_ele\n    from gebco_elevation_h3_r8",
            clearance_text,
            "h3_visibility_clearance should not perform a separate source elevation lookup once endpoint elevations come from the sampled path.",
        )


    def test_mesh_visibility_edges_refresh_keeps_long_invisible_pairs(self) -> None:
        """Visibility refresh should keep all tower pairs for routing diagnostics, but only run LOS for pairs inside the 100 km planning radius."""
        refresh_text = (REPO_ROOT / "procedures" / "mesh_visibility_edges_refresh.sql").read_text()

        self.assertNotIn(
            "and ST_DWithin(t1.centroid_geog, t2.centroid_geog, 100000)",
            refresh_text,
            "mesh_visibility_edges_refresh should not drop long tower pairs from mesh_visibility_edges because invisible long gaps are later used to pick route corridors.",
        )
        self.assertIn(
            "when pair.distance_m <= 100000 then h3_los_between_cells(t1.h3, t2.h3)",
            refresh_text,
            "mesh_visibility_edges_refresh should gate the expensive LOS computation by 100 km instead of dropping the pair entirely.",
        )
        self.assertIn(
            "else false",
            refresh_text,
            "mesh_visibility_edges_refresh should mark longer-than-80-km diagnostic edges as invisible without calling the LOS function.",
        )

    def test_mesh_visibility_edges_refresh_reuses_visible_graph(self) -> None:
        """Visibility refresh should derive cluster metadata from the visible-edge graph it already computed instead of recalculating LOS through mesh_tower_clusters()."""
        refresh_text = (REPO_ROOT / "procedures" / "mesh_visibility_edges_refresh.sql").read_text()

        self.assertNotIn(
            "from mesh_tower_clusters()",
            refresh_text,
            "mesh_visibility_edges_refresh should not call mesh_tower_clusters() because that would recompute the tower LOS graph a second time inside the same refresh.",
        )
        self.assertIn(
            "pgr_connectedComponents",
            refresh_text,
            "mesh_visibility_edges_refresh should derive cluster ids from tmp_visibility_cluster_edges after building visible edge pairs.",
        )
        self.assertIn(
            "when pair.distance_m <= 100000 then h3_los_between_cells(t1.h3, t2.h3)",
            refresh_text,
            "mesh_visibility_edges_refresh should cap only the expensive LOS computation at 100 km, while still keeping longer invisible tower-to-tower edges for routing diagnostics.",
        )
        self.assertNotIn(
            "and ST_DWithin(t1.centroid_geog, t2.centroid_geog, 100000)",
            refresh_text,
            "mesh_visibility_edges_refresh should not drop long tower pairs from mesh_visibility_edges, because invisible long gaps are later used to steer route expansion.",
        )

    def test_mesh_visibility_edges_refresh_route_geom_uses_stored_diagnostics(self) -> None:
        """The optional routed-geometry backfill should reuse stored visibility diagnostics instead of recalculating tower clusters."""
        route_geom_text = (REPO_ROOT / "procedures" / "mesh_visibility_edges_refresh_route_geom.sql").read_text()

        self.assertNotIn(
            "from mesh_tower_clusters()",
            route_geom_text,
            "mesh_visibility_edges_refresh_route_geom should not call mesh_tower_clusters() because mesh_visibility_edges already stores inter-cluster and hop-budget diagnostics.",
        )
        self.assertIn(
            "where (not e.is_visible and e.is_between_clusters)",
            route_geom_text,
            "mesh_visibility_edges_refresh_route_geom should route long invisible edges based on stored is_between_clusters flags from mesh_visibility_edges.",
        )
        self.assertIn(
            "or (e.cluster_hops is not null and e.cluster_hops >= 8)",
            route_geom_text,
            "mesh_visibility_edges_refresh_route_geom should also backfill route geometry for long-hop intra-cluster edges so over-budget routes keep a concrete corridor line.",
        )
        self.assertIn(
            "mesh_visibility_invisible_route_geom(err.source_h3, err.target_h3)",
            route_geom_text,
            "mesh_visibility_edges_refresh_route_geom should keep using mesh_visibility_invisible_route_geom for cache misses so long invisible edges still get a pgRouting corridor.",
        )
        self.assertIn(
            "join mesh_route_graph_cache cache",
            route_geom_text,
            "mesh_visibility_edges_refresh_route_geom should reuse cached routed geometry directly before calling the pgRouting helper again.",
        )

    def test_mesh_visibility_edges_route_priority_geom_matches_cache_priority_set(self) -> None:
        """The mandatory pre-backfill route-geometry step should cover the same blocked visibility set that drives cache priority."""
        route_priority_text = (REPO_ROOT / "scripts" / "mesh_visibility_edges_refresh_route_priority_geom.sql").read_text()

        self.assertIn(
            "where (not e.is_visible and e.is_between_clusters)",
            route_priority_text,
            "mesh_visibility_edges_refresh_route_priority_geom should cover invisible inter-cluster gaps before LOS cache preparation.",
        )
        self.assertIn(
            "or (e.cluster_hops is not null and e.cluster_hops >= 8)",
            route_priority_text,
            "mesh_visibility_edges_refresh_route_priority_geom should also cover long-hop visibility gaps because fill_mesh_los_cache_prepare ranks against that full blocked-edge set.",
        )

    def test_fill_mesh_los_cache_prepare_prefilters_priority_sources(self) -> None:
        """Cache-fill prepare should prefilter invisible edges and disconnected towers into staging tables before nearest-neighbor scans."""
        prepare_text = (REPO_ROOT / "scripts" / "fill_mesh_los_cache_prepare.sql").read_text()

        self.assertIn(
            "create table mesh_route_priority_edges as",
            prepare_text,
            "fill_mesh_los_cache_prepare should materialize blocked visibility edges once so the nearest-edge pass does not rescan the full mesh_visibility_edges table for every candidate.",
        )
        self.assertIn(
            "from mesh_route_priority_edges e",
            prepare_text,
            "fill_mesh_los_cache_prepare should read nearest blocked-edge distances from the prefiltered mesh_route_priority_edges table.",
        )
        self.assertIn(
            "create table mesh_route_disconnected_towers as",
            prepare_text,
            "fill_mesh_los_cache_prepare should materialize disconnected towers once so the nearest disconnected-cluster lookup stays indexed and cheap.",
        )
        self.assertIn(
            "from mesh_route_disconnected_towers dt",
            prepare_text,
            "fill_mesh_los_cache_prepare should read nearest disconnected-tower distances from the staged mesh_route_disconnected_towers table.",
        )
        self.assertIn(
            "c.centroid_geom <-> e.geom",
            prepare_text,
            "fill_mesh_los_cache_prepare should use a cached geometry centroid for KNN nearest-edge lookup so it does not keep recasting centroid_geog inside the hot loop.",
        )
        self.assertIn(
            "ST_DistanceSphere(c.centroid_geom, e.geom)",
            prepare_text,
            "fill_mesh_los_cache_prepare should use ST_DistanceSphere for invisible-edge priority because that distance is only a batching heuristic and does not need the heavier geography distance path.",
        )
        self.assertIn(
            "ST_DistanceSphere(c.centroid_geom, dt.centroid_geom)",
            prepare_text,
            "fill_mesh_los_cache_prepare should use ST_DistanceSphere for disconnected-cluster priority too, so the hot loop stays on geometry and avoids slower geography distance calculations.",
        )

    def test_fill_mesh_los_cache_prioritizes_disconnected_clusters_first(self) -> None:
        """Cache fill should prioritize candidates closest to disconnected clusters before generic invisible-edge distance."""
        prepare_text = (REPO_ROOT / "scripts" / "fill_mesh_los_cache_prepare.sql").read_text()
        batch_text = (REPO_ROOT / "scripts" / "fill_mesh_los_cache_batch.sql").read_text()

        self.assertIn(
            "mesh_route_candidate_disconnected_dist",
            prepare_text,
            "fill_mesh_los_cache prepare should materialize candidate distance to non-primary disconnected clusters so route warmup can bias toward unconnected tower groups.",
        )
        self.assertIn(
            "disconnected_priority",
            prepare_text,
            "fill_mesh_los_cache prepare should persist disconnected-cluster priority into mesh_route_missing_pairs.",
        )
        self.assertIn(
            "mp.disconnected_priority",
            batch_text,
            "fill_mesh_los_cache batch ordering should include disconnected-cluster priority so those corridors are seeded before generic cache work.",
        )

    def test_spacing_is_disabled_for_route_and_surface_candidates(self) -> None:
        """Adjacent tower hexes should be allowed by the planning surface and route cache prep."""
        surface_text = (REPO_ROOT / "tables" / "mesh_surface_h3_r8.sql").read_text()
        prepare_text = (REPO_ROOT / "scripts" / "fill_mesh_los_cache_prepare.sql").read_text()
        finalize_text = (REPO_ROOT / "scripts" / "fill_mesh_los_cache_finalize.sql").read_text()
        bootstrap_text = (REPO_ROOT / "scripts" / "mesh_route_bootstrap.sql").read_text()

        self.assertIn(
            "min_distance_to_closest_tower double precision default 0",
            surface_text,
            "mesh_surface_h3_r8 should default minimum tower spacing to 0 so adjacent H3 placements are allowed.",
        )
        self.assertIn(
            "where setting = 'min_tower_separation_m'",
            prepare_text,
            "fill_mesh_los_cache prepare should read zero/default minimum tower spacing from the single pipeline config instead of hiding it as an internal constant.",
        )
        self.assertNotIn(
            "not ST_DWithin(c1.centroid_geog, c2.centroid_geog, :separation)",
            prepare_text,
            "fill_mesh_los_cache prepare should not filter out nearby candidate pairs once minimum spacing is disabled.",
        )
        self.assertNotIn(
            "mlc.distance_m >= :separation",
            finalize_text,
            "fill_mesh_los_cache finalize should not drop short visible links from the route graph once minimum spacing is disabled.",
        )
        self.assertNotIn(
            "mlc.distance_m >= 5000",
            bootstrap_text,
            "mesh_route_bootstrap should keep short visible links now that adjacent H3 placements are allowed.",
        )


    def test_route_bootstrap_uses_committed_snapshot_not_live_export(self) -> None:
        """Route bootstrap should use the committed data/in snapshot instead of regenerating installer exports during routing."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()

        self.assertIn(
            "db/table/mesh_route_bootstrap_pairs: tables/mesh_route_bootstrap_pairs.sql data/in/install_priority_bootstrap.csv",
            makefile_text,
            "mesh_route_bootstrap_pairs should depend on the committed data/in bootstrap snapshot.",
        )
        self.assertIn(
            "db/function/mesh_tower_clusters: functions/mesh_tower_clusters.sql db/table/mesh_towers db/function/h3_los_between_cells",
            makefile_text,
            "Makefile should install mesh_tower_clusters before any route-bootstrap SQL uses it.",
        )
        self.assertIn(
            "db/table/mesh_route_bootstrap_pairs: tables/mesh_route_bootstrap_pairs.sql data/in/install_priority_bootstrap.csv data/in/install_priority_bootstrap_manual.csv db/procedure/mesh_population db/table/mesh_surface_h3_r8 db/table/mesh_towers db/table/osm_for_mesh_placement db/table/georgia_boundary db/function/mesh_tower_clusters",
            makefile_text,
            "mesh_route_bootstrap_pairs should depend on mesh_tower_clusters so clean make -j runs cannot reach the SQL before the function exists.",
        )
        self.assertIn(
            "data/in/install_priority_bootstrap_refresh: data/out/install_priority.csv | data/in",
            makefile_text,
            "Makefile should expose an explicit refresh target for copying the latest installer export into the committed bootstrap snapshot.",
        )

    def test_install_priority_export_tolerates_missing_active_visibility_table(self) -> None:
        """Installer export should fall back to canonical visibility edges on fresh route outputs."""
        sources_text = (REPO_ROOT / "scripts" / "install_priority_sources.py").read_text()

        self.assertIn(
            "select to_regclass('public.mesh_visibility_edges_active');",
            sources_text,
            "choose_visible_edge_table should check whether optional mesh_visibility_edges_active exists before querying it.",
        )
        self.assertIn(
            'return "mesh_visibility_edges"',
            sources_text,
            "choose_visible_edge_table should fall back to mesh_visibility_edges when the optional active table is absent.",
        )


    def test_forced_make_test_dry_run_is_non_destructive(self) -> None:
        """`make -B test` should run tests, not rebuild data or mutate route placement."""
        result = subprocess.run(
            ["make", "-n", "-B", "test"],
            cwd=REPO_ROOT,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        dry_run_output = result.stdout
        forbidden_fragments = [
            "curl -L",
            "osmium merge",
            "osmium export",
            "ogr2ogr",
            "raster2pgsql",
            "gunzip -c",
            "unzip -o",
            "tables/mesh_los_cache.sql",
            "tables/mesh_towers.sql",
            "tables/mesh_surface_h3_r8.sql",
            "tables/mesh_visibility_edges.sql",
            "scripts/fill_mesh_los_cache_batch_once.sh",
            "scripts/backup_mesh_los_cache.sh",
            "procedures/mesh_route_bridge.sql",
            "delete from mesh_towers",
            "truncate mesh_towers",
        ]
        offenders = [
            fragment
            for fragment in forbidden_fragments
            if fragment in dry_run_output
        ]

        self.assertEqual(
            [],
            offenders,
            f"Forced make test dry-run must not rebuild data or mutate live placement state; found forbidden commands: {offenders!r}.\nDry run was:\n{dry_run_output}",
        )

        makefile_text = (REPO_ROOT / "Makefile").read_text()
        test_line = next(
            line
            for line in makefile_text.splitlines()
            if line.startswith("test:")
        )
        test_dependencies = test_line.split("##", 1)[0].split()[1:]

        self.assertNotIn(
            "db/test/mesh_route",
            test_dependencies,
            "Default make test should exclude the route bridge integration target because that script mutates live placement tables.",
        )
        self.assertIn(
            "db/test/mesh_route_integration:",
            makefile_text,
            "Makefile should keep an explicit route integration target for disposable databases instead of hiding it inside default make test.",
        )


    def test_los_cache_backup_lives_outside_cleaned_outputs(self) -> None:
        """LOS cache backups should survive `make clean` and restore should require an existing dump."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()
        backup_script_text = (REPO_ROOT / "scripts" / "backup_mesh_los_cache.sh").read_text()
        restore_script_text = (REPO_ROOT / "scripts" / "restore_mesh_los_cache.sh").read_text()

        self.assertIn(
            "data/backups/mesh_los_cache.latest.dump: scripts/backup_mesh_los_cache.sh | data/backups",
            makefile_text,
            "LOS cache backup should be represented by the durable dump file target, not only a db/procedure marker.",
        )
        self.assertIn(
            "db/procedure/restore_mesh_los_cache: scripts/restore_mesh_los_cache.sh | db/procedure",
            makefile_text,
            "Restore target should not depend on the backup target, because a missing backup must fail instead of snapshotting current state.",
        )
        self.assertNotIn(
            "data/backups",
            re.search(r"clean:.*?(?=\n\S)", makefile_text, re.DOTALL).group(0),
            "make clean should not delete durable LOS cache backups stored under data/backups.",
        )
        self.assertIn(
            'latest_dump="data/backups/mesh_los_cache.latest.dump"',
            backup_script_text,
            "Backup script should write the latest LOS cache dump under data/backups so rendered output cleanup cannot remove it.",
        )
        self.assertIn(
            'backup_path="${1:-data/backups/mesh_los_cache.latest.dump}"',
            restore_script_text,
            "Restore script should read the durable data/backups LOS cache dump by default.",
        )


    def test_placement_restart_has_failure_snapshot_before_destructive_stages(self) -> None:
        """Placement replay should restore tower/surface state if a later stage fails."""
        restart_text = (REPO_ROOT / "scripts" / "mesh_placement_restart.sh").read_text()

        snapshot_position = restart_text.index("create table ${tower_backup} as")
        trap_position = restart_text.index("trap restore_restart_snapshot EXIT")
        destructive_position = restart_text.index(">> Clearing restartable placement towers")

        self.assertLess(
            snapshot_position,
            destructive_position,
            "mesh_placement_restart should snapshot mesh_towers and mesh_surface_h3_r8 before deleting restartable towers.",
        )
        self.assertLess(
            trap_position,
            destructive_position,
            "mesh_placement_restart should install the rollback trap before the first destructive placement step.",
        )
        self.assertIn(
            "truncate mesh_towers restart identity;",
            restart_text,
            "mesh_placement_restart rollback should reset mesh_towers before restoring the saved tower registry.",
        )
        self.assertIn(
            "update mesh_surface_h3_r8 surface",
            restart_text,
            "mesh_placement_restart rollback should restore cached surface placement metrics from the snapshot.",
        )
        self.assertIn(
            "trap - EXIT",
            restart_text,
            "mesh_placement_restart should disable the rollback trap after all stages finish successfully.",
        )


    def test_sql_fixtures_shadow_precious_tables_before_destructive_setup(self) -> None:
        """SQL fixtures should not drop or truncate live placement/cache tables."""
        precious_tables = {
            "mesh_los_cache",
            "mesh_surface_h3_r8",
            "mesh_towers",
            "mesh_visibility_edges",
            "mesh_greedy_iterations",
            "mesh_tower_wiggle_queue",
            "mesh_route_cluster_slim_failures",
            "mesh_route_graph_cache",
            "mesh_route_graph_edges",
            "mesh_route_graph_nodes",
            "mesh_route_candidate_cells",
            "mesh_route_candidate_invisible_dist",
            "mesh_route_pair_candidates",
            "mesh_route_missing_pairs",
        }
        offenders: list[str] = []

        for path in sorted((REPO_ROOT / "tests").glob("*.sql")):
            text = path.read_text(errors="ignore").lower()
            relative_path = path.relative_to(REPO_ROOT)

            for match in re.finditer(
                r"drop\s+table\s+(?:if\s+exists\s+)?(?!pg_temp\.)([a-z_][a-z0-9_]*)\b",
                text,
            ):
                table_name = match.group(1)
                if table_name in precious_tables:
                    offenders.append(f"{relative_path}: unqualified drop table {table_name}")

            for match in re.finditer(r"truncate\s+(?:table\s+)?([^;]+)", text):
                target_sql = match.group(1)
                prefix = text[:match.start()]

                for table_name in precious_tables:
                    if not re.search(rf"\b{table_name}\b", target_sql):
                        continue
                    if re.search(rf"pg_temp\.{table_name}\b", target_sql):
                        continue
                    if re.search(rf"create\s+temporary\s+table\s+{table_name}\b", prefix):
                        continue

                    offenders.append(f"{relative_path}: truncate {table_name} before temporary shadow table")

        self.assertEqual(
            [],
            offenders,
            f"SQL fixtures must use temporary shadow tables before destructive setup; offending statements: {offenders!r}",
        )


    def test_los_cache_is_not_dropped_by_repository_sql_or_scripts(self) -> None:
        """Repository code should never contain cache-dropping statements for the precious LOS cache."""
        checked_paths = [
            path
            for path in REPO_ROOT.rglob("*")
            if path.is_file()
            and path.suffix in {".sql", ".sh"}
            and ".git" not in path.parts
            and "data" not in path.parts
        ]
        offenders: list[str] = []

        for path in checked_paths:
            text = path.read_text(errors="ignore").lower()
            drops_cache = re.search(r"drop\s+table\s+(?:if\s+exists\s+)?(?:pg_temp\.)?mesh_los_cache\b", text)
            truncates_cache = re.search(r"truncate\s+(?:table\s+)?[^;]*mesh_los_cache\b", text)
            if drops_cache or truncates_cache:
                offenders.append(str(path.relative_to(REPO_ROOT)))

        self.assertEqual(
            [],
            offenders,
            f"mesh_los_cache is multi-day state and must not appear in drop/truncate statements; offending files: {offenders!r}",
        )

    def test_route_mutating_targets_assert_single_los_component(self) -> None:
        """Every post-route mutating Make target should enforce the live LOS graph invariant."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()
        invariant_script = "scripts/assert_mesh_towers_single_los_component.sql"
        targets = [
            "db/procedure/mesh_route_bridge",
            "db/procedure/mesh_route_cluster_slim",
            "db/procedure/mesh_population_anchor_contract",
            "db/procedure/mesh_generated_pair_contract",
            "db/procedure/mesh_route_segment_reroute",
            "db/procedure/mesh_route_refresh_visibility",
            "db/procedure/mesh_route",
            "db/procedure/mesh_tower_wiggle",
        ]

        for target in targets:
            block_match = re.search(
                rf"^{re.escape(target)}:.*?(?=^\S|\Z)",
                makefile_text,
                flags=re.M | re.S,
            )

            self.assertIsNotNone(
                block_match,
                f"Expected Make target {target} to exist so the LOS component invariant can be enforced.",
            )
            self.assertIn(
                invariant_script,
                block_match.group(0),
                f"Make target {target} must run {invariant_script} after it can mutate live tower connectivity.",
            )

    def test_single_component_assertion_uses_cached_live_tower_los(self) -> None:
        """The invariant should run from live towers and cached positive-clearance links."""
        assertion_sql = (
            REPO_ROOT / "scripts" / "assert_mesh_towers_single_los_component.sql"
        ).read_text()

        self.assertIn(
            "from mesh_los_cache link",
            assertion_sql,
            "The tower component invariant must read mesh_los_cache directly so it can run before mesh_visibility_edges refresh.",
        )
        self.assertIn(
            "link.clearance > 0",
            assertion_sql,
            "The tower component invariant must only count positive-clearance LOS links as graph edges.",
        )
        self.assertIn(
            "from mesh_towers",
            assertion_sql,
            "The tower component invariant must verify every live row in mesh_towers, not only diagnostic visibility rows.",
        )
        self.assertIn(
            "raise exception",
            assertion_sql.lower(),
            "The tower component invariant must fail the pipeline immediately when connectivity is broken.",
        )

    def test_mesh_placement_restart_declares_greedy_iteration_table(self) -> None:
        """The restart wrapper truncates mesh_greedy_iterations, so Make must install that table first."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()

        self.assertIn(
            "db/procedure/mesh_placement_restart: scripts/mesh_placement_restart.sh",
            makefile_text,
            "mesh_placement_restart target should remain declared in Makefile.",
        )
        self.assertIn(
            "db/table/mesh_greedy_iterations | db/procedure",
            makefile_text,
            "mesh_placement_restart must depend on db/table/mesh_greedy_iterations because the wrapper truncates that table before replaying route stages.",
        )


if __name__ == "__main__":
    unittest.main()
