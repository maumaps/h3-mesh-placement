"""Regression checks for pipeline wiring and batch-complete routing fill."""

from pathlib import Path
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

    def test_initial_nodes_import_does_not_skip_existing_table(self) -> None:
        """Canonical seed refresh should always reach Postgres on rebuild."""
        makefile_text = (REPO_ROOT / "Makefile").read_text()

        self.assertIn(
            'ogr2ogr -f PostgreSQL "PG:dbname=kom user=kom host=/var/run/postgresql port=5432" data/in/existing_mesh_nodes.geojson -nln mesh_initial_nodes -nlt POINT -lco GEOMETRY_NAME=geom -overwrite -a_srs EPSG:4326',
            makefile_text,
            "db/raw/initial_nodes should reimport the refreshed canonical seed GeoJSON with -overwrite so refreshed Liam Cottle or curated seeds are not skipped.",
        )
        self.assertNotIn(
            "mesh_initial_nodes already exists, skipping import",
            makefile_text,
            "db/raw/initial_nodes must not skip importing just because mesh_initial_nodes already exists, otherwise refreshed seeds never reach Postgres.",
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
            'PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -v batch_limit=50000 -f scripts/fill_mesh_los_cache_batch.sql',
            makefile_text,
            "The single-batch fill target should still run a committed LOS batch so the route pipeline can progress without waiting for a full drain.",
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
            "db/procedure/mesh_route_bridge: procedures/mesh_route_bridge.sql db/procedure/fill_mesh_los_cache_ready",
            makefile_text,
            "mesh_route_bridge should consume the materialized route graph marker without forcing another LOS cache batch when towers are recalculated from a completed cache.",
        )
        self.assertIn(
            "db/procedure/mesh_route_cluster_slim: procedures/mesh_route_cluster_slim.sql db/table/mesh_route_cluster_slim_failures db/procedure/mesh_route_bridge db/procedure/fill_mesh_los_cache_ready",
            makefile_text,
            "mesh_route_cluster_slim should share the same cache-ready marker so downstream tower recalculation does not restart LOS cache preparation.",
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
            "--jobs 8",
            parallel_script_text,
            "The GNU parallel launcher should keep eight concurrent batch jobs for the current eight-core cache-fill workflow.",
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
            "ceil(count(*)::numeric / 50000)::integer from mesh_route_missing_pairs",
            parallel_script_text,
            "The GNU parallel launcher should snapshot the current queue length into a finite number of 50k batch jobs so one run drains a measurable chunk of the queue.",
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
        self.assertIn(
            'while [ "$$(PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -At -c "select case when exists (select 1 from mesh_route_missing_pairs limit 1) then 1 else 0 end")" -eq 1 ]',
            makefile_text,
            "The manual backfill target should loop over committed batch invocations until mesh_route_missing_pairs is empty, using EXISTS instead of a full COUNT(*) scan on every iteration.",
        )
        self.assertIn(
            "psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/fill_mesh_los_cache_queue_indexes.sql; while",
            makefile_text,
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

        self.assertIn(
            "perform set_config('statement_timeout', '0', true);",
            bridge_text,
            "mesh_route_bridge should disable statement_timeout internally before local LOS refresh work, matching mesh_route_cluster_slim.",
        )
        self.assertIn(
            'PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_route_bridge.sql',
            makefile_text,
            "db/procedure/mesh_route_bridge should also run with statement_timeout=0 so the outer psql session does not cancel long refreshes.",
        )
        self.assertIn(
            "min(ST_Distance(cp1.centroid_geog, cp2.centroid_geog)) as cluster_distance",
            bridge_text,
            "mesh_route_bridge should rank disconnected clusters by their closest tower-to-tower gap so partial route graphs do not waste time on extreme centroid-distance pairs first.",
        )
        self.assertIn(
            "order by cluster_distance asc",
            bridge_text,
            "mesh_route_bridge should search the nearest cluster gaps first to improve the chance of early route insertions.",
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
            "separation_default constant double precision := 0",
            wiggle_text,
            "mesh_tower_wiggle should keep fallback tower spacing at 0 so adjacent hex placements stay allowed during wiggle moves.",
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

        self.assertIn(
            'PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f procedures/mesh_run_greedy.sql',
            makefile_text,
            "mesh_run_greedy_full should execute greedy iterations with statement_timeout=0 so LOS calculations do not get canceled mid-run.",
        )
        self.assertIn(
            'PGOPTIONS="$${PGOPTIONS:-} -c statement_timeout=0" psql --no-psqlrc --set=ON_ERROR_STOP=1 -f scripts/mesh_visibility_edges_refresh.sql',
            makefile_text,
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
        """Visibility refresh should keep all tower pairs for routing diagnostics, but only run LOS for pairs inside the 80 km planning radius."""
        refresh_text = (REPO_ROOT / "procedures" / "mesh_visibility_edges_refresh.sql").read_text()

        self.assertNotIn(
            "and ST_DWithin(t1.centroid_geog, t2.centroid_geog, 80000)",
            refresh_text,
            "mesh_visibility_edges_refresh should not drop long tower pairs from mesh_visibility_edges because invisible long gaps are later used to pick route corridors.",
        )
        self.assertIn(
            "when pair.distance_m <= 80000 then h3_los_between_cells(t1.h3, t2.h3)",
            refresh_text,
            "mesh_visibility_edges_refresh should gate the expensive LOS computation by 80 km instead of dropping the pair entirely.",
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
            "when pair.distance_m <= 80000 then h3_los_between_cells(t1.h3, t2.h3)",
            refresh_text,
            "mesh_visibility_edges_refresh should cap only the expensive LOS computation at 80 km, while still keeping longer invisible tower-to-tower edges for routing diagnostics.",
        )
        self.assertNotIn(
            "and ST_DWithin(t1.centroid_geog, t2.centroid_geog, 80000)",
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
            r"\set separation 0",
            prepare_text,
            "fill_mesh_los_cache prepare should not exclude near-adjacent candidate pairs anymore.",
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
            "data/in/install_priority_bootstrap_refresh: data/out/install_priority.csv | data/in",
            makefile_text,
            "Makefile should expose an explicit refresh target for copying the latest installer export into the committed bootstrap snapshot.",
        )

if __name__ == "__main__":
    unittest.main()
