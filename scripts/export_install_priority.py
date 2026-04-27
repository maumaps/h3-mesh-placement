#!/usr/bin/env python3
"""
Export the installer-priority handout as CSV and HTML.

The exporter is intentionally defensive about live database drift:
it prefers direct tower geometries when they exist and falls back to
reconstructing points from visibility-edge endpoints when they do not.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure the repo root is in sys.path so `scripts.*` imports work when this
# script is invoked directly (e.g. `python scripts/export_install_priority.py`).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    import psycopg2
except ImportError as exc:
    raise SystemExit("psycopg2 is required for database access.") from exc

from scripts.pg_connect import add_db_args, pg_conn_kwargs
from scripts.install_priority_cluster_bounds import fetch_cluster_bound_features
from scripts.install_priority_enrichment import (
    build_output_row,
    enrich_tower_records,
    fetch_reachable_seed_mqtt_overview,
    fetch_local_context,
    prepare_context_tables,
)
from scripts.install_priority_geocoder import extract_admin_context, fetch_geocoder_batch
from scripts.install_priority_lib import (
    CSV_COLUMNS,
    PlanRow,
    TowerRecord,
    render_html_document,
)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the export."""

    parser = argparse.ArgumentParser(
        description="Export the installer-priority handout."
    )
    add_db_args(parser)
    parser.add_argument("--csv-output", default="data/out/install_priority.csv")
    parser.add_argument("--html-output", default="data/out/install_priority.html")
    parser.add_argument(
        "--geocoder-base-url",
        default="https://geocoder.batu.market",
    )
    parser.add_argument("--geocoder-radius-m", type=int, default=2000)
    parser.add_argument("--geocoder-timeout-s", type=int, default=10)

    return parser.parse_args()


def open_connection(args: argparse.Namespace):
    """Open a Postgres connection using libpq-style defaults."""

    connection = psycopg2.connect(
        **pg_conn_kwargs(
            dbname=args.dbname,
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
        )
    )
    connection.autocommit = True

    return connection


def write_csv(rows: list[dict[str, object]], output_path: Path) -> None:
    """Write the flat CSV output."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_html(
    rows: list[dict[str, object]],
    output_path: Path,
    geocoder_base_url: str,
    cluster_bound_features: list[dict[str, object]],
    seed_mqtt_points: list[dict[str, object]],
    seed_mqtt_links: list[dict[str, object]],
    phase_one_tower_ids: list[int],
) -> None:
    """Write the mobile-friendly HTML output."""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    html_text = render_html_document(
        rows=rows,
        generated_at=datetime.now(timezone.utc).isoformat(),
        geocoder_base_url=geocoder_base_url,
        cluster_bound_features=cluster_bound_features,
        seed_mqtt_points=seed_mqtt_points,
        seed_mqtt_links=seed_mqtt_links,
        phase_one_tower_ids=phase_one_tower_ids,
    )
    output_path.write_text(html_text, encoding="utf-8")


def summarize_connector_summaries(
    summaries: list[str],
    *,
    max_items: int = 1,
) -> str:
    """Keep connector explanations compact while preserving the full CSV list."""

    if len(summaries) <= max_items:
        return "; ".join(summaries)

    remaining_count = len(summaries) - max_items

    return f"{'; '.join(summaries[:max_items])} and {remaining_count} more"


def fetch_ready_plan(
    cursor,
) -> tuple[list[PlanRow], dict[int, TowerRecord], dict[int, dict[str, object]]]:
    """Read the precomputed SQL installer plan."""

    cursor.execute("select to_regclass('mesh_install_priority_plan');")
    if cursor.fetchone()[0] is None:
        raise RuntimeError(
            "mesh_install_priority_plan is missing; run make db/table/mesh_install_priority_plan_current before exporting."
        )

    query = """
        select
            tower_id,
            cluster_key,
            cluster_label,
            cluster_install_rank,
            install_phase,
            is_next_for_cluster,
            rollout_status,
            installed,
            source,
            label,
            lon,
            lat,
            impact_score,
            impact_people_est,
            impact_tower_count,
            next_unlock_count,
            backlink_count,
            primary_previous_tower_id,
            previous_connection_ids,
            next_connection_ids,
            inter_cluster_neighbor_ids,
            inter_cluster_connections
        from mesh_install_priority_plan
        order by
            cluster_label,
            cluster_install_rank nulls last,
            tower_id;
    """
    cursor.execute(query)

    plan_rows: list[PlanRow] = []
    towers_by_id: dict[int, TowerRecord] = {}
    metadata_by_tower_id: dict[int, dict[str, object]] = {}

    for row in cursor.fetchall():
        (
            tower_id,
            cluster_key,
            cluster_label,
            cluster_install_rank,
            install_phase,
            is_next_for_cluster,
            rollout_status,
            installed,
            source,
            label,
            lon,
            lat,
            impact_score,
            impact_people_est,
            impact_tower_count,
            next_unlock_count,
            backlink_count,
            primary_previous_tower_id,
            previous_connection_ids,
            next_connection_ids,
            inter_cluster_neighbor_ids,
            inter_cluster_connections,
        ) = row
        previous_ids = tuple(int(item) for item in (previous_connection_ids or []))
        next_ids = tuple(int(item) for item in (next_connection_ids or []))
        tower_id = int(tower_id)
        installed_bool = bool(installed)

        plan_rows.append(
            PlanRow(
                cluster_key=str(cluster_key),
                cluster_label=str(cluster_label),
                cluster_install_rank=(
                    None if cluster_install_rank is None else int(cluster_install_rank)
                ),
                is_next_for_cluster=bool(is_next_for_cluster),
                rollout_status=str(rollout_status),
                installed=installed_bool,
                tower_id=tower_id,
                label=str(label),
                source=str(source),
                impact_score=int(impact_score or impact_people_est or 0),
                impact_tower_count=int(impact_tower_count or 0),
                next_unlock_count=int(next_unlock_count or 0),
                backlink_count=int(backlink_count or 0),
                previous_connection_ids=previous_ids,
                next_connection_ids=next_ids,
                lon=float(lon),
                lat=float(lat),
            )
        )
        towers_by_id[tower_id] = TowerRecord(
            tower_id=tower_id,
            source=str(source),
            lon=float(lon),
            lat=float(lat),
            label=str(label),
            installed=installed_bool,
        )
        metadata_by_tower_id[tower_id] = {
            "install_phase": str(install_phase),
            "impact_people_est": int(impact_people_est or impact_score or 0),
            "primary_previous_tower_id": (
                "" if primary_previous_tower_id is None else int(primary_previous_tower_id)
            ),
            "inter_cluster_neighbor_ids": ",".join(
                str(int(item)) for item in (inter_cluster_neighbor_ids or [])
            ),
            "inter_cluster_connections": str(inter_cluster_connections or ""),
        }

    return plan_rows, towers_by_id, metadata_by_tower_id


def export_rows(
    cursor,
    args: argparse.Namespace,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[int],
]:
    """Build the final flat rows for CSV and HTML outputs."""

    print(">> Loading precomputed installer plan", file=sys.stderr)
    plan_rows, towers_by_id, metadata_by_tower_id = fetch_ready_plan(cursor)

    print(">> Preparing local road and terrain context tables", file=sys.stderr)
    prepare_context_tables(cursor)
    local_context_by_tower_id = {
        tower_id: fetch_local_context(cursor, tower.lon, tower.lat)
        for tower_id, tower in towers_by_id.items()
    }
    towers_by_id = enrich_tower_records(
        towers_by_id=towers_by_id,
        local_context_by_tower_id=local_context_by_tower_id,
    )
    seed_mqtt_points, seed_mqtt_links = fetch_reachable_seed_mqtt_overview(
        cursor=cursor,
        visible_edge_table="mesh_visibility_edges",
    )
    print(
        f">> Loaded {len(towers_by_id)} precomputed installer plan rows",
        file=sys.stderr,
    )

    print(">> Fetching bilingual reverse-geocoder context", file=sys.stderr)
    geocoder_results = fetch_geocoder_batch(
        plan_rows=plan_rows,
        geocoder_base_url=args.geocoder_base_url,
        radius_m=args.geocoder_radius_m,
        timeout_s=args.geocoder_timeout_s,
    )

    print(">> Assembling final installer handout rows", file=sys.stderr)
    final_rows: list[dict[str, object]] = []

    for plan_row in plan_rows:
        local_context = local_context_by_tower_id[plan_row.tower_id]
        geocoder_key_en = (round(plan_row.lon, 6), round(plan_row.lat, 6), "en")
        geocoder_key_ru = (round(plan_row.lon, 6), round(plan_row.lat, 6), "ru")
        geocoder_payload_en, geocoder_status_en = geocoder_results[geocoder_key_en]
        geocoder_payload_ru, geocoder_status_ru = geocoder_results[geocoder_key_ru]
        admin_context_en = extract_admin_context(geocoder_payload_en)
        admin_context_ru = extract_admin_context(geocoder_payload_ru)

        tower = towers_by_id[plan_row.tower_id]
        connector_metadata = metadata_by_tower_id.get(
            plan_row.tower_id,
            {
                "inter_cluster_neighbor_ids": "",
                "inter_cluster_connections": "",
            },
        )
        connector_summaries = [
            summary.strip()
            for summary in str(
                connector_metadata["inter_cluster_connections"]
            ).split(" | ")
            if summary.strip()
        ]
        blocked_reason = ""

        if plan_row.rollout_status == "blocked":
            if connector_summaries:
                blocked_reason = (
                    "No visible path from an installed seed yet. "
                    "Earliest cluster connector: "
                    f"{summarize_connector_summaries(connector_summaries)}."
                )
            else:
                blocked_reason = "No visible path from an installed seed yet."

        output_row = build_output_row(
            plan_row=plan_row,
            towers_by_id=towers_by_id,
            local_context=local_context,
            admin_context_en=admin_context_en,
            admin_context_ru=admin_context_ru,
            geocoder_status_en=geocoder_status_en,
            geocoder_status_ru=geocoder_status_ru,
        )
        output_row.update(
            {
                "primary_previous_tower_id": metadata_by_tower_id[plan_row.tower_id][
                    "primary_previous_tower_id"
                ],
                "previous_connection_ids": list(plan_row.previous_connection_ids),
                "inter_cluster_neighbor_ids": connector_metadata[
                    "inter_cluster_neighbor_ids"
                ],
                "inter_cluster_connections": connector_metadata[
                    "inter_cluster_connections"
                ],
                "blocked_reason": blocked_reason,
            }
        )
        final_rows.append(output_row)

    print(">> Building Voronoi cluster bounds in PostGIS", file=sys.stderr)
    cluster_bound_features = fetch_cluster_bound_features(
        cursor,
        final_rows,
    )

    phase_one_tower_ids = [
        int(row["tower_id"])
        for row in final_rows
        if row["rollout_status"] == "installed"
        or metadata_by_tower_id[int(row["tower_id"])]["install_phase"] == "connect"
    ]

    return (
        final_rows,
        cluster_bound_features,
        seed_mqtt_points,
        seed_mqtt_links,
        phase_one_tower_ids,
    )


def main() -> None:
    """Export the installer-priority handout files."""

    args = parse_args()
    csv_output = Path(args.csv_output)
    html_output = Path(args.html_output)

    with open_connection(args) as connection:
        with connection.cursor() as cursor:
            (
                rows,
                cluster_bound_features,
                seed_mqtt_points,
                seed_mqtt_links,
                phase_one_tower_ids,
            ) = export_rows(cursor, args)

    write_csv(rows, csv_output)
    write_html(
        rows,
        html_output,
        args.geocoder_base_url,
        cluster_bound_features,
        seed_mqtt_points,
        seed_mqtt_links,
        phase_one_tower_ids,
    )
    print(f">> Wrote {len(rows)} rows to {csv_output}", file=sys.stderr)
    print(f">> Wrote {len(rows)} rows to {html_output}", file=sys.stderr)


if __name__ == "__main__":
    main()
