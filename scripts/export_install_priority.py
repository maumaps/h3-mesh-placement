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
from typing import Any

# Ensure the repo root is in sys.path so `scripts.*` imports work when this
# script is invoked directly (e.g. `python scripts/export_install_priority.py`).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    import psycopg2
except ImportError as exc:
    raise SystemExit("psycopg2 is required for database access.") from exc

from scripts.pg_connect import add_db_args, pg_conn_kwargs
from scripts.install_priority_cluster_bounds import fetch_cluster_bound_features
from scripts.install_priority_connectors import select_inter_cluster_connectors
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
    build_adjacency,
    build_cluster_plan,
    format_display_label,
    render_html_document,
)
from scripts.install_priority_sources import (
    build_tower_records,
    choose_visible_edge_table,
    fetch_seed_points,
    fetch_tower_metadata,
    fetch_tower_points,
    fetch_visible_edges,
    match_seed_names,
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


def choose_primary_previous_tower_id(
    plan_row,
    rank_by_tower_id: dict[int, int | None],
) -> int | str:
    """Pick the previous node that best represents the rollout corridor on the map."""

    if not plan_row.previous_connection_ids:
        return ""

    def predecessor_score(tower_id: int) -> tuple[int, int]:
        rank = rank_by_tower_id.get(tower_id)
        rank_value = -1 if rank is None else rank

        return (rank_value, -tower_id)

    return max(
        plan_row.previous_connection_ids,
        key=predecessor_score,
    )


def build_inter_cluster_metadata(
    *,
    plan_rows,
    towers_by_id,
    adjacency,
) -> dict[int, dict[str, str]]:
    """Attach canonical inter-cluster connector summaries to connector endpoints."""

    connector_metadata: dict[int, dict[str, list[str] | list[int]]] = {}
    connectors = select_inter_cluster_connectors(
        plan_rows=plan_rows,
        adjacency=adjacency,
    )

    for connector in connectors:
        left_summary = (
            f"{connector.right_cluster_label} via "
            f"{format_display_label(towers_by_id[connector.right_tower_id])}"
        )
        right_summary = (
            f"{connector.left_cluster_label} via "
            f"{format_display_label(towers_by_id[connector.left_tower_id])}"
        )
        connector_pairs = [
            (connector.left_tower_id, connector.right_tower_id, left_summary),
            (connector.right_tower_id, connector.left_tower_id, right_summary),
        ]

        for tower_id, neighbor_id, summary in connector_pairs:
            row_metadata = connector_metadata.setdefault(
                tower_id,
                {
                    "neighbor_ids": [],
                    "summaries": [],
                },
            )
            row_metadata["neighbor_ids"].append(neighbor_id)
            row_metadata["summaries"].append(summary)

    return {
        tower_id: {
            "inter_cluster_neighbor_ids": ",".join(
                str(neighbor_id)
                for neighbor_id in sorted(row_metadata["neighbor_ids"])
            ),
            "inter_cluster_connections": " | ".join(
                sorted(row_metadata["summaries"])
            ),
        }
        for tower_id, row_metadata in connector_metadata.items()
    }


def build_neighbor_cluster_summaries(
    *,
    plan_rows,
    towers_by_id,
    adjacency,
) -> dict[int, list[str]]:
    """Describe every visible neighboring cluster that touches a tower."""

    row_by_tower_id = {
        plan_row.tower_id: plan_row
        for plan_row in plan_rows
    }
    summaries_by_tower_id: dict[int, list[str]] = {}

    for tower_id, plan_row in row_by_tower_id.items():
        cluster_summaries = {
            (
                row_by_tower_id[neighbor_id].cluster_label,
                format_display_label(towers_by_id[neighbor_id]),
            )
            for neighbor_id in adjacency.get(tower_id, {})
            if (
                neighbor_id in row_by_tower_id
                and row_by_tower_id[neighbor_id].cluster_key != plan_row.cluster_key
            )
        }
        summaries_by_tower_id[tower_id] = [
            f"{cluster_label} via {neighbor_label}"
            for cluster_label, neighbor_label in sorted(cluster_summaries)
        ]

    return summaries_by_tower_id


def export_rows(
    cursor,
    args: argparse.Namespace,
) -> tuple[
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    """Build the final flat rows for CSV and HTML outputs."""

    print(">> Loading tower registry and visibility graph", file=sys.stderr)
    tower_points = fetch_tower_points(cursor)
    tower_metadata = fetch_tower_metadata(cursor)
    visible_edge_table = choose_visible_edge_table(
        cursor=cursor,
        tower_count=len(tower_metadata),
    )
    visible_edges = fetch_visible_edges(cursor, visible_edge_table)
    seed_tower_ids = [
        tower_id for tower_id, source, _ in tower_metadata if source == "seed"
    ]
    seed_points = fetch_seed_points(cursor)
    seed_name_by_tower_id = match_seed_names(
        cursor=cursor,
        seed_tower_ids=seed_tower_ids,
        tower_points=tower_points,
        seed_points=seed_points,
    )
    towers_by_id = build_tower_records(
        tower_metadata=tower_metadata,
        tower_points=tower_points,
        seed_name_by_tower_id=seed_name_by_tower_id,
    )

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
        visible_edge_table=visible_edge_table,
    )
    adjacency = build_adjacency(visible_edges)
    plan_rows = build_cluster_plan(
        towers_by_id=towers_by_id,
        adjacency=adjacency,
    )
    connector_metadata_by_tower_id = build_inter_cluster_metadata(
        plan_rows=plan_rows,
        towers_by_id=towers_by_id,
        adjacency=adjacency,
    )
    neighbor_cluster_summaries_by_tower_id = build_neighbor_cluster_summaries(
        plan_rows=plan_rows,
        towers_by_id=towers_by_id,
        adjacency=adjacency,
    )
    print(
        f">> Loaded {len(towers_by_id)} towers and {len(visible_edges)} visible edges from {visible_edge_table}",
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
    rank_by_tower_id = {
        plan_row.tower_id: plan_row.cluster_install_rank
        for plan_row in plan_rows
    }

    for plan_row in plan_rows:
        local_context = local_context_by_tower_id[plan_row.tower_id]
        geocoder_key_en = (round(plan_row.lon, 6), round(plan_row.lat, 6), "en")
        geocoder_key_ru = (round(plan_row.lon, 6), round(plan_row.lat, 6), "ru")
        geocoder_payload_en, geocoder_status_en = geocoder_results[geocoder_key_en]
        geocoder_payload_ru, geocoder_status_ru = geocoder_results[geocoder_key_ru]
        admin_context_en = extract_admin_context(geocoder_payload_en)
        admin_context_ru = extract_admin_context(geocoder_payload_ru)

        tower = towers_by_id[plan_row.tower_id]
        connector_metadata = connector_metadata_by_tower_id.get(
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
        visible_neighbor_cluster_summaries = neighbor_cluster_summaries_by_tower_id.get(
            plan_row.tower_id,
            [],
        )
        blocked_reason = ""

        if plan_row.rollout_status == "blocked":
            if connector_summaries:
                blocked_reason = (
                    "No visible path from an installed seed yet. "
                    "Cheapest cluster connector: "
                    f"{summarize_connector_summaries(connector_summaries)}."
                )
            elif visible_neighbor_cluster_summaries:
                blocked_reason = (
                    "No visible path from an installed seed yet. "
                    f"Visible into {summarize_connector_summaries(visible_neighbor_cluster_summaries)}."
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
                "primary_previous_tower_id": choose_primary_previous_tower_id(
                    plan_row,
                    rank_by_tower_id,
                ),
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

    return final_rows, cluster_bound_features, seed_mqtt_points, seed_mqtt_links


def main() -> None:
    """Export the installer-priority handout files."""

    args = parse_args()
    csv_output = Path(args.csv_output)
    html_output = Path(args.html_output)

    with open_connection(args) as connection:
        with connection.cursor() as cursor:
            rows, cluster_bound_features, seed_mqtt_points, seed_mqtt_links = export_rows(cursor, args)

    write_csv(rows, csv_output)
    write_html(
        rows,
        html_output,
        args.geocoder_base_url,
        cluster_bound_features,
        seed_mqtt_points,
        seed_mqtt_links,
    )
    print(f">> Wrote {len(rows)} rows to {csv_output}", file=sys.stderr)
    print(f">> Wrote {len(rows)} rows to {html_output}", file=sys.stderr)


if __name__ == "__main__":
    main()
