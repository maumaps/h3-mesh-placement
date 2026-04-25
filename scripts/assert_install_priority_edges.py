#!/usr/bin/env python3
"""Verify that installer-priority predecessor links exist in visibility edges."""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

# Keep direct script execution working from the repository root or scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

try:
    import psycopg2
except ImportError as exc:
    raise SystemExit("psycopg2 is required for database access.") from exc

from scripts.pg_connect import add_db_args, pg_conn_kwargs


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the CSV/DB consistency check."""

    parser = argparse.ArgumentParser(
        description="Validate installer-priority predecessor links against mesh_visibility_edges."
    )
    add_db_args(parser)
    parser.add_argument("--csv-input", default="data/out/install_priority.csv")

    return parser.parse_args()


def read_install_priority_rows(csv_path: Path) -> list[dict[str, str]]:
    """Read installer-priority CSV rows and validate required columns."""

    with csv_path.open(encoding="utf-8", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        fieldnames = set(reader.fieldnames or [])
        rows = list(reader)

    missing_columns = {
        column
        for column in (
            "tower_id",
            "cluster_key",
            "cluster_install_rank",
            "primary_previous_tower_id",
        )
        if column not in fieldnames
    }
    if missing_columns:
        raise ValueError(
            f"{csv_path} is missing required column(s): {', '.join(sorted(missing_columns))}"
        )

    return rows


def read_primary_previous_pairs(csv_path: Path) -> list[tuple[int, int]]:
    """Read planned tower predecessor pairs from an installer-priority CSV."""

    return primary_previous_pairs_from_rows(
        read_install_priority_rows(csv_path),
        str(csv_path),
    )


def primary_previous_pairs_from_rows(
    rows: list[dict[str, str]],
    csv_label: str,
) -> list[tuple[int, int]]:
    """Read planned tower predecessor pairs from parsed installer-priority rows."""

    pairs: list[tuple[int, int]] = []
    for row_number, row in enumerate(rows, start=2):
        previous_id = str(row.get("primary_previous_tower_id") or "").strip()
        if not previous_id:
            continue

        try:
            pairs.append((int(row["tower_id"]), int(previous_id)))
        except ValueError as exc:
            raise ValueError(
                f"{csv_label}:{row_number} has non-integer tower/predecessor ids: {row!r}"
            ) from exc

    return pairs


def invalid_primary_previous_order(rows: list[dict[str, str]]) -> list[str]:
    """Return predecessor references that are absent, cross-cluster, or not earlier."""

    rows_by_tower_id: dict[int, dict[str, str]] = {}
    invalid_references: list[str] = []
    for row_number, row in enumerate(rows, start=2):
        try:
            tower_id = int(row["tower_id"])
            int(row["cluster_install_rank"])
        except ValueError:
            invalid_references.append(f"line {row_number}: invalid tower_id/rank in {row!r}")
            continue

        rows_by_tower_id[tower_id] = row

    for row_number, row in enumerate(rows, start=2):
        previous_id = str(row.get("primary_previous_tower_id") or "").strip()
        if not previous_id:
            try:
                tower_rank = int(row["cluster_install_rank"])
            except ValueError:
                invalid_references.append(f"line {row_number}: invalid rank in {row!r}")
                continue

            rollout_status = str(row.get("rollout_status") or "").strip()
            installed = str(row.get("installed") or "").strip().lower()
            if tower_rank > 0 and rollout_status != "blocked" and installed != "true":
                invalid_references.append(
                    f"line {row_number}: rank {tower_rank} row {row.get('tower_id')} "
                    "has no predecessor"
                )
            continue

        try:
            tower_id = int(row["tower_id"])
            tower_rank = int(row["cluster_install_rank"])
            previous_tower_id = int(previous_id)
        except ValueError:
            invalid_references.append(
                f"line {row_number}: invalid predecessor id/rank in {row!r}"
            )
            continue

        previous_row = rows_by_tower_id.get(previous_tower_id)
        if previous_row is None:
            invalid_references.append(
                f"{tower_id}->{previous_tower_id}: predecessor is absent from CSV"
            )
            continue

        if previous_row["cluster_key"] != row["cluster_key"]:
            invalid_references.append(
                f"{tower_id}->{previous_tower_id}: predecessor is in cluster "
                f"{previous_row['cluster_key']!r}, row is in {row['cluster_key']!r}"
            )
            continue

        previous_rank = int(previous_row["cluster_install_rank"])
        installed = str(row.get("installed") or "").strip().lower()
        if tower_rank == 0 and installed == "true":
            continue

        if previous_rank >= tower_rank:
            invalid_references.append(
                f"{tower_id}->{previous_tower_id}: predecessor rank {previous_rank} "
                f"is not before row rank {tower_rank}"
            )

    return invalid_references


def fetch_visible_edge_pairs(cursor) -> set[tuple[int, int]]:
    """Fetch undirected visible tower-edge pairs from the current database."""

    cursor.execute(
        """
        select
            source_id,
            target_id
        from mesh_visibility_edges
        where is_visible
        """
    )

    return {
        tuple(sorted((int(source_id), int(target_id))))
        for source_id, target_id in cursor.fetchall()
    }


def missing_primary_previous_edges(
    primary_previous_pairs: list[tuple[int, int]],
    visible_edge_pairs: set[tuple[int, int]],
) -> list[tuple[int, int]]:
    """Return CSV predecessor pairs that are absent from visible graph edges."""

    return [
        (tower_id, previous_id)
        for tower_id, previous_id in primary_previous_pairs
        if tuple(sorted((tower_id, previous_id))) not in visible_edge_pairs
    ]


def open_connection(args: argparse.Namespace):
    """Open a PostgreSQL connection using the standard repo DB flags."""

    return psycopg2.connect(
        **pg_conn_kwargs(
            dbname=args.dbname,
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
        )
    )


def main() -> int:
    """Run the CSV predecessor visibility check."""

    args = parse_args()
    csv_path = Path(args.csv_input)
    rows = read_install_priority_rows(csv_path)
    primary_previous_pairs = primary_previous_pairs_from_rows(rows, str(csv_path))

    invalid_order = invalid_primary_previous_order(rows)
    if invalid_order:
        formatted = "; ".join(invalid_order)
        raise SystemExit(
            f"Installer-priority predecessor invariant failed: invalid predecessor order: {formatted}"
        )

    with open_connection(args) as connection:
        with connection.cursor() as cursor:
            visible_edge_pairs = fetch_visible_edge_pairs(cursor)

    missing_edges = missing_primary_previous_edges(
        primary_previous_pairs,
        visible_edge_pairs,
    )
    if missing_edges:
        formatted = ", ".join(
            f"{tower_id}->{previous_id}" for tower_id, previous_id in missing_edges
        )
        raise SystemExit(
            f"Installer-priority predecessor invariant failed: missing visible edge(s) {formatted}"
        )

    print(
        "Installer-priority predecessor invariant holds: "
        f"{len(primary_previous_pairs)} predecessor link(s) all visible"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
