#!/usr/bin/env python3
"""Validate Makefile marker files against database objects."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable

import psycopg2


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for marker checks."""

    parser = argparse.ArgumentParser(
        description="Check db/* marker files against database objects."
    )
    parser.add_argument("--fix", action="store_true", help="Remove stale markers.")
    return parser.parse_args()


def connect_db() -> psycopg2.extensions.connection:
    """Open a database connection using environment variables."""

    connection_kwargs = {
        "dbname": os.getenv("PGDATABASE", ""),
        "host": os.getenv("PGHOST", ""),
        "port": int(os.getenv("PGPORT", "5432")),
        "user": os.getenv("PGUSER", ""),
        "password": os.getenv("PGPASSWORD", ""),
    }
    if not connection_kwargs["dbname"]:
        connection_kwargs.pop("dbname")
    if not connection_kwargs["host"]:
        connection_kwargs.pop("host")
    if not connection_kwargs["user"]:
        connection_kwargs.pop("user")
    if not connection_kwargs["password"]:
        connection_kwargs.pop("password")

    return psycopg2.connect(**connection_kwargs)


def list_markers(directory: Path) -> list[str]:
    """List marker filenames in a directory."""

    if not directory.exists():
        return []
    return sorted(entry.name for entry in directory.iterdir() if entry.is_file())


def fetch_existing_tables(conn: psycopg2.extensions.connection) -> set[str]:
    """Fetch existing table names."""

    query = """
        select relname
        from pg_class
        where relkind = 'r';
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return {row[0] for row in cur.fetchall()}


def fetch_extensions(conn: psycopg2.extensions.connection) -> set[str]:
    """Fetch installed Postgres extensions."""

    query = """
        select extname
        from pg_extension;
    """
    with conn.cursor() as cur:
        cur.execute(query)
        return {row[0] for row in cur.fetchall()}


def fetch_existing_routines(
    conn: psycopg2.extensions.connection,
    prokind: str,
    names: Iterable[str],
) -> set[str]:
    """Fetch existing routine names by kind."""

    name_list = list(names)
    if not name_list:
        return set()

    placeholders = ",".join(["%s"] * len(name_list))
    query = f"""
        select proname
        from pg_proc
        where prokind = %s
          and proname in ({placeholders});
    """
    with conn.cursor() as cur:
        cur.execute(query, [prokind, *name_list])
        return {row[0] for row in cur.fetchall()}


def check_markers() -> int:
    """Validate marker files and optionally remove stale ones."""

    args = parse_args()
    root = Path("db")
    table_markers = list_markers(root / "table")
    function_markers = list_markers(root / "function")
    procedure_markers = list_markers(root / "procedure")
    raw_markers = list_markers(root / "raw")

    raw_table_map = {
        "initial_nodes": "mesh_initial_nodes",
        "kontur_population": "kontur_population",
        "gebco_elevation": "gebco_elevation",
    }
    extension_marker_map = {
        "postgis_extension": {"postgis", "h3", "hstore", "pgrouting"},
    }
    stage_markers = {
        "fill_mesh_los_cache",
        "mesh_route",
        "mesh_route_bridge",
        "mesh_route_cache_graph",
        "mesh_route_cluster_slim",
        "mesh_route_refresh_visibility",
        "mesh_run_greedy",
        "mesh_tower_wiggle",
    }

    print("Checking marker files against database objects.")
    print(f"Found {len(table_markers)} table markers.")
    print(f"Found {len(function_markers)} function markers.")
    print(f"Found {len(procedure_markers)} procedure markers.")
    print(f"Found {len(raw_markers)} raw markers.")

    conn = connect_db()
    conn.autocommit = True
    stale_markers: list[Path] = []

    try:
        existing_tables = fetch_existing_tables(conn)
        existing_extensions = fetch_extensions(conn)
        existing_functions = fetch_existing_routines(conn, "f", function_markers)
        procedure_candidates = [name for name in procedure_markers if name not in stage_markers]
        existing_procedures = fetch_existing_routines(conn, "p", procedure_candidates)

        for marker in table_markers:
            if marker in extension_marker_map:
                required = extension_marker_map[marker]
                if not required.issubset(existing_extensions):
                    stale_markers.append(root / "table" / marker)
                continue
            if marker not in existing_tables:
                stale_markers.append(root / "table" / marker)

        for marker in function_markers:
            if marker not in existing_functions:
                stale_markers.append(root / "function" / marker)

        for marker in procedure_candidates:
            if marker not in existing_procedures:
                stale_markers.append(root / "procedure" / marker)

        for marker in raw_markers:
            table_name = raw_table_map.get(marker)
            if not table_name:
                continue
            if table_name not in existing_tables:
                stale_markers.append(root / "raw" / marker)
    finally:
        conn.close()

    if stale_markers:
        print("Stale markers detected:")
        for marker in stale_markers:
            print(f"- {marker}")
        if args.fix:
            for marker in stale_markers:
                marker.unlink(missing_ok=True)
            print(f"Removed {len(stale_markers)} stale markers.")
        return 1

    print("All markers match database objects.")
    return 0


if __name__ == "__main__":
    raise SystemExit(check_markers())
