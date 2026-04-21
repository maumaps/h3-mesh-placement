"""Shared helpers for PostgreSQL connection setup."""

from __future__ import annotations

import argparse
import os


def pg_conn_kwargs(
    dbname: str = "",
    host: str = "",
    port: int = 5432,
    user: str = "",
    password: str = "",
) -> dict:
    """Return psycopg2.connect kwargs, dropping empty strings so libpq defaults apply."""
    kwargs: dict = {
        "dbname": dbname,
        "host": host,
        "port": port,
        "user": user,
        "password": password,
    }
    for key in ["dbname", "host", "user", "password"]:
        if not kwargs[key]:
            kwargs.pop(key)
    return kwargs


def add_db_args(parser: argparse.ArgumentParser) -> None:
    """Add standard PostgreSQL connection arguments to an argument parser."""
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE", ""))
    parser.add_argument("--host", default=os.getenv("PGHOST", ""))
    parser.add_argument("--port", default=int(os.getenv("PGPORT", "5432")))
    parser.add_argument("--user", default=os.getenv("PGUSER", ""))
    parser.add_argument("--password", default=os.getenv("PGPASSWORD", ""))
