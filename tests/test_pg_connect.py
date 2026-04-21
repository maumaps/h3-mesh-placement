"""Tests for pg_connect shared helpers."""

from __future__ import annotations

import argparse
import os
import unittest
from unittest.mock import patch

from scripts.pg_connect import add_db_args, pg_conn_kwargs


class PgConnKwargsTests(unittest.TestCase):
    """pg_conn_kwargs drops empty strings so libpq defaults apply."""

    def test_all_values_present(self) -> None:
        result = pg_conn_kwargs(
            dbname="mydb", host="localhost", port=5433, user="alice", password="secret"
        )
        self.assertEqual(
            result,
            {"dbname": "mydb", "host": "localhost", "port": 5433, "user": "alice", "password": "secret"},
            "pg_conn_kwargs should preserve every explicit connection option when all values are provided",
        )

    def test_empty_strings_dropped(self) -> None:
        result = pg_conn_kwargs(dbname="", host="", port=5432, user="", password="")
        self.assertEqual(
            result,
            {"port": 5432},
            "pg_conn_kwargs should drop empty libpq string options while keeping the integer port default",
        )

    def test_partial_values(self) -> None:
        result = pg_conn_kwargs(dbname="mydb", host="", port=5432, user="bob", password="")
        self.assertEqual(
            result,
            {"dbname": "mydb", "port": 5432, "user": "bob"},
            "pg_conn_kwargs should keep non-empty partial options and omit empty host/password values",
        )

    def test_port_always_included(self) -> None:
        result = pg_conn_kwargs()
        self.assertIn("port", result, "pg_conn_kwargs should always include a port for psycopg2.connect")
        self.assertEqual(
            result["port"],
            5432,
            "pg_conn_kwargs should use port 5432 when no explicit port is supplied",
        )

    def test_returns_dict(self) -> None:
        result = pg_conn_kwargs(dbname="x")
        self.assertIsInstance(
            result,
            dict,
            "pg_conn_kwargs should return a plain dict suitable for psycopg2.connect keyword expansion",
        )


class AddDbArgsTests(unittest.TestCase):
    """add_db_args populates parser from PG* environment variables."""

    def _make_parser(self) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser()
        add_db_args(parser)
        return parser

    def test_defaults_come_from_env(self) -> None:
        env = {
            "PGDATABASE": "envdb",
            "PGHOST": "envhost",
            "PGPORT": "5435",
            "PGUSER": "envuser",
            "PGPASSWORD": "envpass",
        }
        with patch.dict(os.environ, env, clear=False):
            parser = self._make_parser()
            args = parser.parse_args([])

        self.assertEqual(args.dbname, "envdb", "add_db_args should default --dbname from PGDATABASE")
        self.assertEqual(args.host, "envhost", "add_db_args should default --host from PGHOST")
        self.assertEqual(args.port, 5435, "add_db_args should parse PGPORT as an integer default")
        self.assertEqual(args.user, "envuser", "add_db_args should default --user from PGUSER")
        self.assertEqual(args.password, "envpass", "add_db_args should default --password from PGPASSWORD")

    def test_cli_flags_override_env(self) -> None:
        with patch.dict(os.environ, {"PGDATABASE": "envdb"}, clear=False):
            parser = self._make_parser()
            args = parser.parse_args(["--dbname", "clidb"])
        self.assertEqual(
            args.dbname,
            "clidb",
            "add_db_args should let CLI --dbname override PGDATABASE environment defaults",
        )

    def test_empty_defaults_when_env_unset(self) -> None:
        env_clean = {k: "" for k in ("PGDATABASE", "PGHOST", "PGUSER", "PGPASSWORD")}
        env_clean["PGPORT"] = "5432"
        with patch.dict(os.environ, env_clean, clear=False):
            parser = self._make_parser()
            args = parser.parse_args([])

        self.assertEqual(args.dbname, "", "add_db_args should use an empty dbname when PGDATABASE is unset")
        self.assertEqual(args.host, "", "add_db_args should use an empty host when PGHOST is unset")
        self.assertEqual(args.port, 5432, "add_db_args should keep the default PostgreSQL port when PGPORT is unset")
        self.assertEqual(args.user, "", "add_db_args should use an empty user when PGUSER is unset")
        self.assertEqual(args.password, "", "add_db_args should use an empty password when PGPASSWORD is unset")

    def test_port_is_int(self) -> None:
        with patch.dict(os.environ, {"PGPORT": "5433"}, clear=False):
            parser = self._make_parser()
            args = parser.parse_args([])
        self.assertIsInstance(args.port, int, "add_db_args should parse --port defaults as integers")
