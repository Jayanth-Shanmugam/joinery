"""Smoke test for the connector layer.

Builds a small SQLite database on disk, then exercises:
  - SQLAlchemyConnector lifecycle (connect / disconnect / context manager)
  - fetch_query with arbitrary SQL
  - fetch_table with quoted identifier (uses a reserved word)
  - the get_connector factory
  - the catalog loader (with env var interpolation)

Run from the project root:

    python smoke_connectors.py
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from pathlib import Path

import pyarrow as pa
import yaml

from joinery.catalog import load_catalog
from joinery.connectors import get_connector
from joinery.connectors.base import Connector
from joinery.connectors.sqlalchemy import SQLAlchemyConnector


def _seed_sqlite(path: Path) -> None:
    """Create a SQLite db at `path` with two small tables for smoke tests."""
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    cur.executemany(
        "INSERT INTO users (id, name) VALUES (?, ?)",
        [(1, "alice"), (2, "bob"), (3, "carol")],
    )
    # 'order' is a reserved word — verifies that quoting works in fetch_table.
    cur.execute('CREATE TABLE "order" (id INTEGER PRIMARY KEY, amount REAL)')
    cur.executemany(
        'INSERT INTO "order" (id, amount) VALUES (?, ?)',
        [(1, 19.99), (2, 42.50), (3, 7.25)],
    )
    conn.commit()
    conn.close()


def _check(condition: bool, label: str) -> None:
    """Tiny assert helper that prints ✓/✗ for every step."""
    status = "OK " if condition else "FAIL"
    print(f"  [{status}] {label}")
    if not condition:
        raise AssertionError(label)


def test_sqlalchemy_connector(db_path: Path) -> None:
    print("\n# SQLAlchemyConnector — lifecycle & fetching")

    config = {"type": "sqlite", "path": str(db_path)}
    conn = SQLAlchemyConnector("sqlite_smoke", config)

    _check(conn._engine is None and conn._connection is None, "constructed without opening a connection")
    _check(conn._dialect == "sqlite", "dialect resolved to 'sqlite'")

    expected_url = f"sqlite:///{db_path}"
    _check(conn._build_url() == expected_url, f"_build_url() == {expected_url}")

    conn.connect()
    _check(conn._connection is not None, "connect() opened a connection")
    conn.connect()  # idempotent
    _check(conn._connection is not None, "connect() is idempotent (no error on second call)")

    arrow = conn.fetch_query("SELECT id, name FROM users ORDER BY id")
    _check(isinstance(arrow, pa.Table), "fetch_query returned a pyarrow.Table")
    _check(arrow.num_rows == 3, "fetch_query returned 3 rows")
    _check(arrow.column_names == ["id", "name"], "fetch_query column names match")
    names = arrow.column("name").to_pylist()
    _check(names == ["alice", "bob", "carol"], f"fetch_query rows correct (got {names})")

    arrow_orders = conn.fetch_table("order")
    _check(arrow_orders.num_rows == 3, 'fetch_table on reserved-word "order" returned 3 rows')
    _check(set(arrow_orders.column_names) == {"id", "amount"}, "fetch_table columns match")

    conn.disconnect()
    _check(conn._engine is None and conn._connection is None, "disconnect() cleared engine and connection")
    conn.disconnect()  # safe to call again
    _check(True, "disconnect() is safe to call twice")


def test_context_manager(db_path: Path) -> None:
    print("\n# SQLAlchemyConnector — context manager")
    config = {"type": "sqlite", "path": str(db_path)}
    with SQLAlchemyConnector("sqlite_ctx", config) as conn:
        _check(conn._connection is not None, "entered context with live connection")
        arrow = conn.fetch_table("users")
        _check(arrow.num_rows == 3, "fetch_table inside context returned rows")
    _check(conn._connection is None, "exited context closed the connection")


def test_fetch_query_before_connect(db_path: Path) -> None:
    print("\n# SQLAlchemyConnector — error paths")
    config = {"type": "sqlite", "path": str(db_path)}
    conn = SQLAlchemyConnector("sqlite_err", config)
    try:
        conn.fetch_query("SELECT 1")
    except RuntimeError as e:
        _check("not connected" in str(e).lower(), f"fetch_query before connect raised RuntimeError ({e!r})")
    else:
        _check(False, "fetch_query before connect should have raised RuntimeError")

    bad = SQLAlchemyConnector("bad", {"type": "postgresql"})  # no url
    try:
        bad._build_url()
    except ValueError as e:
        _check("connection URL" in str(e), "missing url for postgres raises ValueError")
    else:
        _check(False, "missing url for postgres should have raised")


def test_factory(db_path: Path) -> None:
    print("\n# get_connector factory")
    conn = get_connector("sqlite_x", {"type": "sqlite", "path": str(db_path)})
    _check(isinstance(conn, SQLAlchemyConnector), "sqlite -> SQLAlchemyConnector")
    _check(isinstance(conn, Connector), "result is a Connector")

    conn_pg = get_connector("pg", {"type": "postgresql", "url": "postgresql://x"})
    _check(isinstance(conn_pg, SQLAlchemyConnector), "postgresql -> SQLAlchemyConnector")

    try:
        get_connector("nope", {})
    except ValueError as e:
        _check("missing a 'type'" in str(e), "missing type raises ValueError")
    else:
        _check(False, "missing type should have raised")

    try:
        get_connector("nope", {"type": "mongodb"})
    except ValueError as e:
        _check("Unsupported" in str(e), "unsupported type raises ValueError")
    else:
        _check(False, "unsupported type should have raised")


def test_catalog_loader(db_path: Path, tmpdir: Path) -> None:
    print("\n# catalog loader")
    os.environ["SMOKE_SQLITE_PATH"] = str(db_path)

    catalog = {
        "version": "1",
        "databases": {
            "main": {"type": "sqlite", "path": "${SMOKE_SQLITE_PATH}"},
            "other": {"type": "postgresql", "url": "postgresql://u:p@h/d"},
        },
    }
    catalog_path = tmpdir / "catalog.yaml"
    catalog_path.write_text(yaml.safe_dump(catalog))

    loaded = load_catalog(catalog_path)
    _check(set(loaded.keys()) == {"main", "other"}, "loaded both database aliases")
    _check(loaded["main"]["path"] == str(db_path), "env var ${SMOKE_SQLITE_PATH} interpolated")
    _check(loaded["other"]["url"] == "postgresql://u:p@h/d", "non-env-var string passed through")

    # Round-trip: catalog -> get_connector -> fetch
    conn = get_connector("main", loaded["main"])
    with conn:
        arrow = conn.fetch_table("users")
    _check(arrow.num_rows == 3, "catalog -> connector -> fetch_table works end-to-end")

    # Missing env var
    bad_catalog = {"version": "1", "databases": {"x": {"type": "sqlite", "path": "${UNSET_VAR_XYZ}"}}}
    bad_path = tmpdir / "bad.yaml"
    bad_path.write_text(yaml.safe_dump(bad_catalog))
    try:
        load_catalog(bad_path)
    except KeyError as e:
        _check("UNSET_VAR_XYZ" in str(e), "missing env var raises KeyError naming the var")
    else:
        _check(False, "missing env var should have raised KeyError")

    # Structural errors
    for label, payload in [
        ("top-level not mapping", "- a\n- b\n"),
        ("missing 'databases'", "version: '1'\n"),
        ("entry missing 'type'", "databases:\n  x:\n    url: postgresql://h\n"),
    ]:
        p = tmpdir / f"{label}.yaml"
        p.write_text(payload)
        try:
            load_catalog(p)
        except ValueError:
            _check(True, f"structural error: {label} -> ValueError")
        else:
            _check(False, f"structural error: {label} should have raised ValueError")


def main() -> None:
    with tempfile.TemporaryDirectory() as td:
        tmpdir = Path(td)
        db_path = tmpdir / "smoke.db"
        _seed_sqlite(db_path)
        print(f"Seeded SQLite db at {db_path}")

        test_sqlalchemy_connector(db_path)
        test_context_manager(db_path)
        test_fetch_query_before_connect(db_path)
        test_factory(db_path)
        test_catalog_loader(db_path, tmpdir)

    print("\nAll smoke checks passed.")


if __name__ == "__main__":
    main()
