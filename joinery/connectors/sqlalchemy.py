from __future__ import annotations

from typing import Any

import pandas as pd
import pyarrow as pa
import sqlalchemy
import sqlglot.expressions as exp
from sqlalchemy import text

from joinery.connectors.base import Connector

# Maps catalog 'type' values to sqlglot dialect names for correct SQL generation.
_DIALECT_MAP: dict[str, str] = {
    "postgresql": "postgres",
    "mysql": "mysql",
    "sqlite": "sqlite",
    "duckdb": "duckdb",
}

# Default DBAPI driver per database type. Used when the catalog provides
# a structured config (host/port/...) without an explicit 'driver' field.
# Users can install an alternative driver and select it via config.driver.
_DEFAULT_DRIVERS: dict[str, str] = {
    "postgresql": "psycopg2",
    "mysql": "pymysql",
    "mssql": "pyodbc",
    "oracle": "oracledb",
}


class SQLAlchemyConnector(Connector):
    """Database connector backed by SQLAlchemy.

    Supports any database that SQLAlchemy can speak to. The caller is
    responsible for installing the appropriate DBAPI driver (e.g.
    psycopg2 for Postgres, pymysql for MySQL). All query results are
    returned as Apache Arrow tables via a pandas intermediate step.

    Config shapes accepted (checked in priority order):

        1. Explicit URL — works for any database SQLAlchemy supports:
            {"type": "postgresql",
             "url": "postgresql+psycopg2://user:pw@host/db"}

        2. SQLite shorthand — server-less, just a path:
            {"type": "sqlite", "path": "./data.db"}
            {"type": "sqlite", "path": ":memory:"}

        3. Structured config — per-field for any server-based database.
           Recommended for catalogs shared across a team because secrets
           live in a single field (typically env-var backed) and the
           database can be migrated by swapping individual fields:
            {"type": "postgresql",
             "host": "db.example.com",
             "port": 5432,
             "username": "analyst",
             "password": "${POSTGRES_PASSWORD}",
             "database": "analytics",
             "driver": "psycopg2",          # optional, defaulted per type
             "query": {"sslmode": "require"}}  # optional
    """

    def __init__(self, alias: str, config: dict[str, Any]) -> None:
        """Initialise the connector without opening a connection.

        Resolves the sqlglot dialect for this database type so that
        fetch_table can generate correctly-quoted SQL later.

        Args:
            alias: Catalog key for this database (e.g. "postgres_prod").
            config: Raw config dict from the catalog entry for this database.
        """
        super().__init__(alias, config)
        self._engine: sqlalchemy.Engine | None = None
        self._connection: sqlalchemy.Connection | None = None
        self._dialect: str = _DIALECT_MAP.get(config.get("type", ""), "")

    def _build_url(self) -> str:
        """Derive a SQLAlchemy connection URL from the config block.

        Three shapes are supported, checked in priority order:
        1. Explicit ``url`` key (highest priority, returned as-is).
        2. SQLite shorthand: ``type: sqlite`` plus a ``path`` key.
        3. Structured config: per-field (host/port/username/password/database/...)
           for any server-based database type registered in ``_DEFAULT_DRIVERS``.

        Env-var interpolation in string values is assumed to have happened
        upstream in the catalog loader.

        Returns:
            A SQLAlchemy-compatible connection URL string with all
            components properly URL-escaped.

        Raises:
            ValueError: If none of the three shapes can be matched, or if
                a structured config is missing the required ``database``
                field, or if the type has no default driver and none was
                supplied via the ``driver`` field.
        """
        if "url" in self.config:
            return self.config["url"]

        db_type = self.config.get("type", "")
        if db_type == "sqlite":
            path = self.config.get("path", ":memory:")
            # sqlite:///relative or sqlite:////absolute — the f-string handles
            # both because an absolute path already starts with '/'.
            return f"sqlite:///{path}"

        return self._build_structured_url(db_type)

    def _build_structured_url(self, db_type: str) -> str:
        """Assemble a connection URL from per-field config.

        Resolves the DBAPI driver (defaulted via ``_DEFAULT_DRIVERS``,
        overridable via the ``driver`` config field), then delegates URL
        assembly to ``sqlalchemy.URL.create`` so that special characters
        in passwords or other components are escaped correctly.

        Args:
            db_type: The database type from ``config['type']``. Used to
                look up the default driver and to compose the SQLAlchemy
                ``drivername`` (``"<type>+<driver>"``).

        Returns:
            A SQLAlchemy-compatible connection URL string.

        Raises:
            ValueError: If the type has no default driver and the config
                supplies none, or if the required ``database`` field is
                missing.
        """
        driver = self.config.get("driver") or _DEFAULT_DRIVERS.get(db_type)
        if not driver:
            raise ValueError(
                f"[{self.alias}] No default driver is registered for type "
                f"'{db_type}'. Either provide a 'driver' field in the catalog "
                "or use the 'url' field directly."
            )

        if "database" not in self.config:
            raise ValueError(
                f"[{self.alias}] Structured config requires a 'database' field. "
                f"Received keys: {list(self.config.keys())}"
            )

        url = sqlalchemy.URL.create(
            drivername=f"{db_type}+{driver}",
            username=self.config.get("username"),
            password=self.config.get("password"),
            host=self.config.get("host"),
            port=self.config.get("port"),
            database=self.config["database"],
            query=self.config.get("query", {}),
        )
        return url.render_as_string(hide_password=False)

    def connect(self) -> None:
        """Open a SQLAlchemy engine and acquire a connection.

        Idempotent: if a connection is already open this is a no-op.

        Raises:
            ValueError: If the config does not contain enough information
                to build a connection URL.
            sqlalchemy.exc.SQLAlchemyError: If the underlying driver fails
                to connect (bad credentials, unreachable host, etc.).
        """
        if self._connection is not None:
            return
        url = self._build_url()
        self._engine = sqlalchemy.create_engine(url)
        self._connection = self._engine.connect()

    def disconnect(self) -> None:
        """Close the connection and dispose the engine, releasing all resources.

        Idempotent: safe to call even if connect() was never called.
        """
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def fetch_table(self, table_name: str, schema: str | None = None) -> pa.Table:
        """Fetch every row from a table and return it as an Arrow table.

        Constructs a ``SELECT *`` statement using sqlglot so that identifiers
        are quoted correctly for the target database dialect, then delegates
        to fetch_query.

        Args:
            table_name: Unqualified table name (e.g. "users").
            schema: Optional schema within the database (e.g. "public").
                    When None the database's default schema is used.

        Returns:
            A pyarrow.Table with all rows and columns from the table.
        """
        table_expr = exp.Table(
            this=exp.Identifier(this=table_name, quoted=True),
            db=exp.Identifier(this=schema, quoted=True) if schema else None,
        )
        sql = exp.select("*").from_(table_expr).sql(dialect=self._dialect or None)
        return self.fetch_query(sql)

    def fetch_query(self, sql: str) -> pa.Table:
        """Execute a SQL string and return the result as an Arrow table.

        Uses pandas as the intermediary: SQLAlchemy cursors -> pandas DataFrame
        -> pyarrow Table. Column names and dtypes are inferred by pandas and
        then mapped to Arrow types automatically.

        Args:
            sql: A complete SQL statement valid for the target database dialect.

        Returns:
            A pyarrow.Table containing all rows from the query result.

        Raises:
            RuntimeError: If called before connect().
            sqlalchemy.exc.SQLAlchemyError: On query execution errors.
        """
        if self._connection is None:
            raise RuntimeError(
                f"[{self.alias}] Connector is not connected. "
                "Call connect() or use it as a context manager before fetching."
            )
        df = pd.read_sql(text(sql), self._connection)
        return pa.Table.from_pandas(df, preserve_index=False)
