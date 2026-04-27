from __future__ import annotations

from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any

import pyarrow as pa


class Connector(ABC):
    """Abstract base class for all database connectors.

    A connector is responsible for establishing a connection to a single
    database, executing queries against it, and returning results as
    Apache Arrow tables. Concrete subclasses implement the database-specific
    mechanics; the rest of the engine works exclusively against this interface.

    Lifecycle:
        Use as a context manager (recommended) or call connect/disconnect manually.

        with MyConnector(alias, config) as conn:
            table = conn.fetch_table("users")
    """

    def __init__(self, alias: str, config: dict[str, Any]) -> None:
        """Store the catalog alias and raw config dict for this database.

        Args:
            alias: The key used to reference this database in the catalog
                   (e.g. "postgres_prod"). Used for error messages and logging.
            config: The database's config block from the catalog, already
                    deserialized from YAML. Shape varies by connector type.
        """
        self.alias = alias
        self.config = config

    @abstractmethod
    def connect(self) -> None:
        """Open and store a live connection to the database.

        Implementations should create any driver-level connection objects and
        attach them to self so that fetch_table / fetch_query can use them.
        Calling connect on an already-connected instance should be safe (no-op
        or reconnect, but not raise).
        """

    @abstractmethod
    def disconnect(self) -> None:
        """Close the connection and release all associated resources.

        After this returns, the connector must be in a clean state — no open
        sockets, file handles, or cursors. Calling disconnect on an instance
        that was never connected should be safe (no-op, not raise).
        """

    @abstractmethod
    def fetch_table(self, table_name: str, schema: str | None = None) -> pa.Table:
        """Fetch an entire table and return it as an Arrow table.

        This is the primary method the materializer calls per table reference
        found in the query plan. Implementations should SELECT * from the
        given table, converting all rows to Arrow-compatible types.

        Args:
            table_name: Unqualified name of the table to fetch (e.g. "users").
            schema: Optional schema/namespace within the database (e.g. "public"
                    in Postgres). When None, the connector uses the database's
                    default schema.

        Returns:
            A pyarrow.Table containing all rows and columns from the table.
        """

    @abstractmethod
    def fetch_query(self, sql: str) -> pa.Table:
        """Execute arbitrary SQL and return the result as an Arrow table.

        Intended for cases where the planner can construct a more targeted
        query (e.g. with WHERE filters or column selection) to reduce the
        volume of data transferred. The SQL must be valid for the target
        database's dialect.

        Args:
            sql: A complete, executable SQL statement in the target database's
                 dialect. The caller is responsible for correctness and safety.

        Returns:
            A pyarrow.Table containing all rows from the query result.
        """

    def __enter__(self) -> Connector:
        """Connect and return self, enabling use as a context manager."""
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Disconnect on context manager exit, regardless of exceptions."""
        self.disconnect()
