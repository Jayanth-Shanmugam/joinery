from __future__ import annotations

from typing import Any

from joinery.connectors.base import Connector
from joinery.connectors.sqlalchemy import SQLAlchemyConnector

# Maps catalog 'type' values to their connector class.
# Add new entries here when new connector implementations are introduced.
_CONNECTOR_REGISTRY: dict[str, type[Connector]] = {
    "postgresql": SQLAlchemyConnector,
    "mysql": SQLAlchemyConnector,
    "sqlite": SQLAlchemyConnector,
    "duckdb": SQLAlchemyConnector,
}


def get_connector(alias: str, config: dict[str, Any]) -> Connector:
    """Instantiate the appropriate connector for a catalog database entry.

    Looks up the ``type`` field in the config against the connector registry
    and returns a ready-to-connect (but not yet connected) instance.

    Args:
        alias: The catalog key for this database (e.g. ``"postgres_prod"``).
            Passed through to the connector for use in error messages.
        config: The database's config block from the catalog, already
            deserialized and env-var-interpolated. Must contain a ``type``
            key whose value is a registered database type.

    Returns:
        An unconnected Connector instance for the given database.

    Raises:
        ValueError: If the ``type`` field is missing or not in the registry.
    """
    db_type = config.get("type")
    if not db_type:
        raise ValueError(
            f"[{alias}] Catalog entry is missing a 'type' field. "
            f"Received keys: {list(config.keys())}"
        )

    connector_cls = _CONNECTOR_REGISTRY.get(db_type)
    if connector_cls is None:
        supported = ", ".join(sorted(_CONNECTOR_REGISTRY))
        raise ValueError(
            f"[{alias}] Unsupported database type '{db_type}'. "
            f"Supported types: {supported}"
        )

    return connector_cls(alias, config)
