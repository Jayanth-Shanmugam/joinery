from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

# Matches ${VAR_NAME} patterns in string values.
_ENV_VAR_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _interpolate_env_vars(value: str) -> str:
    """Replace every ``${VAR_NAME}`` placeholder in a string with its environment value.

    Args:
        value: A raw string that may contain one or more ``${VAR}`` placeholders.

    Returns:
        The string with all placeholders substituted.

    Raises:
        KeyError: If a referenced environment variable is not set, so that
            misconfigured catalogs fail loudly at load time rather than
            silently passing an unpopulated URL to the connector.
    """
    def _replace(match: re.Match[str]) -> str:
        var_name = match.group(1)
        if var_name not in os.environ:
            raise KeyError(
                f"Catalog references environment variable '{var_name}' "
                "which is not set."
            )
        return os.environ[var_name]

    return _ENV_VAR_PATTERN.sub(_replace, value)


def _interpolate_config(config: Any) -> Any:
    """Recursively walk a config structure and interpolate env vars in all strings.

    Traverses dicts and lists in place (returns a new structure, original
    is not mutated). Non-string leaf values are returned unchanged.

    Args:
        config: Any value deserialized from YAML — dict, list, str, int, etc.

    Returns:
        The same structure with all string values env-var-interpolated.
    """
    if isinstance(config, dict):
        return {key: _interpolate_config(val) for key, val in config.items()}
    if isinstance(config, list):
        return [_interpolate_config(item) for item in config]
    if isinstance(config, str):
        return _interpolate_env_vars(config)
    return config


def _validate_catalog(raw: Any, path: Path) -> None:
    """Assert that the raw YAML structure is a well-formed catalog.

    Checks only structural requirements — type-level validation of individual
    connection configs is left to the connectors themselves.

    Args:
        raw: The deserialized YAML content.
        path: The catalog file path, used in error messages only.

    Raises:
        ValueError: If the catalog is missing required keys or has an
            unexpected structure.
    """
    if not isinstance(raw, dict):
        raise ValueError(
            f"Catalog at '{path}' must be a YAML mapping at the top level, "
            f"got {type(raw).__name__}."
        )
    if "databases" not in raw:
        raise ValueError(
            f"Catalog at '{path}' is missing the required 'databases' key."
        )
    databases = raw["databases"]
    if not isinstance(databases, dict):
        raise ValueError(
            f"Catalog at '{path}': 'databases' must be a mapping of "
            f"alias -> config, got {type(databases).__name__}."
        )
    for alias, entry in databases.items():
        if not isinstance(entry, dict):
            raise ValueError(
                f"Catalog at '{path}': entry for '{alias}' must be a mapping, "
                f"got {type(entry).__name__}."
            )
        if "type" not in entry:
            raise ValueError(
                f"Catalog at '{path}': entry for '{alias}' is missing "
                "the required 'type' field."
            )


def load_catalog(path: str | Path) -> dict[str, dict[str, Any]]:
    """Load, validate, and return the database catalog from a YAML file.

    Reads the catalog file, checks its structure, interpolates any
    ``${ENV_VAR}`` placeholders in string values, and returns a plain dict
    mapping each database alias to its fully-resolved config block.

    Args:
        path: Path to the catalog YAML file.

    Returns:
        A dict of ``{alias: config}`` where each config is a plain dict
        ready to be passed to ``get_connector``.

    Raises:
        FileNotFoundError: If the catalog file does not exist.
        ValueError: If the catalog is structurally invalid.
        KeyError: If any ``${ENV_VAR}`` placeholder references an unset variable.
        yaml.YAMLError: If the file is not valid YAML.
    """
    catalog_path = Path(path)
    if not catalog_path.exists():
        raise FileNotFoundError(f"Catalog file not found: '{catalog_path}'")

    with catalog_path.open() as f:
        raw = yaml.safe_load(f)

    _validate_catalog(raw, catalog_path)
    interpolated = _interpolate_config(raw)
    return interpolated["databases"]
