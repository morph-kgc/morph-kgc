from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
DataSourceAdapter Protocol + adapter registry
=============================================

Defines the single interface every data-source connector must satisfy and
provides a thread-safe registry so adapters can be registered without
touching the engine core (Open/Closed principle).

Registering a built-in adapter (done in this package's __init__.py)::

    from morph_kgc.source.protocol import register_adapter
    from morph_kgc.source.relational import RelationalAdapter
    from morph_kgc.constants.sources import RDB
    register_adapter(RDB, RelationalAdapter())

Registering a third-party adapter (e.g. a Databricks connector package)::

    # in the external package's __init__.py or entry-point hook:
    from morph_kgc.source.protocol import register_adapter
    register_adapter("DATABRICKS", DatabricksAdapter())

Resolving an adapter at materialisation time::

    adapter = get_adapter(source_format)
    data    = adapter.get_data(config, rml_rule, references, python_source)
"""

import threading
from typing import Any, Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class DataSourceAdapter(Protocol):
    """
    Protocol every data-source connector must satisfy.

    A conforming adapter needs exactly one method.  No base class is required;
    duck-typing is sufficient, but classes may inherit this Protocol for IDE
    support and isinstance() checks.

    Parameters
    ----------
    config:
        The morph-kgc Config object (read connection strings, credentials, etc.)
    rml_rule:
        A single RMLRule (or legacy dict-like Series) describing the mapping.
    references:
        The set of source-column / path references needed for this rule.
    python_source:
        Optional dict of in-memory objects keyed by source name; only used by
        PythonDataAdapter, ignored by all others.

    Returns
    -------
    pd.DataFrame
        One row per source record, one column per reference.
    """

    def get_data(
        self,
        config: Any,
        rml_rule: Any,
        references: set[str],
        python_source: dict | None = None,
        rml_mapping: Any | None = None,
    ) -> pd.DataFrame:
        ...


# ── Registry ──────────────────────────────────────────────────────────────────

_LOCK: threading.Lock = threading.Lock()

# Maps source-type string (upper-case) → adapter instance.
# Built-in types are registered by source/__init__.py at import time.
# Third-party adapters call register_adapter() from their own package.
_REGISTRY: dict[str, DataSourceAdapter] = {}


def register_adapter(source_format: str | list[str], adapter: DataSourceAdapter) -> None:
    """
    Register *adapter* for one or more *source_format* strings.

    *source_format* may be a single string or a list (for multi-token types such
    as EXCEL = ['XLS', 'XLSX', 'XLSM', 'XLSB']).

    Raises
    ------
    TypeError
        If *adapter* does not satisfy the DataSourceAdapter Protocol.
    """
    if not isinstance(adapter, DataSourceAdapter):
        raise TypeError(
            f"{adapter!r} does not implement DataSourceAdapter "
            f"(missing get_data method)."
        )
    keys = [source_format] if isinstance(source_format, str) else source_format
    with _LOCK:
        for key in keys:
            _REGISTRY[key.upper()] = adapter


def get_adapter(source_format: str) -> DataSourceAdapter:
    """
    Return the adapter registered for *source_format*.

    Raises
    ------
    KeyError
        With a descriptive message listing all registered types.
    """
    key = source_format.upper() if source_format else ""
    with _LOCK:
        adapter = _REGISTRY.get(key)
    if adapter is None:
        registered = sorted(_REGISTRY)
        raise KeyError(
            f"No DataSourceAdapter registered for source format {source_format!r}. "
            f"Registered types: {registered}"
        )
    return adapter


def registered_types() -> list[str]:
    """Return a sorted list of all currently registered source-type strings."""
    with _LOCK:
        return sorted(_REGISTRY)
