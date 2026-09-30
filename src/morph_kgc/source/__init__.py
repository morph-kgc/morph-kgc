__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
morph_kgc.source
=================
Data-source package. Importing this package registers all built-in
DataSourceAdapters so callers only need:

    from morph_kgc.source import get_adapter

    adapter = get_adapter(source_type)          # KeyError if unknown
    data    = adapter.get_data(config, rule, refs, python_source, rml_mapping)

External packages can register new adapters without modifying this file::

    from morph_kgc.source.protocol import register_adapter
    register_adapter("MY_CUSTOM_TYPE", MyAdapter())
"""

from .protocol      import get_adapter, register_adapter, registered_types

# ── Built-in adapter instances (one per singleton is enough) ─────────────────
from .relational    import RelationalAdapter
from .file          import FileAdapter
from .http_api      import HttpApiAdapter
from .property_graph import PropertyGraphAdapter
from .python_data   import PythonDataAdapter

from ..constants.sources import (
    RDB, PGDB, HTTPAPI, IN_MEMORY_TYPES, FILE_SOURCE_TYPES,
    MYSQL, MARIADB, MSSQL, ORACLE, POSTGRESQL, SQLITE, DATABRICKS, SNOWFLAKE,
)

_relational = RelationalAdapter()
_file       = FileAdapter()
_http_api   = HttpApiAdapter()
_pg         = PropertyGraphAdapter()
_python     = PythonDataAdapter()

# Relational databases
for _st in [RDB, MYSQL, MARIADB, MSSQL, ORACLE, POSTGRESQL, SQLITE, DATABRICKS, SNOWFLAKE]:
    register_adapter(_st, _relational)

# Property-graph databases
register_adapter(PGDB, _pg)

# HTTP API
register_adapter(HTTPAPI, _http_api)

# File-based sources  (FILE_SOURCE_TYPES is a flat list of strings)
register_adapter(FILE_SOURCE_TYPES, _file)

# In-memory / Python sources
register_adapter(IN_MEMORY_TYPES, _python)
