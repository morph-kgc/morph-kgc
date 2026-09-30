from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Property-graph database adapter
==================================
Implements DataSourceAdapter for property-graph databases:
- Neo4j (neo4j:// URL scheme)

The source type string PGDB is registered in source/__init__.py.
"""

from typing import Any
import pandas as pd

def _fetch_pg(config, rml_rule) -> pd.DataFrame:
    db_url = config.get_db_url(rml_rule.logical_source.config_section_name)
    query = rml_rule.logical_source.value

    import neo4j
    db = db_url.split("/")[-1]
    base_url = "/".join(db_url.split("/")[:-1])
    base_url, user_password = base_url.split("@")
    user, password = user_password.split(":")
    # the driver owns a connection pool, so it must be closed once the query is
    # done; leaving it to the garbage collector holds sockets open for the rest
    # of the materialization, one pool per mapping rule
    with neo4j.GraphDatabase.driver(base_url, auth=(user, password)) as driver:
        return driver.execute_query(
            query, database=db, result_transformer=neo4j.Result.to_df
        )

class PropertyGraphAdapter:
    """DataSourceAdapter for property-graph databases (Neo4j)."""

    def get_data(
        self,
        config: Any,
        rml_rule: Any,
        references: set[str],
        python_source: dict | None = None,
        rml_mapping: Any | None = None,
    ) -> pd.DataFrame:
        return _fetch_pg(config, rml_rule)