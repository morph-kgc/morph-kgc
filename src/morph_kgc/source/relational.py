from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Relational database adapter
============================
Implements DataSourceAdapter for all RDBMS sources (MySQL, MariaDB, MSSQL,
Oracle, PostgreSQL, SQLite, Databricks, Snowflake) plus the generic RDB type.

Public surface
--------------
RelationalAdapter — DataSourceAdapter implementation
get_rdb_reference_datatype() — used by materializer for SQL type inference
"""

import logging
from contextlib import contextmanager
from typing import Any, Iterator

import pandas as pd

from ..constants.misc import LOGGING_NAMESPACE
from ..constants.rml import RML_TABLE_NAME, RML_QUERY
from ..constants.sources import MYSQL, MARIADB, MSSQL, DATABRICKS
from ..constants.xsd import *

LOGGER = logging.getLogger(LOGGING_NAMESPACE)


SQL_RDF_DATATYPE: dict[str | Any, str | Any] = {
    # Python dicts preserve insertion order, mind types are not intercepted by
    # the wrong, shorter key before ever reaching their correct match

    # binary
    'LONG RAW': XSD_HEX_BINARY,
    'VARBINARY': XSD_HEX_BINARY,
    'BINARY': XSD_HEX_BINARY,
    'BLOB': XSD_HEX_BINARY,
    'BFILE': XSD_HEX_BINARY,
    'RAW': XSD_HEX_BINARY,

    # boolean (must precede INT-family keys)
    'TINYINT': XSD_BOOLEAN,
    'BOOLEAN': XSD_BOOLEAN,
    'BOOL': XSD_BOOLEAN,

    # integer
    'BIGSERIAL': XSD_INTEGER,
    'SMALLSERIAL': XSD_INTEGER,
    'SMALLINT': XSD_INTEGER,
    'BIGINT': XSD_INTEGER,
    'SERIAL2': XSD_INTEGER,
    'SERIAL4': XSD_INTEGER,
    'SERIAL8': XSD_INTEGER,
    'INTEGER': XSD_INTEGER,
    'INT8': XSD_INTEGER,
    'INT4': XSD_INTEGER,
    'INT2': XSD_INTEGER,
    'INT': XSD_INTEGER,

    # decimal
    'DECIMAL': XSD_DECIMAL,
    'NUMERIC': XSD_DECIMAL,

    # double / float (longer/more specific keys first)
    'DOUBLE PRECISION': XSD_DOUBLE,
    'FLOAT8': XSD_DOUBLE,
    'DOUBLE': XSD_DOUBLE,
    'NUMBER': XSD_DOUBLE,
    'FLOAT': XSD_DOUBLE,
    'REAL': XSD_DOUBLE,

    # temporal (must precede shorter substrings, e.g. DATE before it's swallowed by nothing,
    # but DATETIME/TIMESTAMP must precede DATE/TIME)
    'TIMESTAMP': XSD_DATETIME,
    'DATETIME': XSD_DATETIME,
    'DATE': XSD_DATE,
    'TIME': XSD_TIME,

    # string / text
    'NVARCHAR': XSD_STRING,
    'VARCHAR': XSD_STRING,
    'NCHAR': XSD_STRING,
    'CHAR': XSD_STRING,
    'CLOB': XSD_STRING,
    'TEXT': XSD_STRING,
    'STRING': XSD_STRING,
    'UUID': XSD_STRING,
    'JSON': XSD_STRING,
}


# ── Internal helpers ──────────────────────────────────────────────────────────

# Delimiters used by each dialect to quote an identifier, as (open, close).
# The close character is also the one that has to be doubled to escape it
# inside the identifier.
_IDENTIFIER_DELIMITERS: dict[str, tuple[str, str]] = {
    MYSQL: ('`', '`'),
    MARIADB: ('`', '`'),
    MSSQL: ('[', ']'),
    # Databricks (Spark SQL) delimits with backticks: double quotes are string
    # literals there unless spark.sql.ansi.doubleQuotedIdentifiers is enabled.
    DATABRICKS: ('`', '`'),
}

# ANSI SQL: Oracle, PostgreSQL, SQLite, Snowflake and any unknown dialect.
_ANSI_IDENTIFIER_DELIMITERS = ('"', '"')


def _quote_identifier(identifier: str, db_dialect: str) -> str:
    """
    Quote *identifier* the way *db_dialect* expects it.

    Dots are treated as qualification separators, so that schema-qualified
    names are quoted part by part (``myschema.Student`` becomes
    ``"myschema"."Student"`` and not a single identifier containing a dot,
    see issue #89). The closing delimiter is doubled to escape it, so that an
    identifier carrying one cannot break out of the quoting.
    """
    open_char, close_char = _IDENTIFIER_DELIMITERS.get(db_dialect, _ANSI_IDENTIFIER_DELIMITERS)

    return '.'.join(
        f'{open_char}{part.replace(close_char, close_char * 2)}{close_char}'
        for part in identifier.split('.')
    )


def _replace_query_enclosing_characters(sql_query: str, db_dialect: str) -> str:
    """
    Swap backtick identifier quoting for dialect-specific quoting in a
    user-supplied ``rml:query``. Queries built by Morph-KGC are quoted for the
    target dialect directly by _quote_identifier() and must not go through
    this function.
    """
    if db_dialect in (MYSQL, MARIADB):
        # backticks are already correct for MySQL/MariaDB
        return sql_query

    if db_dialect == MSSQL:
        # backticks alternate open/close; can't be done with a single replace
        square_brackets = ['[', ']']
        num_enclosing_char = 0
        result = []
        for char in sql_query:
            if char == '`':
                result.append(square_brackets[num_enclosing_char % 2])
                num_enclosing_char += 1
            else:
                result.append(char)
        return ''.join(result)

    if db_dialect == DATABRICKS:
        # Databricks doesn't require identifier quoting; strip backticks
        return sql_query.replace('`', '')

    # ANSI-compliant dialects (Oracle, PostgreSQL, SQLite, Snowflake, default)
    return sql_query.replace('`', '"')

@contextmanager
def _relational_db_connection(config, source_name: str) -> Iterator[tuple[Any, str]]:
    """
    Yield (connection, dialect_string) for *source_name*, closing the
    connection and disposing of the engine on exit.

    Must be used as a context manager so that the underlying DBAPI connection
    is released as soon as the query is done, instead of lingering until the
    garbage collector happens to reclaim it. Callers that hold connections
    open implicitly stack up one connection per mapping rule and exhaust the
    server's connection limit on large mappings.

    A fresh engine is built per call on purpose: the materializer forks worker
    processes, and SQLAlchemy engines (and their pooled connections) must not
    be shared across a fork. NullPool already keeps the engine from holding
    connections open behind our back; disposing it releases the rest.
    """
    import ast

    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool

    connect_args = (
        ast.literal_eval(config.get_connect_args(source_name))
        if config.has_connect_args(source_name)
        else {}
    )

    engine = create_engine(
        config.get_db_url(source_name), connect_args=connect_args, poolclass=NullPool
    )
    try:
        connection = engine.connect()
        try:
            yield connection, engine.dialect.name.upper()
        finally:
            connection.close()
    finally:
        engine.dispose()

def _get_table_columns(config, source_name: str, table_name: str) -> list | None:
    """
    Query the information schema for the columns of *table_name*.
    Returns None when the table cannot be inspected (views, CTEs, missing
    tables, etc.).
    """
    from sqlalchemy import inspect

    with _relational_db_connection(config, source_name) as (connection, _):
        try:
            return inspect(connection).get_columns(table_name)
        except Exception:
            LOGGER.debug(f'Could not inspect table `{table_name}` of data source `{source_name}`.')
            return None

def _get_column_table_datatype(config, source_name: str, table_name: str, column_name: str,
                               columns_cache: dict | None = None) -> str | None:
    """
    Query the information schema to obtain the SQL datatype of *column_name*,
    mapped to its corresponding XSD datatype via SQL_RDF_DATATYPE.
    Returns None when the column cannot be found (views, CTEs, etc.) or when
    the SQL datatype has no known XSD mapping.

    *columns_cache* optionally memoizes the inspected columns per table, so
    that a mapping with several references over one table only hits the
    information schema once.
    """
    cache_key = (source_name, table_name)
    if columns_cache is not None and cache_key in columns_cache:
        columns = columns_cache[cache_key]
    else:
        columns = _get_table_columns(config, source_name, table_name)
        if columns_cache is not None:
            columns_cache[cache_key] = columns

    if columns is None:
        return None

    data_type = None
    for col in columns:
        if col["name"].lower() == column_name.lower():
            data_type = str(col["type"]).upper()
            break

    if data_type is None:
        return None

    for sql_type, xsd_type in SQL_RDF_DATATYPE.items():
        if sql_type in data_type:
            return xsd_type

    return None

def _build_sql_query(config, rml_rule, references, db_dialect: str) -> str | None:
    """
    Construct the SQL SELECT that fetches exactly the *references* columns,
    with identifiers quoted for *db_dialect*.
    Returns None when the rule has no column references (all constants).
    """
    col_refs = list(references)
    if not col_refs:
        return None

    ls_type = rml_rule.logical_source.value_type
    ls_value = rml_rule.logical_source.value

    if ls_type == RML_QUERY:
        # user-supplied SQL: only its backtick quoting is translated
        return _replace_query_enclosing_characters(ls_value, db_dialect)
    elif ls_type == RML_TABLE_NAME:
        quoted = ", ".join(_quote_identifier(r, db_dialect) for r in col_refs)
        from_clause = _quote_identifier(ls_value, db_dialect)
        conditions = " AND ".join(
            f"{_quote_identifier(r, db_dialect)} IS NOT NULL" for r in col_refs
        )
        filter_null = f" WHERE {conditions}"

        return f"SELECT {quoted} FROM {from_clause}{filter_null}"
    else:
        return None


# ── Public helpers ────────────────────────────────────────────────────────────

def get_rdb_reference_datatype(config, logical_source, reference: str,
                               columns_cache: dict | None = None) -> str | None:
    """
    Return the XSD datatype for *reference* in *logical_source*'s table.
    Used by the mapping parser for automatic XSD datatype assignment.
    Returns None for query-based sources (arbitrary SQL cannot be inspected),
    when the column is not found, or when its SQL datatype has no known XSD
    counterpart.

    *columns_cache* is an optional dictionary reused across calls to inspect
    each table only once.
    """
    if logical_source.value_type != RML_TABLE_NAME:
        return None  # cannot inspect arbitrary SQL queries (rml:query)

    return _get_column_table_datatype(
        config, logical_source.config_section_name, logical_source.value, reference, columns_cache
    )

# ── Adapter ───────────────────────────────────────────────────────────────────

class RelationalAdapter:
    """DataSourceAdapter for relational databases."""

    def get_data(
        self,
        config: Any,
        rml_rule: Any,
        references: set[str],
        python_source: dict | None = None,
        rml_mapping: Any | None = None,
    ) -> pd.DataFrame:
        if not references:
            # all-constant rule: nothing to fetch, do not open a connection
            return pd.DataFrame(columns=list(references))

        with _relational_db_connection(
            config, rml_rule.logical_source.config_section_name
        ) as (conn, dialect):
            # the query is built here because quoting depends on the dialect,
            # which is only known once the engine has been created
            sql_query = _build_sql_query(config, rml_rule, references, dialect)
            if sql_query is None:
                return pd.DataFrame(columns=list(references))

            LOGGER.debug(
                f"SQL query for mapping rule `{rml_rule.triples_map_id}`: [{sql_query}]"
            )

            return pd.read_sql_query(sql_query, con=conn, coerce_float=False)
