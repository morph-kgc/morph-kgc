from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
On-disk tables of shared contexts
=================================
A shared context too large to hold in memory (the labels of a vocabulary of a
million concepts, the answer of a SPARQL query over a whole knowledge graph)
keeps its data in Parquet files, written to the state directory of the run by
the initializer. The context itself only holds a :class:`ParquetTable` handle
per file, so loading it costs nothing.

The files are read through DuckDB, with a connection per process and run opened
on first use, running a single thread under the ``state_memory_limit`` of the
configuration. A query skips the parts (row groups) of a file whose smallest
and largest values its filters rule out, so a file sorted by the column it is
looked up by is read in part, and the operating system caches the files once
for every worker process instead of each holding a copy of the context.

The files are built the same way, through a scratch DuckDB database on disk, so
that building them takes the same capped memory whatever their size.

Public API
----------
TableBuilder(state_dir, memory_limit)
    Streams rows into scratch tables and writes Parquet files out of them.
write_table(state_dir, memory_limit, rows, columns, ...) -> ParquetTable
    Writes an iterable of rows to a Parquet file in one go.
ParquetTable.select(sql, parameters, **frames) -> list[tuple]
    Queries a Parquet file, written ``{table}`` in *sql*.
close_connections(state_dir)
    Closes the DuckDB connection this process opened for a run, at its end.
"""

import os
import tempfile
import threading
from dataclasses import dataclass
from typing import Any, Iterable, Optional

import duckdb
import pandas as pd

# Rows kept in memory before they are moved to a scratch table on disk.
BATCH_ROWS = 50_000

# The DuckDB connections of this process, one per run it reads the tables of:
# (process id, state directory, memory limit) -> connection. A forked worker
# inherits the entries of its parent, which it must not use, hence the process
# id. Runs materialized at the same time in threads of one process each have
# their own.
_CONNECTIONS: dict[tuple, Any] = {}
_CONNECTION_LOCK = threading.Lock()
# Each thread queries through cursors of its own (see AsyncExecutor).
_CURSORS = threading.local()


def _reset_lock_in_child() -> None:
    """A forked worker gets a lock of its own: a thread of the parent may hold it."""
    global _CONNECTION_LOCK
    _CONNECTION_LOCK = threading.Lock()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_reset_lock_in_child)


def sql_string(value: str) -> str:
    """*value* as an SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def _configure(connection, state_dir: str, memory_limit: str) -> None:
    """Cap the memory of *connection*, which spills to the state directory."""
    set_memory_limit(connection, memory_limit)
    # Without it, DuckDB would spill to a '.tmp' directory of the current one.
    temp_directory = os.path.join(state_dir, f"duckdb-{os.getpid()}.tmp")
    connection.execute(f"SET temp_directory = {sql_string(temp_directory)}")


def set_memory_limit(connection, memory_limit: str) -> None:
    """Set the 'state_memory_limit' option as the memory limit of *connection*."""
    try:
        connection.execute(f"SET memory_limit = {sql_string(memory_limit)}")
    except duckdb.Error as exc:
        raise ValueError(
            f"Option 'state_memory_limit' is '{memory_limit}', which is not a "
            "valid amount of memory, e.g. '512MB' or '2GB': "
            f"{str(exc).splitlines()[0]}"
        ) from exc


def check_memory_limit(memory_limit: str) -> None:
    """Raise a ValueError unless DuckDB takes *memory_limit* for an amount of memory."""
    probe = duckdb.connect(":memory:", config={"threads": 1})
    try:
        set_memory_limit(probe, memory_limit)
    finally:
        probe.close()


def connection(state_dir: str, memory_limit: str):
    """
    The DuckDB connection this process reads the tables of the run in
    *state_dir* with, opened on first use, with a single thread: the worker
    processes already run in parallel.
    """
    key = (os.getpid(), state_dir, memory_limit)
    with _CONNECTION_LOCK:
        base = _CONNECTIONS.get(key)
        if base is None:
            base = duckdb.connect(":memory:", config={"threads": 1})
            _configure(base, state_dir, memory_limit)
            _CONNECTIONS[key] = base

    cursors = getattr(_CURSORS, "cursors", None)
    if cursors is None:
        cursors = _CURSORS.cursors = {}
    cursor = cursors.get(key)
    if cursor is None or cursor[0] is not base:
        # The cursors of the connections closed since are dropped on the way.
        for stale in [k for k, (b, _) in cursors.items() if _CONNECTIONS.get(k) is not b]:
            del cursors[stale]
        cursor = cursors[key] = (base, base.cursor())
    return cursor[1]


def close_connections(state_dir: Optional[str] = None) -> None:
    """
    Close the DuckDB connection this process opened for the run in
    *state_dir*, or every connection it opened.
    """
    with _CONNECTION_LOCK:
        for key in [k for k in _CONNECTIONS if state_dir is None or k[1] == state_dir]:
            previous = _CONNECTIONS.pop(key)
            # A connection inherited from the parent process is not this
            # process's to close.
            if key[0] == os.getpid():
                previous.close()


@dataclass(frozen=True)
class ParquetTable:
    """A table of a shared context, kept on disk as a Parquet file."""

    path: str
    memory_limit: str

    @property
    def state_dir(self) -> str:
        return os.path.dirname(self.path)

    def select(self, sql: str, parameters: Optional[list] = None, /, **frames) -> list[tuple]:
        """
        Run the SELECT query *sql*, in which ``{table}`` stands for this table,
        and return its rows. Values go in the query as ``?`` placeholders, bound
        to *parameters* in order. *frames* are pandas DataFrames the query reads
        under their keyword, e.g. the values to look up.
        """
        cursor = connection(self.state_dir, self.memory_limit)
        for name, frame in frames.items():
            cursor.register(name, frame)
        try:
            query = sql.replace("{table}", f"read_parquet({sql_string(self.path)})")
            return cursor.execute(query, parameters or []).fetchall()
        finally:
            for name in frames:
                cursor.unregister(name)


class TableBuilder:
    """
    A scratch DuckDB database on disk, in the state directory of the run, that
    rows are streamed into before the Parquet files of a context are written
    out of them: whatever the number of rows, only a batch of them is held in
    memory, and DuckDB spills to disk past the memory limit.
    """

    def __init__(self, state_dir: str, memory_limit: str) -> None:
        self.state_dir = state_dir
        self.memory_limit = memory_limit
        descriptor, self._path = tempfile.mkstemp(
            prefix="build-", suffix=".duckdb", dir=state_dir,
        )
        os.close(descriptor)
        os.remove(self._path)
        # Closed before the worker processes are forked, as DuckDB's threads
        # would not survive the fork.
        # A single thread: each thread of DuckDB keeps a hash table and a sort of
        # its own, which would not fit in the memory limit together.
        self._connection = duckdb.connect(self._path, config={"threads": 1})
        _configure(self._connection, state_dir, memory_limit)
        self._batches: dict[str, list[tuple]] = {}
        self._columns: dict[str, tuple[str, ...]] = {}

    def __enter__(self) -> "TableBuilder":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def create(self, name: str, columns: dict[str, str]) -> None:
        """Create the scratch table *name* with *columns* (name -> SQL type)."""
        definition = ", ".join(f'"{column}" {sql_type}' for column, sql_type in columns.items())
        self._connection.execute(f'CREATE TABLE "{name}" ({definition})')
        self._columns[name] = tuple(columns)
        self._batches[name] = []

    def append(self, name: str, row: tuple) -> None:
        """Add *row* to the scratch table *name*."""
        batch = self._batches[name]
        batch.append(row)
        if len(batch) >= BATCH_ROWS:
            self.flush(name)

    def flush(self, name: Optional[str] = None) -> None:
        """Move the rows held in memory to the scratch tables on disk."""
        for table in [name] if name else list(self._batches):
            batch = self._batches[table]
            if not batch:
                continue
            # Values are kept as they are: inferring the column types would
            # turn an integer column holding a None into floats.
            frame = pd.DataFrame(batch, columns=self._columns[table], dtype=object)
            self._connection.register("_batch", frame)
            try:
                self._connection.execute(f'INSERT INTO "{table}" SELECT * FROM _batch')
            finally:
                self._connection.unregister("_batch")
            batch.clear()

    def fetch(self, sql: str) -> list[tuple]:
        """The rows of the query *sql* over the scratch tables."""
        self.flush()
        return self._connection.execute(sql).fetchall()

    def write(self, sql: str, prefix: str = "table-") -> ParquetTable:
        """Write the rows of the query *sql* to a new Parquet file of the run."""
        self.flush()
        descriptor, path = tempfile.mkstemp(prefix=prefix, suffix=".parquet", dir=self.state_dir)
        os.close(descriptor)
        self._connection.execute(f"COPY ({sql}) TO {sql_string(path)} (FORMAT PARQUET)")
        return ParquetTable(path=path, memory_limit=self.memory_limit)

    def close(self) -> None:
        """Close and remove the scratch database."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
        for path in (self._path, f"{self._path}.wal"):
            if os.path.exists(path):
                os.remove(path)


def write_table(
    state_dir: str,
    memory_limit: str,
    rows: Iterable[tuple],
    columns: dict[str, str],
    *,
    order_by: tuple[str, ...] = (),
    distinct: bool = False,
) -> ParquetTable:
    """
    Write *rows* (an iterable, consumed as it goes) to a Parquet file of the
    run, as a table of *columns* (name -> SQL type), sorted by *order_by*: a
    file sorted by the column it is looked up by lets DuckDB skip the parts of
    it that cannot match.
    """
    with TableBuilder(state_dir, memory_limit) as builder:
        builder.create("rows", columns)
        for row in rows:
            builder.append("rows", tuple(row))
        query = f"SELECT {'DISTINCT ' if distinct else ''}* FROM rows"
        if order_by:
            query += " ORDER BY " + ", ".join(f'"{column}"' for column in order_by)
        return builder.write(query)
