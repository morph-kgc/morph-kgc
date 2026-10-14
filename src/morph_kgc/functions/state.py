from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Shared context of stateful functions
====================================
A stateful function declares an *initializer*: a callable executed exactly once,
before any triple is materialized, whose return value becomes a shared,
immutable context reused by every invocation of the function.

The context is persisted to disk (one pickle file per function) rather than kept
in a service or a background process, so that the worker processes spawned by
the multiprocess executor read it back instead of rebuilding it. Each worker
loads a context at most once, the first time it executes the function, and
keeps it in memory afterwards.

A context too large to hold in memory keeps its data in Parquet files the
initializer writes to the state directory of the run, and only holds handles
to them (see :mod:`morph_kgc.functions.tables`): the reconciliation functions
keep their indexes that way, so that their memory does not grow with the size
of the vocabulary or of the answer of the SPARQL endpoint.

The files go to a directory of their own for every run, created inside the
configured ``state_dir`` (a disk with room for large contexts, for instance) or
in the system temporary directory when none is configured, and removed when the
run ends. ``state_dir`` is therefore not a cache reused across runs, and runs
sharing it never read each other's contexts.

Lifecycle
---------
initialize_contexts(config, rml_mapping) -> ContextSession
    Pre-execution step: runs every initializer needed by the mapping and writes
    the resulting contexts to the state directory of the run.

get_context(function_iri, config) -> Any
    Execution step: reads (once per process) the context of a function.

release_contexts(session)
    Post-execution step: forgets the contexts of the run, closes the DuckDB
    connection of the process and removes the state directory of the run.
"""

import logging
import os
import pickle
import shutil
import tempfile
from dataclasses import dataclass, field
from hashlib import sha256
from inspect import signature
from typing import Any, Optional

from ..config.defaults import DEFAULT_STATE_MEMORY_LIMIT
from ..constants import LOGGING_NAMESPACE, RML_CONSTANT
from .model import FNMLExecution
from .registry import FunctionRegistry
from .tables import ParquetTable, close_connections, write_table

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

# Contexts already read by this process, keyed by (state directory, function
# IRI): two materializations in the same process must not share a context.
_LOADED_CONTEXTS: dict[tuple[str, str], Any] = {}


# ── Initialization context handed to initializers ─────────────────────────────

@dataclass(frozen=True)
class ExecutionParameters:
    """
    What one execution of a function binds to each parameter IRI, as far as it
    is known before any data is read: the constant values, in mapping order,
    and the parameter IRIs bound to the data (a reference, a template or
    another execution), whose values are only known row by row.
    """

    execution_id: str
    constants: dict[str, list[str]] = field(default_factory=dict)
    bound_to_data: frozenset[str] = frozenset()


@dataclass(frozen=True)
class InitializationContext:
    """
    What an initializer is told about the mapping it is initialized for.

    Attributes
    ----------
    function_iri:
        IRI of the stateful function being initialized.
    config:
        The ``MorphConfig`` of the run, holding the declared resources.
    parameters:
        Constant values bound to each parameter IRI across every execution of
        the function in the mapping. Only ``rml:constant`` bindings appear here:
        they are the values known before any data is read, which is what an
        initializer can act on (which resource to fetch, which attributes to
        index, ...).
    executions:
        What each execution of the function in the mapping binds, so that
        values that only make sense together, such as a query and the endpoint
        it is sent to, are paired by the execution binding them.
    """

    function_iri: str
    config: Any
    parameters: dict[str, list[str]] = field(default_factory=dict)
    executions: tuple[ExecutionParameters, ...] = ()
    # The state directory of the run, where an initializer may write the files
    # its context refers to; they are removed with it when the run ends.
    state_dir: str = ""

    # -- Files ----------------------------------------------------------------

    @property
    def memory_limit(self) -> str:
        """The memory DuckDB may use in a process, as 'state_memory_limit' says."""
        return getattr(self.config, "state_memory_limit", "") or DEFAULT_STATE_MEMORY_LIMIT

    def write_table(
        self,
        rows,
        columns: dict[str, str],
        *,
        order_by: tuple[str, ...] = (),
        distinct: bool = False,
    ) -> ParquetTable:
        """
        Write *rows* (an iterable of tuples, consumed as it goes) to a Parquet
        file of the run, as a table of *columns* (name -> SQL type) sorted by
        *order_by*, with the same memory whatever their number. The returned
        :class:`~morph_kgc.functions.tables.ParquetTable` is what the context
        keeps: the function queries the file through it.
        """
        return write_table(
            self.state_dir, self.memory_limit, rows, columns,
            order_by=order_by, distinct=distinct,
        )

    # -- Constants ----------------------------------------------------------

    def constants(self, *parameter_iris: str) -> list[str]:
        """
        Constant values bound to any of *parameter_iris*, deduplicated and in
        mapping order. Several IRIs may be given when a parameter is accepted
        under more than one name.
        """
        values: list[str] = []
        for parameter_iri in parameter_iris:
            for value in self.parameters.get(parameter_iri, []):
                if value not in values:
                    values.append(value)
        return values

    # -- Resources ----------------------------------------------------------

    def resource(self, name_or_url: str):
        """
        Resolve a ``[RESOURCE:<name>]`` section by name or by URL.
        Raises ValueError when the mapping references an undeclared resource.
        """
        resource = self.config.find_resource(name_or_url)
        if resource is None:
            raise ValueError(
                f"Function '{self.function_iri}' references the resource "
                f"'{name_or_url}', which is not declared in the configuration "
                f"file. Add a '[RESOURCE:{name_or_url}]' section."
            )
        return resource

    def resources(self, *parameter_iris: str) -> dict[str, Any]:
        """
        Resolve every resource referenced through *parameter_iris*, keyed by the
        value used in the mapping (so that the transformation function can look
        a context up with the very same value).
        """
        return {
            value: self.resource(value)
            for value in self.constants(*parameter_iris)
        }


# ── State directory session ───────────────────────────────────────────────────

@dataclass
class ContextSession:
    """The state directory of one run."""

    path: str = ""
    function_iris: list[str] = field(default_factory=list)
    # The configuration of the run and the state_dir it was given, so that
    # releasing the session leaves the configuration exactly as the run found it.
    config: Any = None
    configured_state_dir: str = ""

    def __bool__(self) -> bool:
        return bool(self.function_iris)


def _context_file(state_dir: str, function_iri: str) -> str:
    """Path of the context file of *function_iri* inside *state_dir*."""
    digest = sha256(function_iri.encode("utf-8")).hexdigest()[:32]
    return os.path.join(state_dir, f"{digest}.pickle")


# ── Collecting what the mapping needs ─────────────────────────────────────────

def _walk_executions(rml_mapping) -> list[FNMLExecution]:
    """Every FNML execution of the mapping, nested executions included."""
    executions: dict[int, FNMLExecution] = {}

    def visit(execution: FNMLExecution) -> None:
        if id(execution) in executions:
            return
        executions[id(execution)] = execution
        for input_binding in execution.inputs:
            for value in input_binding.values:
                if value.nested_execution is not None:
                    visit(value.nested_execution)

    for execution in rml_mapping.fnml_executions.values():
        visit(execution)

    return list(executions.values())


def _constant_parameters(executions: list[FNMLExecution]) -> dict[str, list[str]]:
    """Constant values bound to each parameter IRI across *executions*."""
    parameters: dict[str, list[str]] = {}
    for execution in executions:
        for input_binding in execution.inputs:
            for value in input_binding.values:
                if value.map_type != RML_CONSTANT:
                    continue
                values = parameters.setdefault(input_binding.parameter_iri, [])
                if value.map_value not in values:
                    values.append(value.map_value)
    return parameters


def _execution_parameters(execution: FNMLExecution) -> ExecutionParameters:
    """What *execution* binds to each parameter IRI before any data is read."""
    constants: dict[str, list[str]] = {}
    bound_to_data: set[str] = set()
    for input_binding in execution.inputs:
        for value in input_binding.values:
            if value.map_type == RML_CONSTANT:
                constants.setdefault(input_binding.parameter_iri, []).append(value.map_value)
            else:
                bound_to_data.add(input_binding.parameter_iri)
    return ExecutionParameters(
        execution_id  = execution.execution_id,
        constants     = constants,
        bound_to_data = frozenset(bound_to_data),
    )


def _stateful_functions(rml_mapping, config) -> dict[str, list[FNMLExecution]]:
    """
    Group the executions of the mapping by stateful function IRI.

    Function IRIs the registry does not know are ignored here: they are reported
    when (and only if) the mapping rule using them is actually materialized.
    """
    executions_by_function: dict[str, list[FNMLExecution]] = {}

    for execution in _walk_executions(rml_mapping):
        executions_by_function.setdefault(execution.function_iri, []).append(execution)

    stateful: dict[str, list[FNMLExecution]] = {}
    for function_iri, executions in executions_by_function.items():
        registered = FunctionRegistry.try_get(function_iri, config)
        if registered is not None and registered.is_stateful:
            stateful[function_iri] = executions

    return stateful


# ── Pre-execution step ────────────────────────────────────────────────────────

def initialize_contexts(config, rml_mapping) -> ContextSession:
    """
    Run the initializer of every stateful function used by *rml_mapping* and
    persist the resulting contexts to disk.

    Returns the :class:`ContextSession` to hand to :func:`release_contexts`
    once materialization is over.
    """
    if not rml_mapping.fnml_executions:
        return ContextSession()

    stateful = _stateful_functions(rml_mapping, config)
    if not stateful:
        return ContextSession()

    session = _open_state_dir(config)
    # Contexts of an earlier materialization in this process are stale now.
    _LOADED_CONTEXTS.clear()

    try:
        for function_iri, executions in stateful.items():
            registered = FunctionRegistry.get(function_iri, config)

            LOGGER.info(f"Initializing the shared context of '{function_iri}'.")

            initialization = InitializationContext(
                function_iri = function_iri,
                config       = config,
                parameters   = _constant_parameters(executions),
                executions   = tuple(
                    _execution_parameters(execution) for execution in executions
                ),
                state_dir    = session.path,
            )

            if len(signature(registered.initializer).parameters) == 0:
                context = registered.initializer()
            else:
                context = registered.initializer(initialization)

            _persist_context(session.path, function_iri, context)
            session.function_iris.append(function_iri)
    except BaseException:
        # An initializer that fails must not leave a state directory behind.
        release_contexts(session)
        raise

    return session


def _open_state_dir(config) -> ContextSession:
    """
    Create the state directory of this run: a private directory inside the
    configured ``state_dir``, or inside the system temporary directory when
    none is configured.
    """
    if config.state_dir:
        os.makedirs(config.state_dir, exist_ok=True)

    # A directory of its own, so that runs sharing a configured state_dir never
    # read (or overwrite) each other's contexts.
    path = tempfile.mkdtemp(prefix="morph-kgc-state-", dir=config.state_dir or None)
    session = ContextSession(
        path                 = path,
        config               = config,
        configured_state_dir = config.state_dir,
    )
    # Workers receive the path through the config, which travels with them.
    config.state_dir = path
    return session


def _persist_context(state_dir: str, function_iri: str, context: Any) -> None:
    """
    Write the shared context of *function_iri* to *state_dir*, under a
    temporary name first, so that the context file is complete or absent.
    """
    path = _context_file(state_dir, function_iri)
    # A temporary file left behind by a failure goes with the directory of the
    # run, which a failing initialization removes.
    descriptor, temporary_path = tempfile.mkstemp(dir=state_dir, suffix=".tmp")
    try:
        with os.fdopen(descriptor, "wb") as state_file:
            pickle.dump(context, state_file, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(temporary_path, path)
    except (pickle.PicklingError, AttributeError, TypeError) as exc:
        raise ValueError(
            f"The shared context of '{function_iri}' cannot be persisted to "
            f"disk: {exc}. The value returned by an initializer must be "
            "picklable."
        ) from exc

    LOGGER.debug(f"Shared context of '{function_iri}' written to '{path}'.")


# ── Execution step ────────────────────────────────────────────────────────────

def get_context(function_iri: str, config) -> Any:
    """
    Return the shared context of *function_iri*, reading it from disk the first
    time it is needed in this process.
    """
    state_dir = getattr(config, "state_dir", "") or ""

    cache_key = (state_dir, function_iri)
    if cache_key in _LOADED_CONTEXTS:
        return _LOADED_CONTEXTS[cache_key]

    path = _context_file(state_dir, function_iri) if state_dir else ""

    if not path or not os.path.isfile(path):
        raise FileNotFoundError(
            f"The shared context of the stateful function '{function_iri}' has "
            "not been initialized. Stateful functions are initialized by the "
            "materialization pipeline; call morph_kgc.materialize* instead of "
            "executing the mapping rule directly."
        )

    with open(path, "rb") as state_file:
        context = pickle.load(state_file)   # noqa: S301  written by this engine

    _LOADED_CONTEXTS[cache_key] = context
    return context


# ── Post-execution step ───────────────────────────────────────────────────────

def release_contexts(session: Optional[ContextSession]) -> None:
    """
    Remove the state directory of the run and give the configuration back the
    ``state_dir`` the run started with — a run must leave nothing behind, on
    disk, in memory or in the configuration it was handed. A configured
    ``state_dir`` is kept, with anything else it holds.
    """
    if session is None or not session.path:
        return

    # The contexts of the run, read by this process when it materialized the
    # mapping itself, and the connection their tables were read with.
    # Other runs may be materialized at the same time in threads of this one.
    for cache_key in [key for key in list(_LOADED_CONTEXTS) if key[0] == session.path]:
        _LOADED_CONTEXTS.pop(cache_key, None)
    close_connections(session.path)

    shutil.rmtree(session.path, ignore_errors=True)
    LOGGER.debug(f"State directory '{session.path}' removed.")

    if session.config is not None and session.config.state_dir == session.path:
        session.config.state_dir = session.configured_state_dir
