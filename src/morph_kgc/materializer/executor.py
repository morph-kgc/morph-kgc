from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Execution Strategy
==================
Three concrete executors implement the Executor protocol:

SequentialExecutor   — single-process, for-loop
MultiprocessExecutor — mp.Pool.starmap (Linux / CLI only)
AsyncExecutor        — asyncio.gather with a thread-pool bridge

All three share the same signature::

    executor.run(groups, materialize_fn, rml_mapping, config, **kwargs) -> list[Any]

where *materialize_fn* is one of the group-level functions from
materializer.pipeline (materialize_group_to_set / _to_file).

Protocol
--------
Runtime typing via typing.Protocol so callers can accept any executor
without importing concrete classes::

    def materialize_pipeline(config, executor: Executor | None = None): ...
"""

import asyncio
import logging
import multiprocessing as mp
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Protocol, runtime_checkable

from ..constants import LOGGING_NAMESPACE
from ..mapping.model import RMLRule, RMLMapping

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

# ── Protocol ──────────────────────────────────────────────────────────────────

@runtime_checkable
class Executor(Protocol):
    """
    Execution strategy protocol.

    Parameters
    ----------
    groups:
        Iterable of rule groups (list[RMLRule]) produced by the partitioner.
    materialize_fn:
        Callable(group, rml_mapping, config, **kwargs) -> Any
    rml_mapping:
        Parsed RMLMapping — passed through to materialize_fn.
    config:
        Morph-KGC configuration object.
    kwargs:
        Extra keyword arguments forwarded to materialize_fn
        (e.g. python_source for in-memory data).
    """

    def run(
        self,
        groups: list[list[RMLRule]],
        materialize_fn: Callable,
        rml_mapping: RMLMapping,
        config,
        **kwargs,
    ) -> list[Any]: ...


# ── Sequential ────────────────────────────────────────────────────────────────

class SequentialExecutor:
    """
    Single-process for-loop executor.
    Always used when running as a library on non-Linux platforms (see #94)
    and whenever number_of_processes == 1.
    """

    def run(
        self,
        groups: list[list[RMLRule]],
        materialize_fn: Callable,
        rml_mapping: RMLMapping,
        config,
        **kwargs,
    ) -> list[Any]:
        results = []
        for group in groups:
            results.append(materialize_fn(group, rml_mapping, config, **kwargs))
        return results


# ── Multiprocess ──────────────────────────────────────────────────────────────

class MultiprocessExecutor:
    """
    mp.Pool-based executor.

    Uses starmap so each worker receives its own copy of rml_mapping and
    config (pickle-safe). Only safe on Linux (fork semantics) — the pipeline
    gates this with a platform check before constructing this class.
    """

    def __init__(self, n_processes: int) -> None:
        self.n_processes = n_processes

    def run(
        self,
        groups: list[list[RMLRule]],
        materialize_fn: Callable,
        rml_mapping: RMLMapping,
        config,
        **kwargs,
    ) -> list[Any]:
        LOGGER.debug(f"MultiprocessExecutor: spawning {self.n_processes} workers.")

        # Build positional args list for starmap; kwargs are flattened in
        # materialize_fn wrappers that accept python_source as a positional arg.
        python_source = kwargs.get("python_source")
        args = [
            (group, rml_mapping, config, python_source)
            for group in groups
        ]

        with mp.Pool(self.n_processes) as pool:
            results = pool.starmap(materialize_fn, args)

        return results


# ── Async ─────────────────────────────────────────────────────────────────────

class AsyncExecutor:
    """
    asyncio.gather-based executor.

    Each group is submitted as a coroutine that delegates blocking I/O to a
    ThreadPoolExecutor, so the event loop stays responsive while data sources
    are being fetched.

    Usage::

        executor = AsyncExecutor(max_workers=8)
        results = executor.run(groups, materialize_fn, rml_mapping, config)

    When called from an already-running event loop (e.g. Jupyter), the
    executor transparently falls back to nest_asyncio if available, otherwise
    raises RuntimeError with an actionable message.
    """

    def __init__(self, max_workers: int | None = None) -> None:
        self.max_workers = max_workers

    def run(
        self,
        groups: list[list[RMLRule]],
        materialize_fn: Callable,
        rml_mapping: RMLMapping,
        config,
        **kwargs,
    ) -> list[Any]:
        coro = self._run_async(groups, materialize_fn, rml_mapping, config, **kwargs)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            try:
                import nest_asyncio
                nest_asyncio.apply()
                return loop.run_until_complete(coro)
            except ImportError:
                raise RuntimeError(
                    "AsyncExecutor was called from a running event loop "
                    "(e.g. Jupyter). Install nest_asyncio to enable this: "
                    "pip install nest_asyncio"
                )

        return asyncio.run(coro)

    async def _run_async(
        self,
        groups: list[list[RMLRule]],
        materialize_fn: Callable,
        rml_mapping: RMLMapping,
        config,
        **kwargs,
    ) -> list[Any]:
        loop = asyncio.get_event_loop()

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            tasks = [
                loop.run_in_executor(
                    pool,
                    lambda g=group: materialize_fn(g, rml_mapping, config, **kwargs),
                )
                for group in groups
            ]

            results = await asyncio.gather(*tasks)

        return list(results)


# ── Factory ───────────────────────────────────────────────────────────────────

def make_executor(config) -> Executor:
    """
    Instantiate the correct executor from config, applying the platform guard
    for multiprocessing (non-Linux library usage falls back to sequential).

    Decision table
    --------------
    number_of_processes == 1               -> SequentialExecutor
    number_of_processes > 1 on Linux       -> MultiprocessExecutor
    number_of_processes > 1 non-Linux      -> SequentialExecutor + warning
    async_enabled (future config key)      -> AsyncExecutor
    """

    if config.is_multiprocessing_enabled():
        if "linux" in sys.platform:
            return MultiprocessExecutor(config.number_of_processes)
        else:
            LOGGER.info(
                f"Parallelization is not supported for {sys.platform} when "
                "running as a library. For parallel execution, use the CLI. "
                "Falling back to SequentialExecutor."
            )
            return SequentialExecutor()

    return SequentialExecutor()
