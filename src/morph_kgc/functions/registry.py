from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Function Registry
=================
Single source of truth for all available functions (built-in + UDFs).

FunctionRegistry.get(function_iri, config) -> RegisteredFunction
  Returns the function together with its decorator-declared parameter mapping
  and, for stateful functions, its initializer.

Built-in functions are registered via the @bif / @stateful_bif decorators.
UDFs are loaded on first access and cached per UDF file path, so that several
configurations can be materialized in the same process.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Optional

from .udf_loader import load_udfs
from .bif_decorator import *

from .grel import *              # needed to populate bif_dict
from . import reconciliation     # noqa: F401  needed to populate bif_dict


@dataclass(frozen=True, slots=True)
class RegisteredFunction:
    """A function the engine can execute, as declared by its decorator."""

    function_iri:      str
    function:          Callable
    # kwarg name -> parameter IRI (or tuple of accepted parameter IRIs)
    parameters:        dict = field(default_factory=dict)
    # Run once before materialization; its result is the shared context.
    initializer:       Optional[Callable] = None
    # Keyword argument through which the shared context is passed.
    context_parameter: str = DEFAULT_CONTEXT_PARAMETER

    @property
    def is_stateful(self) -> bool:
        return self.initializer is not None


class FunctionRegistry:
    """Lazy-loading registry for built-in functions and UDFs."""

    # UDF file path -> {function IRI: entry}
    _udf_caches: dict[str, dict] = {}

    @classmethod
    def get(cls, function_iri: str, config) -> RegisteredFunction:
        """
        Return the :class:`RegisteredFunction` for *function_iri*.
        Raises KeyError if it is neither a built-in function nor a UDF.
        """
        function = cls.try_get(function_iri, config)
        if function is None:
            raise KeyError(
                f"Function '{function_iri}' is not a built-in function and was "
                "not found in the UDFs file."
            )
        return function

    @classmethod
    def try_get(cls, function_iri: str, config) -> Optional[RegisteredFunction]:
        """Same as :meth:`get`, but returns None when the function is unknown."""
        entry = bif_dict.get(function_iri)
        if entry is None:
            entry = cls._udfs(config).get(function_iri)
        if entry is None:
            return None

        return RegisteredFunction(
            function_iri      = function_iri,
            function          = entry["function"],
            parameters        = entry["parameters"],
            initializer       = entry.get("initializer"),
            context_parameter = entry.get("context_parameter", DEFAULT_CONTEXT_PARAMETER),
        )

    @classmethod
    def _udfs(cls, config) -> dict:
        """Load (and cache) the UDFs declared by *config*."""
        udfs_path = getattr(config, "udfs", "") or ""
        if not udfs_path:
            return {}

        if udfs_path not in cls._udf_caches:
            cls._udf_caches[udfs_path] = load_udfs(config)

        return cls._udf_caches[udfs_path]
