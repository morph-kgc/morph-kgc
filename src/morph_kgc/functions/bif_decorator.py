from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Function decorators
===================
Declaration of the functions the engine can execute from an FNML mapping.

Stateless functions
-------------------
``@bif`` (built-in) and ``@udf`` (user-defined) map every keyword argument of
the callable to the parameter IRI it is bound to in the mapping::

    @bif(fun_id="http://example.com/toUpperCase",
         text="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam")
    def to_upper_case(text):
        return text.upper()

Stateful functions
------------------
``@stateful_bif`` / ``@stateful_udf`` additionally declare an *initializer*: a
callable run exactly once, before any triple is materialized, whose return
value becomes a shared, immutable context. The context is persisted to disk and
handed to the transformation function on every invocation as an extra keyword
argument (``context`` by default)::

    def load_table(initialization):
        return {...}                       # built once

    @stateful_udf(fun_id="http://example.com/lookup",
                  initializer=load_table,
                  value="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam")
    def lookup(value, context):
        return context.get(value)

An initializer takes either no argument or a single
:class:`~morph_kgc.functions.state.InitializationContext`, which exposes the
configuration, the declared resources and the constant parameter values used by
the mapping. Which of the two is used is decided from its signature.

A parameter may declare several IRIs (a tuple), in which case the mapping may
bind the argument through any of them.
"""

import inspect
from collections.abc import Callable

# Keyword argument through which a stateful function receives its context.
DEFAULT_CONTEXT_PARAMETER = "context"

bif_dict: dict[str, dict] = {}


def _register(
    registry: dict,
    fun_id: str,
    function: Callable,
    parameters: dict,
    initializer: Callable | None,
    context_parameter: str,
) -> None:
    """Add one function entry to *registry*, validating the declaration."""
    if initializer is not None:
        if not callable(initializer):
            raise TypeError(
                f"The initializer declared for function '{fun_id}' is not callable."
            )
        if len(inspect.signature(initializer).parameters) > 1:
            raise TypeError(
                f"The initializer declared for function '{fun_id}' must take no "
                "argument or a single InitializationContext argument."
            )
        if context_parameter in parameters:
            raise ValueError(
                f"Function '{fun_id}' declares '{context_parameter}' both as a "
                "mapping parameter and as its context argument."
            )

    registry[fun_id] = {
        "function": function,
        "parameters": parameters,
        "initializer": initializer,
        "context_parameter": context_parameter,
    }


def make_decorator(registry: dict) -> Callable:
    """Build a stateless-function decorator that registers into *registry*."""

    def decorator(fun_id, **params):
        def wrapper(funct):
            _register(
                registry,
                fun_id=fun_id,
                function=funct,
                parameters=params,
                initializer=None,
                context_parameter=DEFAULT_CONTEXT_PARAMETER,
            )
            return funct
        return wrapper

    return decorator


def make_stateful_decorator(registry: dict) -> Callable:
    """Build a stateful-function decorator that registers into *registry*."""

    def decorator(
        fun_id,
        initializer,
        context_parameter=DEFAULT_CONTEXT_PARAMETER,
        **params,
    ):
        def wrapper(funct):
            _register(
                registry,
                fun_id=fun_id,
                function=funct,
                parameters=params,
                initializer=initializer,
                context_parameter=context_parameter,
            )
            return funct
        return wrapper

    return decorator


# ── Built-in functions ────────────────────────────────────────────────────────
# We borrow the idea of using decorators from pyRML by Andrea Giovanni Nuzzolese.

bif = make_decorator(bif_dict)
stateful_bif = make_stateful_decorator(bif_dict)
