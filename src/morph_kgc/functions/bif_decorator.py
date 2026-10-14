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

Vectorized functions
--------------------
With ``vectorized=True``, a function is called once for all the rows of a
mapping rule instead of once per row: every argument the mapping binds arrives
as a list holding its value for each row, an argument it does not bind keeps its
default, and the function returns the list of its results, one per row. It is
not called for a rule with no row. ``vectorized`` is therefore no name for a
parameter. A function looking its values up in an on-disk table of its context (see
:mod:`morph_kgc.functions.tables`) is vectorized, so that it runs one query for
all the rows rather than one per row::

    @stateful_udf(fun_id="http://example.com/lookup",
                  initializer=load_table,
                  vectorized=True,
                  value="http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam")
    def lookup(value, context):
        found = dict(context.select(
            "SELECT code, name FROM {table} WHERE code IN (SELECT code FROM codes)",
            codes=pandas.DataFrame({"code": value}, dtype=object),
        ))
        return [found.get(code) for code in value]

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
    vectorized: bool = False,
) -> None:
    """Add one function entry to *registry*, validating the declaration."""
    if not isinstance(vectorized, bool):
        raise TypeError(
            f"Function '{fun_id}' gives 'vectorized' the value {vectorized!r}: it "
            "must be True or False, and no parameter may be named 'vectorized'."
        )
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
        "vectorized": vectorized,
    }


def make_decorator(registry: dict) -> Callable:
    """Build a stateless-function decorator that registers into *registry*."""

    def decorator(fun_id, vectorized=False, **params):
        def wrapper(funct):
            _register(
                registry,
                fun_id=fun_id,
                function=funct,
                parameters=params,
                initializer=None,
                context_parameter=DEFAULT_CONTEXT_PARAMETER,
                vectorized=vectorized,
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
        vectorized=False,
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
                vectorized=vectorized,
            )
            return funct
        return wrapper

    return decorator


# ── Built-in functions ────────────────────────────────────────────────────────
# We borrow the idea of using decorators from pyRML by Andrea Giovanni Nuzzolese.

bif = make_decorator(bif_dict)
stateful_bif = make_stateful_decorator(bif_dict)
