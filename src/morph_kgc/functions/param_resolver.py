from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Parameter Resolver
==================
Converts FNMLExecution.inputs into the concrete per-row parameter arrays
that the function callable expects.

Public API
----------
resolve_params(data, execution, config, decorator_params) -> dict[str, list]
  Returns {function_param_name: [value_per_row, ...], ...}
"""

import re

import pandas as pd

from ..constants import RML_CONSTANT, RML_TEMPLATE, RML_REFERENCE, RML_EXECUTION
from ..utils import get_references_in_template
from .model import FNMLExecution, InputBinding, ValueBinding

# The number of a position: 1, 2, ..., without leading zeros.
_POSITION = re.compile(r"[1-9][0-9]*")


class MergedAliases(tuple):
    """
    Accepted parameter IRIs whose values are all taken, under whichever of them
    they are bound, rather than those of the first one bound only. For a
    parameter meant to be bound several times, such as the attributes a value
    is matched against.
    """


class NumberedParameter(tuple):
    """
    Accepted prefixes of a parameter bound once per position, under IRIs
    numbered from 1: ``<prefix>1``, ``<prefix>2``, ... for any of the prefixes,
    the prefix alone standing for ``<prefix>1``. The argument holds, for each
    row, the tuple of the values bound to every position, in order. For a
    parameter taking any number of values that only make sense in order, such
    as the values an entity is matched by, since RDF puts no order on the
    inputs of an execution.
    """

    def position(self, parameter_iri: str) -> int | None:
        """The position *parameter_iri* binds, or None when it is none of them."""
        for prefix in self:
            if parameter_iri == prefix:
                return 1
            number = parameter_iri[len(prefix):] if parameter_iri.startswith(prefix) else ""
            if _POSITION.fullmatch(number):
                return int(number)
        return None

    def bound_positions(self, parameter_iris) -> dict[int, list[str]]:
        """The positions *parameter_iris* bind, and the IRIs binding each."""
        positions: dict[int, list[str]] = {}
        for parameter_iri in parameter_iris:
            position = self.position(parameter_iri)
            if position is not None:
                positions.setdefault(position, []).append(parameter_iri)
        return positions


def _expand_template(data: pd.DataFrame, template: str) -> list:
    """Resolve an rml:template against *data* rows, return list of strings."""
    references = get_references_in_template(template)
    template   = template.replace("\\{", "{").replace("\\}", "}")

    result = pd.Series([""] * len(data), index=data.index)
    for ref in references:
        parts  = template.split("{" + ref + "}")
        result = result + parts[0] + data[ref].astype(str)
        template = ("{" + ref + "}").join(parts[1:])
    if template:
        result = result + template
    return list(result)


def _resolve_value_binding(
    data: pd.DataFrame,
    vb: ValueBinding,
) -> list:
    """Turn one ValueBinding into a per-row value list (non-EXECUTION types)."""
    if vb.map_type == RML_CONSTANT:
        return [vb.map_value] * len(data)
    if vb.map_type == RML_TEMPLATE:
        return _expand_template(data, vb.map_value)
    # RML_REFERENCE (or fallback)
    return list(data[vb.map_value])


def _build_param_lookup(execution: FNMLExecution) -> dict[str, list[ValueBinding]]:
    """
    Group the value bindings of *execution* by parameter IRI.

    A parameter may be bound by several rml:input nodes (or by a single
    rml:input holding several rml:inputValueMap nodes); both spellings mean the
    same thing, so their value bindings are merged here.
    """
    lookup: dict[str, list[ValueBinding]] = {}
    for ib in execution.inputs:
        lookup.setdefault(ib.parameter_iri, []).extend(ib.values)
    return lookup


def _bound_values(
    param_lookup: dict[str, list[ValueBinding]],
    param_iris,
) -> list[ValueBinding]:
    """
    Return the value bindings of the first declared parameter IRI that the
    mapping actually binds. A kwarg may declare several accepted IRIs (aliases),
    in which case they are tried in declaration order; when declared as
    :class:`MergedAliases`, the value bindings of all of them are returned.
    """
    if isinstance(param_iris, str):
        param_iris = (param_iris,)

    if isinstance(param_iris, MergedAliases):
        return [
            value
            for param_iri in param_iris
            for value in param_lookup.get(param_iri, [])
        ]

    for param_iri in param_iris:
        if param_iri in param_lookup:
            return param_lookup[param_iri]

    return []


def resolve_params(
    data: pd.DataFrame,
    execution: FNMLExecution,
    config,
    decorator_params: dict,
) -> dict[str, list]:
    """
    Build the concrete parameter dict expected by the function callable.

    decorator_params maps function_kwarg_name -> parameter_IRI, or to a tuple of
    accepted parameter IRIs (sourced from the @bif / @udf decorator), or to a
    MergedAliases of them, or to a NumberedParameter, whose argument is the
    tuple of the values of every position for each row.

    For each kwarg the resolver:
      1. Finds the value bindings of the matching parameter IRI.
      2. Resolves each ValueBinding to a per-row list.
      3. If there are multiple ValueBindings (array parameter), wraps
         the per-row values into a list per row.
    """
    param_lookup = _build_param_lookup(execution)

    result: dict[str, list] = {}

    for kwarg_name, param_iris in decorator_params.items():
        if isinstance(param_iris, NumberedParameter):
            positions = param_iris.bound_positions(param_lookup)
            if not positions:
                continue
            # The value bindings of each position, under the IRIs binding it.
            columns = [
                _per_row_values(data, [
                    value
                    for parameter_iri in positions[position]
                    for value in param_lookup[parameter_iri]
                ])
                for position in sorted(positions)
            ]
            result[kwarg_name] = [tuple(row) for row in zip(*columns)]
            continue

        values = _bound_values(param_lookup, param_iris)
        if not values:
            # parameter not provided — skip (function will use its default)
            continue

        result[kwarg_name] = _per_row_values(data, values)

    return result


def _per_row_values(data: pd.DataFrame, values: list[ValueBinding]) -> list:
    """
    The value of a parameter bound by *values* for each row of *data*: the
    value of its single value binding, or the list of the values of them all.
    """
    # Filter out EXECUTION bindings — those were already evaluated
    # recursively before this call and the result lives in data columns.
    non_exec = [vb for vb in values if vb.map_type != RML_EXECUTION]
    exec_vbs = [vb for vb in values if vb.map_type == RML_EXECUTION]

    if exec_vbs:
        # Execution result is already a column in data
        per_row = [list(data[vb.map_value]) for vb in exec_vbs]
        per_row += [_resolve_value_binding(data, vb) for vb in non_exec]
    else:
        per_row = [_resolve_value_binding(data, vb) for vb in non_exec]

    if len(per_row) == 1:
        return per_row[0]

    # array parameter: zip across the multiple value lists
    return [
        list(vals) if len(vals) > 1 else vals[0]
        for vals in zip(*per_row)
    ]
