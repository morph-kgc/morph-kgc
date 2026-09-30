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

import pandas as pd

from ..constants import RML_CONSTANT, RML_TEMPLATE, RML_REFERENCE, RML_EXECUTION
from ..utils import get_references_in_template
from .model import FNMLExecution, InputBinding, ValueBinding


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
    in which case they are tried in declaration order.
    """
    if isinstance(param_iris, str):
        param_iris = (param_iris,)

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
    accepted parameter IRIs (sourced from the @bif / @udf decorator).

    For each kwarg the resolver:
      1. Finds the value bindings of the matching parameter IRI.
      2. Resolves each ValueBinding to a per-row list.
      3. If there are multiple ValueBindings (array parameter), wraps
         the per-row values into a list per row.
    """
    param_lookup = _build_param_lookup(execution)

    result: dict[str, list] = {}

    for kwarg_name, param_iris in decorator_params.items():
        values = _bound_values(param_lookup, param_iris)
        if not values:
            # parameter not provided — skip (function will use its default)
            continue

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
            result[kwarg_name] = per_row[0]
        else:
            # array parameter: zip across the multiple value lists
            result[kwarg_name] = [
                list(vals) if len(vals) > 1 else vals[0]
                for vals in zip(*per_row)
            ]

    return result
