from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
FNML Executor
=============
Evaluates one rml:functionExecution against a source DataFrame and writes
the result into a new column named after the execution id.

Public API
----------
execute_fnml(data, execution, config, in_recursion=False) -> pd.DataFrame
"""

import pandas as pd

from ..constants import RML_EXECUTION
from ..utils import remove_null_values_from_dataframe

from .model          import FNMLExecution
from .registry       import FunctionRegistry
from .param_resolver import resolve_params
from .state          import get_context


def execute_fnml(
    data: pd.DataFrame,
    execution: FNMLExecution,
    config=None,
    in_recursion: bool = False,
) -> pd.DataFrame:
    """
    Execute *execution* against *data* rows and write the result into
    ``data[execution.execution_id]``.
    """

    registered = FunctionRegistry.get(execution.function_iri, config)

    # Recursively evaluate any nested function executions first so their
    # results are available as columns in data before resolving parameters.
    for ib in execution.inputs:
        for vb in ib.values:
            if vb.map_type != RML_EXECUTION:
                continue

            nested = vb.nested_execution

            if nested is None:
                raise KeyError(
                    f"Nested execution {vb.map_value!r} was not attached "
                    f"to execution {execution.execution_id!r}."
                )

            data = execute_fnml(
                data,
                nested,
                config,
                in_recursion=True,
            )

    params   = resolve_params(data, execution, config, registered.parameters)
    function = registered.function

    # The shared context of a stateful function is read once per execution and
    # handed to the transformation function on every row.
    fixed_params = {}
    if registered.is_stateful:
        fixed_params[registered.context_parameter] = get_context(
            execution.function_iri, config
        )

    exec_res = []
    for i in range(len(data)):
        row_params = {k: v[i] for k, v in params.items()}
        row_params.update(fixed_params)
        exec_res.append(function(**row_params))

    data[execution.execution_id] = exec_res

    data = remove_null_values_from_dataframe(
        data, config, execution.execution_id, column=execution.execution_id
    )

    if not in_recursion:
        # explode list results only at the outermost call
        data = data.explode(execution.execution_id)

    return data
