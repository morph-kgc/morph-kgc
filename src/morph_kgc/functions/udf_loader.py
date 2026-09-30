from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
UDF Loader
==========
Dynamically loads user-defined functions from a Python file specified in
the config. The ``@udf`` and ``@stateful_udf`` decorators are injected into the
module namespace before it is executed, mirroring the ``@bif`` /
``@stateful_bif`` decorators used for built-in functions.

Public API
----------
load_udfs(config) -> dict[str, {"function": callable, "parameters": dict,
                               "initializer": callable | None,
                               "context_parameter": str}]
"""

from types import ModuleType
import sys

from .bif_decorator import make_decorator, make_stateful_decorator


def load_udfs(config) -> dict:
    """
    Load UDFs from the file path returned by ``config.udfs``.
    Returns an empty dict when no UDF file is configured.
    """
    udfs_path = config.udfs
    if not udfs_path:
        return {}

    with open(udfs_path, "r") as f:
        udfs_code = f.read()

    udf_dict: dict = {}

    udf_mod = ModuleType("udfs")
    udf_mod.__dict__["udf_dict"] = udf_dict
    udf_mod.__dict__["udf"] = make_decorator(udf_dict)
    udf_mod.__dict__["stateful_udf"] = make_stateful_decorator(udf_dict)
    sys.modules["udfs"] = udf_mod
    exec(udfs_code, udf_mod.__dict__)   # noqa: S102

    return udf_dict
