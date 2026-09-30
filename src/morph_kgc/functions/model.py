from __future__ import annotations
from typing import Optional

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
FNML domain model
Representation of a resolved FNML execution.

FNMLExecution    — one function call: function IRI + list of InputBinding
InputBinding     — one rml:input: parameter IRI + list of ValueBinding
ValueBinding     — one rml:inputValueMap: map_type + map_value
"""

from dataclasses import dataclass, field


@dataclass(slots=True)
class ValueBinding:
    """Corresponds to one rml:inputValueMap node."""
    map_type:  str   # rml:constant | rml:template | rml:reference | rml:functionExecution
    map_value: str
    nested_execution: Optional["FNMLExecution"] = None


@dataclass(slots=True)
class InputBinding:
    """Corresponds to one rml:input node."""
    parameter_iri: str            # rml:parameterMap constant value
    # TODO: this should be a scalar
    values:        list[ValueBinding] = field(default_factory=list)


@dataclass(slots=True)
class FNMLExecution:
    """
    Fully resolved representation of one rml:functionExecution node.
    """
    execution_id: str             # blank-node or IRI identifying the execution
    function_iri: str             # rml:functionMap constant value
    inputs:       list[InputBinding] = field(default_factory=list)
