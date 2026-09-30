__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
The grammar of a function call. The RDF predicates the parser looks for when reading a mapping graph:
rml:functionExecution, rml:input, rml:functionMap, rml:parameterMap, rml:inputValueMap, their shortcut forms, the FnO 
ontology terms, and the legacy fnml: namespace used as translation targets. These describe how a function call is 
written in RML — they're identical no matter which function is being called. Consumed by code that parses mappings.

FnO  — https://w3id.org/function/ontology#
FNML — via RML namespace http://w3id.org/rml/
FNML legacy — http://semweb.mmlab.be/ns/fnml#  (translation targets only)
"""

from .rml import RML_NAMESPACE

# ── FnO ontology ──────────────────────────────────────────────────────────────
FNO_NAMESPACE = "https://w3id.org/function/ontology#"

FNO_FUNCTION  = f"{FNO_NAMESPACE}Function"
FNO_EXECUTION = f"{FNO_NAMESPACE}Execution"
FNO_PARAMETER = f"{FNO_NAMESPACE}Parameter"
FNO_OUTPUT    = f"{FNO_NAMESPACE}Output"

FNO_EXECUTES  = f"{FNO_NAMESPACE}executes"
FNO_PREDICATE = f"{FNO_NAMESPACE}predicate"
FNO_TYPE      = f"{FNO_NAMESPACE}type"
FNO_REQUIRED  = f"{FNO_NAMESPACE}required"
FNO_NAME      = f"{FNO_NAMESPACE}name"
FNO_SOLVES    = f"{FNO_NAMESPACE}solves"
FNO_EXPECTS   = f"{FNO_NAMESPACE}expects"
FNO_RETURNS   = f"{FNO_NAMESPACE}returns"

# ── FNML (current, via rml: namespace) ───────────────────────────────────────
RML_EXECUTION          = f"{RML_NAMESPACE}functionExecution"
RML_INPUT              = f"{RML_NAMESPACE}input"
RML_FUNCTION_MAP       = f"{RML_NAMESPACE}functionMap"
RML_RETURN_MAP         = f"{RML_NAMESPACE}returnMap"
RML_PARAMETER_MAP      = f"{RML_NAMESPACE}parameterMap"
RML_VALUE_MAP          = f"{RML_NAMESPACE}inputValueMap"
RML_FUNCTION_SHORTCUT  = f"{RML_NAMESPACE}function"
RML_RETURN_SHORTCUT    = f"{RML_NAMESPACE}return"
RML_PARAMETER_SHORTCUT = f"{RML_NAMESPACE}parameter"
RML_VALUE_SHORTCUT     = f"{RML_NAMESPACE}inputValue"

# ── FNML legacy (http://semweb.mmlab.be/ns/fnml#) — translation targets only ─
FNML_NAMESPACE         = "http://semweb.mmlab.be/ns/fnml#"
FNML_EXECUTION         = f"{FNML_NAMESPACE}execution"
FNML_INPUT             = f"{FNML_NAMESPACE}input"
FNML_FUNCTION_MAP      = f"{FNML_NAMESPACE}functionMap"
FNML_RETURN_MAP        = f"{FNML_NAMESPACE}returnMap"
FNML_PARAMETER_MAP     = f"{FNML_NAMESPACE}parameterMap"
FNML_VALUE_MAP         = f"{FNML_NAMESPACE}valueMap"
FNML_FUNCTION_SHORTCUT  = f"{FNML_NAMESPACE}function"
FNML_RETURN_SHORTCUT    = f"{FNML_NAMESPACE}return"
FNML_PARAMETER_SHORTCUT = f"{FNML_NAMESPACE}parameter"
FNML_VALUE_SHORTCUT     = f"{FNML_NAMESPACE}value"
