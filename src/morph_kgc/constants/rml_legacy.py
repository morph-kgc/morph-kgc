__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML legacy namespace constants (http://semweb.mmlab.be/ns/rml#).

Used ONLY in formats/rml.py during the backward-compatibility translation
pass that rewrites legacy URIs to their RML-Core / RML 1.2 equivalents.
No other module should import from here.
"""

RML_LEGACY_NAMESPACE = "http://semweb.mmlab.be/ns/rml#"

# ── Core legacy properties ────────────────────────────────────────────────────
RML_LEGACY_LOGICAL_SOURCE        = f"{RML_LEGACY_NAMESPACE}logicalSource"
RML_LEGACY_SOURCE                = f"{RML_LEGACY_NAMESPACE}source"
RML_LEGACY_QUERY                 = f"{RML_LEGACY_NAMESPACE}query"
RML_LEGACY_ITERATOR              = f"{RML_LEGACY_NAMESPACE}iterator"
RML_LEGACY_REFERENCE             = f"{RML_LEGACY_NAMESPACE}reference"
RML_LEGACY_REFERENCE_FORMULATION = f"{RML_LEGACY_NAMESPACE}referenceFormulation"
RML_LEGACY_SUBJECT_MAP           = f"{RML_LEGACY_NAMESPACE}subjectMap"
RML_LEGACY_OBJECT_MAP            = f"{RML_LEGACY_NAMESPACE}objectMap"

# ── QL namespace (legacy reference formulations) ─────────────────────────────
QL_NAMESPACE = "http://semweb.mmlab.be/ns/ql#"
QL_CSV       = f"{QL_NAMESPACE}CSV"
QL_JSON      = f"{QL_NAMESPACE}JSONPath"
QL_XML       = f"{QL_NAMESPACE}XPath"
