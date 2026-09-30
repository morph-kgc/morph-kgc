"""
formats/rml.py – Load an RML 1.0 / RML 1.2 file (or any RDF serialisation)
into an rdflib.Graph.

This module only *loads* the graph from a file; normalisation lives in
normalizer.py so that R2RML, RML-legacy, and YARRRML loaders all pass
through the same pipeline.
"""

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

import rdflib

from ...constants import (
    RML_LEGACY_LOGICAL_SOURCE, RML_LOGICAL_SOURCE,
    RML_LEGACY_SOURCE,         RML_SOURCE,
    RML_LEGACY_QUERY,          RML_QUERY,
    RML_LEGACY_ITERATOR,       RML_ITERATOR,
    RML_LEGACY_REFERENCE,      RML_REFERENCE,
    RML_LEGACY_REFERENCE_FORMULATION, RML_REFERENCE_FORMULATION,
    RML_LEGACY_SUBJECT_MAP,    RML_SUBJECT_MAP,
    RML_LEGACY_OBJECT_MAP,     RML_OBJECT_MAP,
    FNML_EXECUTION,            RML_EXECUTION,
    FNML_INPUT,                RML_INPUT,
    FNML_FUNCTION_MAP,         RML_FUNCTION_MAP,
    FNML_RETURN_MAP,           RML_RETURN_MAP,
    FNML_PARAMETER_MAP,        RML_PARAMETER_MAP,
    FNML_VALUE_MAP,            RML_VALUE_MAP,
    FNML_FUNCTION_SHORTCUT,    RML_FUNCTION_SHORTCUT,
    FNML_RETURN_SHORTCUT,      RML_RETURN_SHORTCUT,
    FNML_PARAMETER_SHORTCUT,   RML_PARAMETER_SHORTCUT,
    FNML_VALUE_SHORTCUT,       RML_VALUE_SHORTCUT,
)
from ...utils import replace_predicates_in_graph

_LEGACY_PREDICATE_MAP = {
    # ── RML legacy namespace (semweb.mmlab.be/ns/rml#) ────────────────────
    RML_LEGACY_LOGICAL_SOURCE:        RML_LOGICAL_SOURCE,
    RML_LEGACY_SOURCE:                RML_SOURCE,
    RML_LEGACY_QUERY:                 RML_QUERY,
    RML_LEGACY_ITERATOR:              RML_ITERATOR,
    RML_LEGACY_REFERENCE:             RML_REFERENCE,
    RML_LEGACY_REFERENCE_FORMULATION: RML_REFERENCE_FORMULATION,
    RML_LEGACY_SUBJECT_MAP:           RML_SUBJECT_MAP,
    RML_LEGACY_OBJECT_MAP:            RML_OBJECT_MAP,
    # ── FNML legacy namespace (semweb.mmlab.be/ns/fnml#) ──────────────────
    FNML_EXECUTION:        RML_EXECUTION,
    FNML_INPUT:            RML_INPUT,
    FNML_FUNCTION_MAP:     RML_FUNCTION_MAP,
    FNML_RETURN_MAP:       RML_RETURN_MAP,
    FNML_PARAMETER_MAP:    RML_PARAMETER_MAP,
    FNML_VALUE_MAP:        RML_VALUE_MAP,
    FNML_FUNCTION_SHORTCUT:  RML_FUNCTION_SHORTCUT,
    FNML_RETURN_SHORTCUT:    RML_RETURN_SHORTCUT,
    FNML_PARAMETER_SHORTCUT: RML_PARAMETER_SHORTCUT,
    FNML_VALUE_SHORTCUT:     RML_VALUE_SHORTCUT,
}


def _rml_legacy_to_rml(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """
    Replace legacy RML / FNML predicate URIs with their current equivalents.
    Called by load_rml_graph immediately after parsing, before any normalisation.
    """
    for legacy, current in _LEGACY_PREDICATE_MAP.items():
        mapping_graph = replace_predicates_in_graph(mapping_graph, legacy, current)
    return mapping_graph


def load_rml_graph(file_path: str) -> rdflib.Graph:
    """
    Parse an RML mapping file and return a normalised rdflib Graph.
    Handles .rml, .ttl, .n3, .nt, .trig, .nq, .jsonld and .owl files.
    Files with an unrecognised extension are attempted as Turtle.
    """
    g = rdflib.Graph()
    try:
        g.parse(file_path)
    except Exception:
        # unrecognised extension — fall back to Turtle (issue #80)
        g.parse(file_path, format='turtle')
    return _rml_legacy_to_rml(g)