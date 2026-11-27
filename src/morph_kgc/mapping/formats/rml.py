"""
formats/rml.py – Load an RML 1.0 / RML 1.2 file (or any RDF serialisation)
into an rdflib.Graph.

This module only *loads* the graph from a file; normalisation lives in
normalizer.py so that R2RML, RML-legacy, and YARRRML loaders all pass
through the same pipeline.
"""

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

import os

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
    RML_RELATIVE_PATH_SOURCE_CLASS, RML_FILE_PATH_CLASS,
    RML_PATH, RML_ROOT, RML_CURRENT_WORKING_DIRECTORY, RML_MAPPING_DIRECTORY,
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
    g = _rml_legacy_to_rml(g)
    return _resolve_relative_path_sources(g, file_path)


def _resolve_relative_path_sources(mapping_graph: rdflib.Graph, file_path: str) -> rdflib.Graph:
    """
    Replace every RML-IO source description with an ``rml:path``
    (``rml:RelativePathSource``, ``rml:FilePath``) by the path it denotes,
    resolved against its ``rml:root``:

    - ``rml:MappingDirectory``: the directory of the mapping file;
    - ``rml:CurrentWorkingDirectory`` (the default): the path as it is;
    - a literal: that directory.

    The logical source then has a path literal as ``rml:source``, as in RML 1.0.
    """
    rml_source = rdflib.URIRef(RML_SOURCE)
    rml_path = rdflib.URIRef(RML_PATH)
    rml_root = rdflib.URIRef(RML_ROOT)
    source_classes = {rdflib.URIRef(RML_RELATIVE_PATH_SOURCE_CLASS), rdflib.URIRef(RML_FILE_PATH_CLASS)}
    for logical_source, source in list(mapping_graph.subject_objects(rml_source)):
        if isinstance(source, rdflib.Literal):
            continue
        path = mapping_graph.value(source, rml_path)
        types = set(mapping_graph.objects(source, rdflib.RDF.type))
        if path is None or (types and not types & source_classes):
            continue
        root = mapping_graph.value(source, rml_root)
        if root == rdflib.URIRef(RML_MAPPING_DIRECTORY):
            resolved = os.path.join(os.path.dirname(os.path.abspath(file_path)), str(path))
        elif root is None or root == rdflib.URIRef(RML_CURRENT_WORKING_DIRECTORY):
            resolved = str(path)
        else:
            resolved = os.path.join(str(root), str(path))
        mapping_graph.remove((logical_source, rml_source, source))
        mapping_graph.add((logical_source, rml_source, rdflib.Literal(resolved)))
        # drop the description if no other logical source shares it
        if (None, rml_source, source) not in mapping_graph:
            for triple in list(mapping_graph.triples((source, None, None))):
                mapping_graph.remove(triple)
    return mapping_graph