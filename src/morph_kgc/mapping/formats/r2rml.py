"""
formats/r2rml.py – Convert an R2RML graph to an RML 1.2 graph in-place.
"""

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

import rdflib

from ...constants import (
    RML_NAMESPACE,
    R2RML_LOGICAL_TABLE, RML_LOGICAL_SOURCE,
    R2RML_TABLE_NAME, RML_TABLE_NAME,
    R2RML_SQL_QUERY, RML_QUERY,
    R2RML_PARENT_TRIPLES_MAP, RML_PARENT_TRIPLES_MAP,
    R2RML_SUBJECT_MAP, RML_SUBJECT_MAP,
    R2RML_PREDICATE_OBJECT_MAP, RML_PREDICATE_OBJECT_MAP,
    R2RML_PREDICATE_MAP, RML_PREDICATE_MAP,
    R2RML_OBJECT_MAP, RML_OBJECT_MAP,
    R2RML_GRAPH_MAP, RML_GRAPH_MAP,
    R2RML_SUBJECT_SHORTCUT, RML_SUBJECT_SHORTCUT,
    R2RML_PREDICATE_SHORTCUT, RML_PREDICATE_SHORTCUT,
    R2RML_OBJECT_SHORTCUT, RML_OBJECT_SHORTCUT,
    R2RML_GRAPH_SHORTCUT, RML_GRAPH_SHORTCUT,
    R2RML_COLUMN, RML_REFERENCE,
    R2RML_TEMPLATE, RML_TEMPLATE,
    R2RML_CONSTANT, RML_CONSTANT,
    R2RML_CLASS, RML_CLASS,
    R2RML_CHILD, RML_CHILD,
    R2RML_PARENT, RML_PARENT,
    R2RML_JOIN_CONDITION, RML_JOIN_CONDITION,
    R2RML_DATATYPE, RML_DATATYPE_SHORTCUT,
    R2RML_LANGUAGE, RML_LANGUAGE_SHORTCUT,
    R2RML_SQL_VERSION, RML_SQL_VERSION,
    R2RML_TERM_TYPE, RML_TERM_TYPE,
    R2RML_IRI, RML_IRI,
    R2RML_LITERAL, RML_LITERAL,
    R2RML_BLANK_NODE, RML_BLANK_NODE,
    R2RML_SQL2008, RML_SQL2008,
    R2RML_DEFAULT_GRAPH, RML_DEFAULT_GRAPH,
    R2RML_GRAPH_MAP_CLASS, RML_GRAPH_MAP_CLASS,
    R2RML_JOIN_CLASS, RML_JOIN_CLASS,
    R2RML_LOGICAL_TABLE_CLASS, RML_LOGICAL_SOURCE_CLASS,
    R2RML_OBJECT_MAP_CLASS, RML_OBJECT_MAP_CLASS,
    R2RML_PREDICATE_MAP_CLASS, RML_PREDICATE_MAP_CLASS,
    R2RML_PREDICATE_OBJECT_MAP_CLASS, RML_PREDICATE_OBJECT_MAP_CLASS,
    R2RML_REF_OBJECT_MAP_CLASS, RML_REF_OBJECT_MAP_CLASS,
    R2RML_SUBJECT_MAP_CLASS, RML_SUBJECT_MAP_CLASS,
    R2RML_TERM_MAP_CLASS, RML_TERM_MAP_CLASS,
    R2RML_TRIPLES_MAP_CLASS, RML_TRIPLES_MAP_CLASS,
)
from ...utils import replace_predicates_in_graph, replace_objects_in_graph


_R2RML_TO_RML_PREDICATES: dict[str, str] = {
    R2RML_LOGICAL_TABLE:       RML_LOGICAL_SOURCE,
    R2RML_TABLE_NAME:          RML_TABLE_NAME,
    R2RML_SQL_QUERY:           RML_QUERY,
    R2RML_PARENT_TRIPLES_MAP:  RML_PARENT_TRIPLES_MAP,
    R2RML_SUBJECT_MAP:         RML_SUBJECT_MAP,
    R2RML_PREDICATE_OBJECT_MAP: RML_PREDICATE_OBJECT_MAP,
    R2RML_PREDICATE_MAP:       RML_PREDICATE_MAP,
    R2RML_OBJECT_MAP:          RML_OBJECT_MAP,
    R2RML_GRAPH_MAP:           RML_GRAPH_MAP,
    R2RML_SUBJECT_SHORTCUT:    RML_SUBJECT_SHORTCUT,
    R2RML_PREDICATE_SHORTCUT:  RML_PREDICATE_SHORTCUT,
    R2RML_OBJECT_SHORTCUT:     RML_OBJECT_SHORTCUT,
    R2RML_GRAPH_SHORTCUT:      RML_GRAPH_SHORTCUT,
    R2RML_COLUMN:              RML_REFERENCE,
    R2RML_TEMPLATE:            RML_TEMPLATE,
    R2RML_CONSTANT:            RML_CONSTANT,
    R2RML_CLASS:               RML_CLASS,
    R2RML_CHILD:               RML_CHILD,
    R2RML_PARENT:              RML_PARENT,
    R2RML_JOIN_CONDITION:      RML_JOIN_CONDITION,
    R2RML_DATATYPE:            RML_DATATYPE_SHORTCUT,
    R2RML_LANGUAGE:            RML_LANGUAGE_SHORTCUT,
    R2RML_SQL_VERSION:         RML_SQL_VERSION,
    R2RML_TERM_TYPE:           RML_TERM_TYPE,
    R2RML_IRI:                 RML_IRI,
    R2RML_LITERAL:             RML_LITERAL,
    R2RML_BLANK_NODE:          RML_BLANK_NODE,
    R2RML_SQL2008:             RML_SQL2008,
}

_R2RML_TO_RML_OBJECTS: dict[str, str] = {
    R2RML_GRAPH_MAP_CLASS:              RML_GRAPH_MAP_CLASS,
    R2RML_JOIN_CLASS:                   RML_JOIN_CLASS,
    R2RML_LOGICAL_TABLE_CLASS:          RML_LOGICAL_SOURCE_CLASS,
    R2RML_OBJECT_MAP_CLASS:             RML_OBJECT_MAP_CLASS,
    R2RML_PREDICATE_MAP_CLASS:          RML_PREDICATE_MAP_CLASS,
    R2RML_PREDICATE_OBJECT_MAP_CLASS:   RML_PREDICATE_OBJECT_MAP_CLASS,
    R2RML_REF_OBJECT_MAP_CLASS:         RML_REF_OBJECT_MAP_CLASS,
    R2RML_SUBJECT_MAP_CLASS:            RML_SUBJECT_MAP_CLASS,
    R2RML_TERM_MAP_CLASS:               RML_TERM_MAP_CLASS,
    R2RML_TRIPLES_MAP_CLASS:            RML_TRIPLES_MAP_CLASS,
    R2RML_DEFAULT_GRAPH:                RML_DEFAULT_GRAPH,
    R2RML_IRI:                          RML_IRI,
    R2RML_LITERAL:                      RML_LITERAL,
    R2RML_BLANK_NODE:                   RML_BLANK_NODE,
}


def r2rml_to_rml(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """
    Replace all R2RML constructs in *mapping_graph* with their RML 1.2
    equivalents and return the (mutated) graph.

    Unlike the old ``_r2rml_to_rml``, this function does **not** collapse
    ``rml:AssertedTriplesMap`` → ``rml:TriplesMap`` so that the full RML 1.2
    class hierarchy is preserved for later processing by
    ``normalizer.complete_triples_map_class``.

    Parameters
    ----------
    mapping_graph : rdflib.Graph

    Returns
    -------
    rdflib.Graph
        The same graph instance, mutated in place.
    """
    mapping_graph.bind('rml', rdflib.term.URIRef(RML_NAMESPACE))

    # Annotate RDB logical sources with rml:sqlVersion
    for query_str in (
        f'SELECT ?ls ?x WHERE {{ ?ls <{R2RML_TABLE_NAME}> ?x . }}',
        f'SELECT ?ls ?x WHERE {{ ?ls <{R2RML_SQL_QUERY}> ?x . }}',
    ):
        for logical_source, _ in mapping_graph.query(query_str):
            mapping_graph.add((
                logical_source,
                rdflib.term.URIRef(RML_SQL_VERSION),
                rdflib.term.URIRef(RML_SQL2008),
            ))

    # rml:referenceFormulation for SQL query sources
    for logical_source, _ in mapping_graph.query(
        f'SELECT ?ls ?x WHERE {{ ?ls <{R2RML_SQL_QUERY}> ?x . }}'
    ):
        mapping_graph.add((
            logical_source,
            rdflib.term.URIRef(f'{RML_NAMESPACE}referenceFormulation'),
            rdflib.term.URIRef(RML_SQL2008),
        ))

    for r2rml_p, rml_p in _R2RML_TO_RML_PREDICATES.items():
        mapping_graph = replace_predicates_in_graph(mapping_graph, r2rml_p, rml_p)

    for r2rml_o, rml_o in _R2RML_TO_RML_OBJECTS.items():
        mapping_graph = replace_objects_in_graph(mapping_graph, r2rml_o, rml_o)

    return mapping_graph
