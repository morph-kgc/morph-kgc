"""
Graph-level normalization of an RML 1.2 mapping graph.
"""

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

import rdflib

from ..constants import (
    RDF_REIFIES,
    RML_TRIPLES_MAP_CLASS,
    RML_NON_ASSERTED_TRIPLES_MAP_CLASS,
    RML_TRIPLE_TERM_MAP_CLASS,
    RML_TRIPLE_TERM,
    RML_LEGACY_LOGICAL_SOURCE,
    RML_LEGACY_SOURCE,
    RML_LEGACY_QUERY,
    RML_LEGACY_ITERATOR,
    RML_LEGACY_REFERENCE,
    RML_LEGACY_REFERENCE_FORMULATION,
    RML_LEGACY_SUBJECT_MAP,
    RML_LEGACY_OBJECT_MAP,
    RML_LOGICAL_SOURCE, RML_SOURCE, RML_QUERY, RML_ITERATOR,
    RML_REFERENCE_FORMULATION,
    RML_SUBJECT_MAP, RML_OBJECT_MAP,
    RML_PREDICATE_OBJECT_MAP, RML_GRAPH_MAP,
    RML_CLASS, RML_CONSTANT, RML_TERM_TYPE,
    RML_SUBJECT_SHORTCUT, RML_PREDICATE_SHORTCUT,
    RML_OBJECT_SHORTCUT, RML_GRAPH_SHORTCUT,
    RML_LANGUAGE_MAP, RML_LANGUAGE_SHORTCUT,
    RML_DATATYPE_MAP, RML_DATATYPE_SHORTCUT,
    RML_FUNCTION_MAP, RML_FUNCTION_SHORTCUT,
    RML_RETURN_MAP, RML_RETURN_SHORTCUT,
    RML_PARAMETER_MAP, RML_PARAMETER_SHORTCUT,
    RML_VALUE_MAP, RML_VALUE_SHORTCUT,
    RML_PREDICATE_MAP,
    RML_IRI, RML_LITERAL, RML_BLANK_NODE,
    RML_DEFAULT_GRAPH,
    RML_PARENT_TRIPLES_MAP, RML_EXECUTION,
    RML_CHILD, RML_PARENT,
    RML_TEMPLATE, RML_REFERENCE,
    RML_DIRECTION_SHORTCUT, RML_DIRECTION_MAP, RML_TRIPLE_TERM_MAP, RML_REIFYING_MAP,
    RML_INPUT,
    FNML_EXECUTION, FNML_INPUT, FNML_FUNCTION_MAP, FNML_RETURN_MAP,
    FNML_PARAMETER_MAP, FNML_VALUE_MAP, FNML_FUNCTION_SHORTCUT,
    FNML_RETURN_SHORTCUT, FNML_PARAMETER_SHORTCUT, FNML_VALUE_SHORTCUT,
    XSD_STRING,
)
from ..utils import replace_predicates_in_graph


# ---------------------------------------------------------------------------
# Legacy RML → RML 1.2
# ---------------------------------------------------------------------------

def rml_legacy_to_rml(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """
    Translate legacy RML constructs to their RML 1.2 equivalents.

    Covers:
    - RML legacy namespace (semweb.mmlab.be) → RML 1.2 namespace (w3id.org/rml/)
    - FNML namespace predicates → RML 1.2 equivalents
    """
    legacy_predicate_map: dict[str, str] = {
        # Core legacy namespace
        RML_LEGACY_LOGICAL_SOURCE:          RML_LOGICAL_SOURCE,
        RML_LEGACY_SOURCE:                  RML_SOURCE,
        RML_LEGACY_QUERY:                   RML_QUERY,
        RML_LEGACY_ITERATOR:                RML_ITERATOR,
        RML_LEGACY_REFERENCE:               RML_REFERENCE,
        RML_LEGACY_REFERENCE_FORMULATION:   RML_REFERENCE_FORMULATION,
        RML_LEGACY_SUBJECT_MAP:             RML_SUBJECT_MAP,
        RML_LEGACY_OBJECT_MAP:              RML_OBJECT_MAP,
        # FNML → RML 1.2
        FNML_EXECUTION:     RML_EXECUTION,
        FNML_INPUT:         RML_INPUT,
        FNML_FUNCTION_MAP:  RML_FUNCTION_MAP,
        FNML_RETURN_MAP:    RML_RETURN_MAP,
        FNML_PARAMETER_MAP: RML_PARAMETER_MAP,
        FNML_VALUE_MAP:     RML_VALUE_MAP,
        FNML_FUNCTION_SHORTCUT:  RML_FUNCTION_SHORTCUT,
        FNML_RETURN_SHORTCUT:    RML_RETURN_SHORTCUT,
        FNML_PARAMETER_SHORTCUT: RML_PARAMETER_SHORTCUT,
        FNML_VALUE_SHORTCUT:     RML_VALUE_SHORTCUT,
    }

    for old, new in legacy_predicate_map.items():
        mapping_graph = replace_predicates_in_graph(mapping_graph, old, new)

    return mapping_graph


# ---------------------------------------------------------------------------
# Shortcut expansion
# ---------------------------------------------------------------------------

def expand_shortcuts(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """
    Expand all shortcut properties into their full term-map form.

    Covers:
    - Standard RML constant shortcuts (rml:subject, rml:predicate, …)
    - ``rml:language`` / ``rml:datatype`` / ``rml:direction`` (RML 1.2) shortcuts
    - ``rml:reifyingMap`` shortcut  (RML 1.2)
    """
    mapping_graph = _expand_constant_shortcut_properties(mapping_graph)
    mapping_graph = _expand_reifying_map_shortcut(mapping_graph)
    return mapping_graph


def _expand_constant_shortcut_properties(
    mapping_graph: rdflib.Graph,
) -> rdflib.Graph:
    """Expand rml:subject, rml:predicate, rml:object, rml:graph, etc."""
    shortcuts: dict[str, str] = {
        RML_SUBJECT_SHORTCUT:    RML_SUBJECT_MAP,
        RML_PREDICATE_SHORTCUT:  RML_PREDICATE_MAP,
        RML_OBJECT_SHORTCUT:     RML_OBJECT_MAP,
        RML_LANGUAGE_SHORTCUT:   RML_LANGUAGE_MAP,
        RML_DATATYPE_SHORTCUT:   RML_DATATYPE_MAP,
        RML_GRAPH_SHORTCUT:      RML_GRAPH_MAP,
        RML_FUNCTION_SHORTCUT:   RML_FUNCTION_MAP,
        RML_RETURN_SHORTCUT:     RML_RETURN_MAP,
        RML_PARAMETER_SHORTCUT:  RML_PARAMETER_MAP,
        RML_VALUE_SHORTCUT:      RML_VALUE_MAP,
        RML_DIRECTION_SHORTCUT:      RML_DIRECTION_MAP,
    }
    for shortcut, full_prop in shortcuts.items():
        for s, o in list(mapping_graph.query(
            f'SELECT ?s ?o WHERE {{ ?s <{shortcut}> ?o . }}'
        )):
            bn = rdflib.BNode()
            mapping_graph.add((s, rdflib.term.URIRef(full_prop), bn))
            mapping_graph.add((bn, rdflib.term.URIRef(RML_CONSTANT), o))
        mapping_graph.remove((None, rdflib.term.URIRef(shortcut), None))
    return mapping_graph


def _expand_reifying_map_shortcut(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """
    Expand ``?tmReif rml:reifyingMap ?tmBase``
    →  a full ``rdf:reifies`` predicate-object map with an ``rml:TripleTermMap``.

    The shortcut is only valid when both triples maps share the same
    logical source, so no join condition is added.
    """

    for tm_reif, tm_base in list(mapping_graph.query(
        f'SELECT ?tmReif ?tmBase WHERE {{ ?tmReif <{RML_REIFYING_MAP}> ?tmBase . }}'
    )):
        pom_bn  = rdflib.BNode()
        pm_bn   = rdflib.BNode()
        om_bn   = rdflib.BNode()

        mapping_graph.add((tm_reif, rdflib.term.URIRef(RML_PREDICATE_OBJECT_MAP), pom_bn))
        mapping_graph.add((pom_bn,  rdflib.term.URIRef(RML_PREDICATE_MAP), pm_bn))
        mapping_graph.add((pm_bn,   rdflib.term.URIRef(RML_CONSTANT), rdflib.term.URIRef(RDF_REIFIES)))
        mapping_graph.add((pom_bn,  rdflib.term.URIRef(RML_OBJECT_MAP), om_bn))
        mapping_graph.add((om_bn,   rdflib.RDF.type, rdflib.term.URIRef(RML_TRIPLE_TERM_MAP_CLASS)))
        mapping_graph.add((om_bn,   rdflib.term.URIRef(RML_TRIPLE_TERM_MAP), tm_base))

    mapping_graph.remove((None, rdflib.term.URIRef(RML_REIFYING_MAP), None))
    return mapping_graph


# ---------------------------------------------------------------------------
# Structural normalizations
# ---------------------------------------------------------------------------

def rdf_class_to_pom(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """Replace ``rml:class`` declarations with predicate-object maps."""
    query = (
        'SELECT ?tm ?c WHERE { '
        f'?tm <{RML_SUBJECT_MAP}> ?sm . '
        f'?sm <{RML_CLASS}> ?c . }}'
    )
    for tm, cls in list(mapping_graph.query(query)):
        bn = rdflib.BNode()
        mapping_graph.add((tm, rdflib.term.URIRef(RML_PREDICATE_OBJECT_MAP), bn))
        mapping_graph.add((bn, rdflib.term.URIRef(RML_PREDICATE_SHORTCUT), rdflib.RDF.type))
        mapping_graph.add((bn, rdflib.term.URIRef(RML_OBJECT_SHORTCUT), cls))
    mapping_graph.remove((None, rdflib.term.URIRef(RML_CLASS), None))
    return mapping_graph


def subject_graph_maps_to_pom(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """Move graph maps that are on subject maps to each of their POMs."""
    query = (
        'SELECT ?sm ?gm ?pom WHERE { '
        f'?tm <{RML_SUBJECT_MAP}> ?sm . '
        f'?sm <{RML_GRAPH_MAP}> ?gm . '
        f'?tm <{RML_PREDICATE_OBJECT_MAP}> ?pom . }}'
    )
    for sm, gm, pom in list(mapping_graph.query(query)):
        mapping_graph.add((pom, rdflib.term.URIRef(RML_GRAPH_MAP), gm))

    query2 = (
        'SELECT ?sm ?gm WHERE { '
        f'?tm <{RML_SUBJECT_MAP}> ?sm . '
        f'?sm <{RML_GRAPH_MAP}> ?gm . }}'
    )
    for sm, gm in list(mapping_graph.query(query2)):
        mapping_graph.remove((sm, rdflib.term.URIRef(RML_GRAPH_MAP), gm))
    return mapping_graph


def complete_pom_with_default_graph(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """Add ``rml:defaultGraph`` to predicate-object maps that have no graph map."""
    query = (
        'SELECT DISTINCT ?tm ?pom WHERE { '
        f'?tm <{RML_PREDICATE_OBJECT_MAP}> ?pom . '
        f'OPTIONAL {{ ?pom <{RML_GRAPH_MAP}> ?gm . }} . '
        'FILTER ( !bound(?gm) ) }'
    )
    for _, pom in list(mapping_graph.query(query)):
        bn = rdflib.BNode()
        mapping_graph.add((pom, rdflib.term.URIRef(RML_GRAPH_MAP), bn))
        mapping_graph.add((bn, rdflib.term.URIRef(RML_CONSTANT),
                           rdflib.term.URIRef(RML_DEFAULT_GRAPH)))
    return mapping_graph


def complete_termtypes(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """
    Infer missing ``rml:termType`` values following RML rules.
    """
    # triple-term maps → rml:TripleTerm
    q = (
        'SELECT DISTINCT ?term_map ?ttm WHERE { '
        f'?term_map <{RML_TRIPLE_TERM_MAP}> ?ttm . '
        f'OPTIONAL {{ ?term_map <{RML_TERM_TYPE}> ?tt . }} . '
        'FILTER ( !bound(?tt) ) }'
    )
    for term_map, _ in list(mapping_graph.query(q)):
        mapping_graph.add((term_map, rdflib.term.URIRef(RML_TERM_TYPE),
                           rdflib.term.URIRef(RML_TRIPLE_TERM)))

    # blank-node constants
    q = (
        'SELECT DISTINCT ?tm ?c WHERE { '
        f'?tm <{RML_CONSTANT}> ?c . '
        f'OPTIONAL {{ ?tm <{RML_TERM_TYPE}> ?tt . }} . '
        'FILTER ( !bound(?tt) && isBlank(?c) ) }'
    )
    for tm, _ in list(mapping_graph.query(q)):
        mapping_graph.add((tm, rdflib.term.URIRef(RML_TERM_TYPE),
                           rdflib.term.URIRef(RML_BLANK_NODE)))

    # literal constants
    q = (
        'SELECT DISTINCT ?tm ?c WHERE { '
        f'?tm <{RML_CONSTANT}> ?c . '
        f'OPTIONAL {{ ?tm <{RML_TERM_TYPE}> ?tt . }} . '
        'FILTER ( !bound(?tt) && isLiteral(?c) ) }'
    )
    for tm, _ in list(mapping_graph.query(q)):
        mapping_graph.add((tm, rdflib.term.URIRef(RML_TERM_TYPE),
                           rdflib.term.URIRef(RML_LITERAL)))

    # literal object maps (with reference / language / datatype / execution)
    q = (
        'SELECT DISTINCT ?om ?pom WHERE { '
        f'?pom <{RML_OBJECT_MAP}> ?om . '
        f'OPTIONAL {{ ?om <{RML_TERM_TYPE}> ?tt . }} . '
        f'OPTIONAL {{ ?om <{RML_REFERENCE}> ?r . }} . '
        f'OPTIONAL {{ ?om <{RML_EXECUTION}> ?e . }} . '
        f'OPTIONAL {{ ?om <{RML_LANGUAGE_MAP}> ?l . }} . '
        f'OPTIONAL {{ ?om <{RML_DATATYPE_MAP}> ?d . }} . '
        'FILTER ( !bound(?tt) && ( bound(?r) || bound(?e) '
        '|| bound(?l) || bound(?d) ) ) }'
    )
    for om, _ in list(mapping_graph.query(q)):
        mapping_graph.add((om, rdflib.term.URIRef(RML_TERM_TYPE),
                           rdflib.term.URIRef(RML_LITERAL)))

    # referencing object maps inherit subject termtype from parent
    q = (
        'SELECT DISTINCT ?tm ?tt WHERE { '
        f'?tm <{RML_PARENT_TRIPLES_MAP}> ?ptm . '
        f'?ptm <{RML_SUBJECT_MAP}> ?psm . '
        f'?psm <{RML_TERM_TYPE}> ?tt . }}'
    )
    for tm, tt in list(mapping_graph.query(q)):
        mapping_graph.add((tm, rdflib.term.URIRef(RML_TERM_TYPE),
                           rdflib.term.URIRef(tt)))

    # everything else defaults to IRI
    for prop in (RML_SUBJECT_MAP, RML_PREDICATE_MAP, RML_OBJECT_MAP, RML_GRAPH_MAP):
        q = (
            'SELECT DISTINCT ?tm ?x WHERE { '
            f'?x <{prop}> ?tm . '
            f'OPTIONAL {{ ?tm <{RML_TERM_TYPE}> ?tt . }} . '
            'FILTER ( !bound(?tt) ) }'
        )
        for tm, _ in list(mapping_graph.query(q)):
            mapping_graph.add((tm, rdflib.term.URIRef(RML_TERM_TYPE),
                               rdflib.term.URIRef(RML_IRI)))

    return mapping_graph


def complete_triples_map_class(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """
    Normalize triples map typing to explicit RML 1.2 subclasses.

    Normalization policy:
    - Untyped triples maps become:
      * rml:NonAssertedTriplesMap if they have no predicate-object maps
      * rml:TriplesMap otherwise
    - Existing rml:TriplesMap / rml:NonAssertedTriplesMap typings are preserved
    """

    RDF_TYPE = rdflib.RDF.type
    RML_POM_REF = rdflib.term.URIRef(RML_PREDICATE_OBJECT_MAP)
    RML_TM_REF = rdflib.term.URIRef(RML_TRIPLES_MAP_CLASS)
    RML_NON_ASSERTED_TM_REF = rdflib.term.URIRef(RML_NON_ASSERTED_TRIPLES_MAP_CLASS)

    # All candidate triples maps: resources with a subject map
    q = (
        "SELECT DISTINCT ?tm WHERE { "
        f"?tm <{RML_SUBJECT_MAP}> ?sm . "
        "}"
    )

    for (tm,) in list(mapping_graph.query(q)):
        types = set(mapping_graph.objects(tm, RDF_TYPE))

        has_asserted = RML_TM_REF in types
        has_non_asserted = RML_NON_ASSERTED_TM_REF in types

        # Infer subtype for previously untyped triples maps
        has_pom = (tm, RML_POM_REF, None) in mapping_graph

        # If typed but has no pom, replace TM class to non-asserted
        if has_asserted and not has_pom:
            mapping_graph.add((tm, RDF_TYPE, RML_NON_ASSERTED_TM_REF))
            mapping_graph.remove((tm, RDF_TYPE, RML_TM_REF))

        # If already explicitly typed, leave it as is
        if has_asserted or has_non_asserted:
            continue

        if has_pom:
            mapping_graph.add((tm, RDF_TYPE, RML_TM_REF))
        else:
            mapping_graph.add((tm, RDF_TYPE, RML_NON_ASSERTED_TM_REF))

    return mapping_graph


def remove_string_datatypes(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """Remove redundant ``xsd:string`` datatypes (equivalent to no datatype)."""
    mapping_graph.remove((
        None,
        rdflib.term.URIRef(RML_CONSTANT),
        rdflib.term.URIRef(XSD_STRING),
    ))
    return mapping_graph


def normalize_delimited_identifiers(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """
    Strip R2RML/RML delimited-SQL-identifier quoting from reference-bearing
    literals. Per R2RML section 5, a delimited SQL identifier written in
    Turtle as "\\"Sport\\"" decodes to the literal string '"Sport"' (quotes
    included). Since morph-kgc treats every rml:reference/rml:template
    placeholder/rml:child/rml:parent value as a bare column/field name,
    these delimiter quotes must be stripped here — once, centrally — so
    every later stage (reference collection, data loading, term
    materialization) works with consistent, bare identifiers.
    """
    # TODO: in RML-Core child and parent are childMap and parentMap
    # So this would be covered with just RML_REFERENCE
    reference_predicates = (RML_REFERENCE, RML_CHILD, RML_PARENT)

    for predicate in reference_predicates:
        for s, o in list(mapping_graph.query(
            f'SELECT ?s ?o WHERE {{ ?s <{predicate}> ?o . }}'
        )):
            normalized = _strip_delimited_quotes(str(o))
            if normalized != str(o):
                mapping_graph.remove((s, rdflib.term.URIRef(predicate), o))
                mapping_graph.add((s, rdflib.term.URIRef(predicate),
                                    rdflib.Literal(normalized, datatype=o.datatype)))

    mapping_graph = _normalize_template_placeholders(mapping_graph)
    return mapping_graph


def _strip_delimited_quotes(ref: str) -> str:
    if len(ref) >= 2 and ref.startswith('"') and ref.endswith('"'):
        return ref[1:-1]
    return ref


def _normalize_template_placeholders(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """
    Strip delimited-identifier quoting from inside {...} placeholders of
    rml:template values, without disturbing the surrounding template text.
    """
    import re

    for s, o in list(mapping_graph.query(
        f'SELECT ?s ?o WHERE {{ ?s <{RML_TEMPLATE}> ?o . }}'
    )):
        def _clean(match: "re.Match") -> str:
            return "{" + _strip_delimited_quotes(match.group(1)) + "}"

        normalized = re.sub(r"\{([^{}]*)\}", _clean, str(o))
        if normalized != str(o):
            mapping_graph.remove((s, rdflib.term.URIRef(RML_TEMPLATE), o))
            mapping_graph.add((s, rdflib.term.URIRef(RML_TEMPLATE),
                                rdflib.Literal(normalized, datatype=o.datatype)))

    return mapping_graph


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_termtypes(mapping_graph: rdflib.Graph) -> None:
    """
    Raise ``ValueError`` if any term map has an illegal termtype.
    """
    checks: list[tuple[str, str, set[str]]] = [
        (RML_PREDICATE_MAP, 'predicate', {RML_IRI}),
        (RML_GRAPH_MAP,     'graph',     {RML_IRI}),
        (RML_SUBJECT_MAP,   'subject',   {RML_IRI, RML_BLANK_NODE}),
        (RML_OBJECT_MAP,    'object',    {RML_IRI, RML_BLANK_NODE, RML_LITERAL, RML_TRIPLE_TERM}),
    ]
    for map_prop, label, allowed in checks:
        q = (
            'SELECT DISTINCT ?tt ?m WHERE { '
            f'?x <{map_prop}> ?m . '
            f'?m <{RML_TERM_TYPE}> ?tt . }}'
        )
        found = {str(tt) for tt, _ in mapping_graph.query(q)}
        illegal = found - allowed
        if illegal:
            raise ValueError(
                f'Invalid {label} termtype(s): {illegal}. '
                f'Allowed: {allowed}.'
            )


# ---------------------------------------------------------------------------
# Public pipeline entry points
# ---------------------------------------------------------------------------

def normalize_mapping_graph(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """
    Apply the full normalization pipeline to a raw mapping graph.

    Steps (in order):
    1. Translate legacy RML names to RML 1.2.
    2. Translate ``rml:class`` to predicate-object maps.
    3. Expand all shortcut properties (incl. RML 1.2 shortcuts).
    4. Move subject-level graph maps to their POMs.
    5. Add ``rml:defaultGraph`` to POMs without a graph map.
    6. Infer missing term types.
    7. Normalize triples map typing to explicit RML 1.2 subclasses.
    8. Remove redundant ``xsd:string`` datatypes.
    9. Strip R2RML delimited-identifier quoting from references.

    Returns the mutated graph.
    """
    mapping_graph = rdf_class_to_pom(mapping_graph)
    mapping_graph = expand_shortcuts(mapping_graph)
    mapping_graph = subject_graph_maps_to_pom(mapping_graph)
    mapping_graph = complete_pom_with_default_graph(mapping_graph)
    mapping_graph = complete_termtypes(mapping_graph)
    mapping_graph = complete_triples_map_class(mapping_graph)
    mapping_graph = remove_string_datatypes(mapping_graph)
    mapping_graph = normalize_delimited_identifiers(mapping_graph)
    return mapping_graph


def validate_mapping(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """
    Validates mappings.

    Call this *after* ``normalize_mapping_graph`` and the FNML translation.
    """
    validate_termtypes(mapping_graph)
    return mapping_graph
