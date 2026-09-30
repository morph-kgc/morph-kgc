"""
Orchestrates loading, normalization, RMLMapping construction,
and partitioning of RML 1.2 mapping rules.
"""

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

from typing import Optional
from collections import defaultdict

from ..constants import *
from ..utils import *
from ..source.relational import get_rdb_reference_datatype
from .model import (
    RMLMapping,
    RMLRule,
    LogicalSource,
    TermMap,
    JoinCondition,
    HTTPAPIEntry,
    HTTPAPIHeader,
)
from ..functions.model import FNMLExecution, InputBinding, ValueBinding
from .normalizer import normalize_mapping_graph, validate_mapping
from .formats.rml import load_rml_graph
from .formats.r2rml import r2rml_to_rml
from .formats.yarrrml import load_yarrrml
from .partitioner import MappingPartitioner

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

##############################################################################
# Public entry point
##############################################################################

def retrieve_mappings(config) -> RMLMapping:
    """
    Parse all mapping files defined in *config* and return the processed
    mapping representation.

    Returns
    -------
    RMLMapping
        An :class:`~mapping.model.RMLMapping` instance containing typed
        :class:`~mapping.model.RMLRule` objects, plus the
        :class:`~functions.model.FNMLExecution` registry and the
        :class:`~mapping.model.HTTPAPIEntry` list.
    """
    parser = MappingParser(config)
    start = time.time()
    rml_mapping = parser.parse_mappings()
    LOGGER.info(f'Mappings processed in {get_delta_time(start)} seconds.')
    return rml_mapping


##############################################################################
########################   RML PARSING QUERIES   #############################
##############################################################################

_RML_PARSING_QUERY = """
    prefix rml: <http://w3id.org/rml/>
    prefix sd:  <https://w3id.org/okn/o/sd#>

    SELECT DISTINCT
        ?triples_map_id ?triples_map_type
        ?logical_source_type ?logical_source_value ?iterator ?reference_formulation
        ?subject_map_type ?subject_map_value ?subject_map ?subject_termtype
        ?predicate_map_type ?predicate_map_value
        ?object_map_type ?object_map_value ?object_map ?object_termtype
        ?lang_datatype ?lang_datatype_map_type ?lang_datatype_map_value
        ?direction_map_type ?direction_map_value
        ?graph_map_type ?graph_map_value

    WHERE {
        ?triples_map_id rml:logicalSource ?_source ;
                        a ?triples_map_type .

        OPTIONAL {
            ?_source ?logical_source_type ?logical_source_value_raw .
            OPTIONAL { ?logical_source_value_raw sd:name ?logical_source_in_memory_value . }
            BIND(
                IF (
                    BOUND(?logical_source_in_memory_value),
                    CONCAT("{", STR(?logical_source_in_memory_value), "}"),
                    STR(?logical_source_value_raw)
                ) AS ?logical_source_value
            )
            FILTER ( ?logical_source_type IN ( rml:source, rml:tableName, rml:query ) ) .
        }
        OPTIONAL { ?_source rml:iterator ?iterator . }
        OPTIONAL { ?_source rml:referenceFormulation ?reference_formulation . }

    # Subject -----------------------------------------------------------------------
        ?triples_map_id rml:subjectMap ?subject_map .
        ?subject_map ?subject_map_type ?subject_map_value .
        FILTER ( ?subject_map_type IN (
            rml:constant, rml:template, rml:reference, rml:functionExecution ) ) .
        OPTIONAL { ?subject_map rml:termType ?subject_termtype . }

    # Predicate ---------------------------------------------------------------------
        OPTIONAL {
            ?triples_map_id rml:predicateObjectMap ?_pom .
            ?_pom rml:predicateMap ?_pm .
            ?_pm  ?predicate_map_type ?predicate_map_value .
            FILTER ( ?predicate_map_type IN (
                rml:constant, rml:template, rml:reference, rml:functionExecution ) ) .

    # Object ------------------------------------------------------------------------
            OPTIONAL {
                ?_pom rml:objectMap ?object_map .
                ?object_map ?object_map_type ?object_map_value .
                FILTER ( ?object_map_type IN (
                    rml:constant, rml:template, rml:reference,
                    rml:tripleTermMap, rml:functionExecution ) ) .
                OPTIONAL { ?object_map rml:termType ?object_termtype . }
                OPTIONAL {
                    ?object_map ?lang_datatype ?_ld_map .
                    ?_ld_map ?lang_datatype_map_type ?lang_datatype_map_value .
                    FILTER ( ?lang_datatype_map_value !=
                             <http://www.w3.org/2001/XMLSchema#string> ) .
                    FILTER ( ?lang_datatype_map_type IN (
                        rml:constant, rml:template, rml:reference,
                        rml:functionExecution ) ) .
                    FILTER ( ?lang_datatype IN (
                        rml:languageMap, rml:datatypeMap ) ) .
                }
                OPTIONAL {
                    ?object_map rml:directionMap ?_dir_map .
                    ?_dir_map ?direction_map_type ?direction_map_value .
                    FILTER ( ?direction_map_type IN (
                        rml:constant, rml:template, rml:reference ) ) .
                }
            }
            OPTIONAL {
                ?_pom rml:objectMap ?object_map .
                ?object_map rml:parentTriplesMap ?object_map_value .
                OPTIONAL { ?object_map rml:termType ?object_termtype . }
                BIND ( rml:parentTriplesMap AS ?object_map_type ) .
            }
            OPTIONAL {
                ?_pom rml:graphMap ?_gm .
                ?_gm ?graph_map_type ?graph_map_value .
                FILTER ( ?graph_map_type IN (
                    rml:constant, rml:template, rml:reference,
                    rml:functionExecution ) ) .
            }
        }
    }
"""

_RML_JOIN_CONDITION_PARSING_QUERY = """
    prefix rml: <http://w3id.org/rml/>

    SELECT DISTINCT ?term_map ?join_condition ?child_value ?parent_value
    WHERE {
        ?term_map rml:joinCondition ?join_condition .
        ?join_condition rml:child  ?child_value ;
                        rml:parent ?parent_value .
    }
"""


##############################################################################
# FNML graph-level translation
##############################################################################

_FNML_FUNCTION_QUERY = """
PREFIX fnml: <http://semweb.mmlab.be/ns/fnml#>
PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rml:  <http://w3id.org/rml/>
PREFIX rr:   <http://www.w3.org/ns/r2rml#>
PREFIX fno:  <https://w3id.org/function/ontology#>

SELECT *
WHERE {
  ?functionCaller fnml:functionValue [
    rml:predicateObjectMap
      [ rml:predicateMap/rml:constant fno:executes ;
        rml:objectMap/rml:constant    ?functionID ],
      [ rml:predicateMap [ rml:constant ?parameter ] ;
        rml:objectMap   [ ?_objectConnector ?_objectValue ] ]
  ] .
  OPTIONAL { ?_callingOM fnml:functionValue ?_objectValue . }

  # The YARRRML converter emits rr:-namespaced predicates for these
  # structural triples, so parameters in that namespace must be excluded.
  FILTER (!STRSTARTS(str(?parameter), str(rr:)))
  FILTER (?parameter != fno:executes)
  FILTER (?_objectConnector != rml:termType)
  FILTER (?_objectConnector != rdf:type)

  BIND (IF(?_objectConnector = fnml:functionValue, rml:functionExecution, ?_objectConnector) AS ?objectConnector)
  BIND (IF(?_objectConnector = fnml:functionValue, ?_callingOM, ?_objectValue) AS ?objectValue)
}
ORDER BY ?functionCaller ?parameter
"""

_FNML_DELETE_QUERY = """
PREFIX fnml: <http://semweb.mmlab.be/ns/fnml#>
PREFIX rml:  <http://w3id.org/rml/>

DELETE {
  ?functionCaller fnml:functionValue ?functionValue .
  ?functionValue ?functionMap ?POMMaps .
  ?POMMaps ?DeeperMaps ?PredicateAndObjectMaps .
}
WHERE {
  ?functionCaller fnml:functionValue ?functionValue .
  ?functionValue ?functionMap ?POMMaps .
  ?POMMaps ?DeeperMaps ?PredicateAndObjectMaps .
  FILTER (?functionMap != rml:logicalSource)
}
"""

# SPARQL query that extracts all HTTP-API source descriptions from the graph.
_HTTP_API_QUERY = """
PREFIX rml:   <http://w3id.org/rml/>
PREFIX htv:   <http://www.w3.org/2011/http#>
PREFIX rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT DISTINCT ?source ?absolute_path ?field_name ?field_value
WHERE {
  ?source htv:absoluteURI ?absolute_path .

  OPTIONAL {
    ?source htv:headers/rdf:rest*/rdf:first ?headerNode .
    ?headerNode htv:fieldName ?field_name .
    ?headerNode htv:fieldValue ?field_value .
  }
}
"""


def _translate_fnml_to_rml(mapping_graph: rdflib.Graph) -> rdflib.Graph:
    """Translate FNML function mappings to their RML 1.2 equivalents."""
    qres = mapping_graph.query(_FNML_FUNCTION_QUERY)
    prev_caller = prev_fn_id = None
    blank_exec = blanknode_fnmap = None
    inner_fns: dict = {}

    for row in qres:
        if prev_fn_id != row.functionID:
            prev_fn_id = row.functionID
            blanknode_fnmap = rdflib.BNode()
        if prev_caller != row.functionCaller:
            prev_caller = row.functionCaller
            blank_exec = rdflib.BNode()
            # The caller is a blank node whenever the calling term map is
            # written inline, so it is used as returned: wrapping it in URIRef
            # would attach the execution to a node absent from the graph.
            mapping_graph.add((
                row.functionCaller,
                rdflib.term.URIRef(RML_EXECUTION),
                blank_exec,
            ))
        mapping_graph.add((blank_exec, rdflib.URIRef(RML_FUNCTION_MAP), blanknode_fnmap))
        mapping_graph.add((blanknode_fnmap, rdflib.URIRef(RML_CONSTANT), row.functionID))

        blank_input = rdflib.BNode()
        mapping_graph.add((blank_exec, rdflib.term.URIRef(RML_INPUT), blank_input))
        blank_value_map = rdflib.BNode()
        mapping_graph.add((blank_input, rdflib.term.URIRef(RML_VALUE_MAP), blank_value_map))

        # Nested calls are only visible through the BIND-resolved variables,
        # which rewrite fnml:functionValue into rml:functionExecution and point
        # at the object map issuing the inner call.
        if 'functionExecution' in str(row.objectConnector):
            inner_fns[row.objectValue] = blank_value_map
        else:
            mapping_graph.add((blank_value_map, rdflib.term.URIRef(str(row.objectConnector)), row.objectValue))

        blank_pm = rdflib.BNode()
        mapping_graph.add((blank_input, rdflib.URIRef(RML_PARAMETER_MAP), blank_pm))
        mapping_graph.add((blank_pm, rdflib.term.URIRef(RML_CONSTANT), row.parameter))

    for inner_om, outer_bn in inner_fns.items():
        # Also a blank node for an inline object map, hence used as returned.
        inner_blanks = list(mapping_graph.objects(
            inner_om,
            rdflib.term.URIRef(RML_EXECUTION),
        ))
        if inner_blanks:
            mapping_graph.add((
                outer_bn,
                rdflib.term.URIRef(RML_EXECUTION),
                inner_blanks[0],
            ))

    mapping_graph.update(_FNML_DELETE_QUERY)
    return mapping_graph


##############################################################################
# Row-level helpers for SPARQL result conversion
##############################################################################

def _str_row(row: dict) -> dict[str, Optional[str]]:
    """Convert a SPARQL result binding row to {str_key: str_value | None}."""
    return {str(k): (str(v) if v is not None else None) for k, v in row.items()}


def _req(row: dict[str, Optional[str]], key: str) -> str:
    """Return a required field value, raising clearly if absent."""
    value = row.get(key)
    if value is None:
        raise ValueError(
            f"Required SPARQL binding {key!r} is missing or None in row {row}."
        )
    return value


##############################################################################
# Join-condition helpers
##############################################################################

def _get_join_conditions_dict(join_query_results) -> dict:
    jc_dict: dict = {}
    for jc in join_query_results:
        if jc.term_map not in jc_dict:
            jc_dict[jc.term_map] = {}
        jc_dict[jc.term_map][str(jc.join_condition)] = {
            'child_value':  str(jc.child_value),
            'parent_value': str(jc.parent_value),
        }
    return jc_dict


def _build_join_conditions(join_dict_entry) -> list[JoinCondition]:
    if not join_dict_entry:
        return []
    return [
        JoinCondition(child_value=v['child_value'], parent_value=v['parent_value'])
        for v in join_dict_entry.values()
    ]


##############################################################################
# Graph → RMLMapping conversion
##############################################################################

def _parse_fnml_execution(
    mapping_graph,
    execution_node,
    cache,
    visiting=None,
):
    if visiting is None:
        visiting = set()

    execution_id = str(execution_node)

    if execution_id in cache:
        return cache[execution_id]

    if execution_id in visiting:
        raise ValueError(
            f"Cyclic FNML execution detected at {execution_id!r}."
        )

    visiting.add(execution_id)

    function_map = mapping_graph.value(
        execution_node,
        rdflib.URIRef(RML_FUNCTION_MAP),
    )
    if function_map is None:
        raise ValueError(
            f"Execution {execution_id!r} has no rml:functionMap."
        )

    function_iri = mapping_graph.value(
        function_map,
        rdflib.URIRef(RML_CONSTANT),
    )
    if function_iri is None:
        raise ValueError(
            f"Execution {execution_id!r} has no function IRI."
        )

    execution = FNMLExecution(
        execution_id=execution_id,
        function_iri=str(function_iri),
        inputs=[],
    )

    cache[execution_id] = execution

    for input_node in mapping_graph.objects(
        execution_node,
        rdflib.URIRef(RML_INPUT),
    ):
        parameter_map = mapping_graph.value(
            input_node,
            rdflib.URIRef(RML_PARAMETER_MAP),
        )
        if parameter_map is None:
            raise ValueError(
                f"Execution {execution_id!r} has an input "
                "without rml:parameterMap."
            )

        parameter_iri = mapping_graph.value(
            parameter_map,
            rdflib.URIRef(RML_CONSTANT),
        )
        if parameter_iri is None:
            raise ValueError(
                f"Execution {execution_id!r} has a parameter map "
                "without rml:constant."
            )

        input_binding = InputBinding(
            parameter_iri=str(parameter_iri),
            values=[],
        )

        for value_map in mapping_graph.objects(
            input_node,
            rdflib.URIRef(RML_VALUE_MAP),
        ):
            input_binding.values.append(
                _parse_fnml_value_map(
                    mapping_graph,
                    value_map,
                    cache,
                    visiting,
                )
            )

        execution.inputs.append(input_binding)

    visiting.remove(execution_id)
    return execution


def _parse_fnml_value_map(
    mapping_graph,
    value_map,
    cache,
    visiting,
):
    candidates = []

    for map_type in (
        RML_CONSTANT,
        RML_TEMPLATE,
        RML_REFERENCE,
        RML_EXECUTION,
    ):
        for map_value in mapping_graph.objects(
            value_map,
            rdflib.URIRef(map_type),
        ):
            candidates.append((map_type, map_value))

    if len(candidates) != 1:
        raise ValueError(
            f"Expected one value-map predicate for {value_map!r}, "
            f"found {candidates!r}."
        )

    map_type, map_value = candidates[0]

    if map_type == RML_EXECUTION:
        nested_execution = _parse_fnml_execution(
            mapping_graph=mapping_graph,
            execution_node=map_value,
            cache=cache,
            visiting=visiting,
        )

        return ValueBinding(
            map_type=map_type,
            map_value=str(map_value),
            nested_execution=nested_execution,
        )

    return ValueBinding(
        map_type=map_type,
        map_value=str(map_value),
    )


def _graph_to_rml_mapping(
    mapping_graph: rdflib.Graph,
    section_name: str,
) -> RMLMapping:
    """
    Convert a normalized rdflib mapping graph directly to an RMLMapping.

    All three result sets (RML rules, FNML entries, HTTP-API entries) are
    built as typed dataclass instances.
    """
    rml_qr  = mapping_graph.query(_RML_PARSING_QUERY)
    join_qr = mapping_graph.query(_RML_JOIN_CONDITION_PARSING_QUERY)

    jc_dict = _get_join_conditions_dict(join_qr)

    # ── RML rules ─────────────────────────────────────────────────────────
    rules: list[RMLRule] = []
    for row in rml_qr.bindings:
        s = _str_row(row)

        rules.append(RMLRule(
            triples_map_id   = _req(s, 'triples_map_id'),
            triples_map_type = _req(s, 'triples_map_type'),
            logical_source=LogicalSource(
                format_                    = '',   # resolved later in _complete_source_types
                value_type                 = _req(s, 'logical_source_type'),
                value                      = _req(s, 'logical_source_value'),
                config_section_name        = section_name,
                iterator                   = s.get('iterator'),
                reference_formulation    = s.get('reference_formulation'),
            ),
            subject=TermMap(
                map_type       = _req(s, 'subject_map_type'),
                map_value      = _req(s, 'subject_map_value'),
                term_type      = s.get('subject_termtype'),
                join_conditions= _build_join_conditions(
                    jc_dict.get(row.get('subject_map'))
                ),
            ),
            predicate=TermMap(
                map_type  = _req(s, 'predicate_map_type'),
                map_value = _req(s, 'predicate_map_value'),
                term_type = RML_IRI,
            ) if s.get('predicate_map_type') else None,
            #  use underscore to avoid using the bare name object which is a Python built-in type
            object_=TermMap(
                map_type              = _req(s, 'object_map_type'),
                map_value             = _req(s, 'object_map_value'),
                term_type             = s.get('object_termtype'),
                join_conditions       = _build_join_conditions(
                    jc_dict.get(row.get('object_map'))
                ),
                lang_datatype         = s.get('lang_datatype'),
                lang_datatype_map_type= s.get('lang_datatype_map_type'),
                lang_datatype_map_value=s.get('lang_datatype_map_value'),
                direction_map_type    = s.get('direction_map_type'),
                direction_map_value   = s.get('direction_map_value'),
            ) if s.get('object_map_type') else None,
            graph=TermMap(
                map_type  = _req(s, 'graph_map_type'),
                map_value = _req(s, 'graph_map_value'),
                term_type = RML_IRI,
            ) if s.get('graph_map_type') else None,
        ))

    # ── FNML executions ───────────────────────────────────────────────────
    # The execution registry holds the (possibly nested) FNMLExecution objects.
    fnml_executions: dict[str, FNMLExecution] = {}

    for execution_node in set(
        mapping_graph.subjects(
            rdflib.URIRef(RML_FUNCTION_MAP),
            None,
        )
    ):
        _parse_fnml_execution(
            mapping_graph=mapping_graph,
            execution_node=execution_node,
            cache=fnml_executions,
        )

    # ── HTTP-API entries ──────────────────────────────────────────────────
    http_api_map: dict[tuple[str, str], list[tuple[str | None, str | None]]] = defaultdict(list)

    for row in mapping_graph.query(_HTTP_API_QUERY).bindings:
        s = _str_row(row)
        source = _req(s, "source")
        absolute_path = _req(s, "absolute_path")
        field_name = s.get("field_name")
        field_value = s.get("field_value")

        http_api_map[(source, absolute_path)].append((field_name, field_value))

    http_api_entries: list[HTTPAPIEntry] = []
    for (source, absolute_path), pairs in http_api_map.items():
        headers = [
            HTTPAPIHeader(field_name=fn, field_value=fv)
            for fn, fv in pairs
            if fn is not None and fv is not None
        ]
        http_api_entries.append(
            HTTPAPIEntry(
                source=source,
                absolute_path=absolute_path,
                headers=headers,
            )
        )

    return RMLMapping(
        rules=rules,
        fnml_executions=fnml_executions,
        http_api_entries=http_api_entries,
    )


##############################################################################
# Rule identity helpers
##############################################################################

def _get_term_map_identity(term_map: Optional[TermMap]) -> Optional[tuple]:
    """
    Hashable identity of a term map.

    Every attribute that can make two term maps yield different RDF terms is
    part of the identity: two object maps over the same reference but with
    distinct language tags, datatypes, base directions or join conditions are
    different term maps and must not be considered equal.

    Join conditions are conjunctive, so their order carries no meaning: they
    are sorted to keep the identity stable regardless of the order in which
    the SPARQL parsing queries returned them.
    """
    if term_map is None:
        return None

    return (
        term_map.map_type,
        term_map.map_value,
        term_map.term_type,
        term_map.lang_datatype,
        term_map.lang_datatype_map_type,
        term_map.lang_datatype_map_value,
        term_map.direction_map_type,
        term_map.direction_map_value,
        tuple(sorted(
            (jc.child_value, jc.parent_value) for jc in term_map.join_conditions
        )),
    )


def _get_logical_source_identity(logical_source: LogicalSource) -> tuple:
    """
    Hashable identity of a logical source.

    ``format_`` is left out: it is still empty at deduplication time and is
    derived from the remaining attributes later in ``_complete_source_types``.
    ``config_section_name`` is part of the identity because the same logical
    source declared under two config sections can resolve to different data
    (e.g. a different database or file path).
    """
    return (
        logical_source.config_section_name,
        logical_source.value_type,
        logical_source.value,
        logical_source.iterator,
        logical_source.reference_formulation,
    )


##############################################################################
# Identifier helpers
##############################################################################

def _is_delimited_identifier(identifier: str) -> bool:
    return len(identifier) > 2 and identifier[0] == '"' and identifier[-1] == '"'


def _get_undelimited_identifier(identifier: str) -> str:
    return identifier[1:-1] if _is_delimited_identifier(identifier) else identifier


def _get_valid_template_identifiers(template: str) -> str:
    return template.replace('{"', '{').replace('"}', '}')


##############################################################################
# MappingParser
##############################################################################

class MappingParser:

    def __init__(self, config):
        self.rml_mapping = RMLMapping()
        self.config = config

    def __str__(self):
        return str([str(r) for r in self.rml_mapping.rules])

    def __len__(self):
        return len(self.rml_mapping)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def parse_mappings(self) -> RMLMapping:
        self._load_and_parse_all_sources()
        self._preprocess_mappings()
        self._infer_datatypes()
        self.validate_mappings()
        LOGGER.info(f'{len(self.rml_mapping)} mapping rules retrieved.')
        MappingPartitioner(self.rml_mapping, self.config).partition_mappings()
        return self.rml_mapping

    # ------------------------------------------------------------------
    # Internal – loading
    # ------------------------------------------------------------------

    def _load_and_parse_all_sources(self):
        for section_name in self.config.get_data_sources_sections():
            partial = self._parse_data_source_mapping_files(section_name)
            self.rml_mapping.rules.extend(partial.rules)
            self.rml_mapping.fnml_executions.update(partial.fnml_executions)
            self.rml_mapping.http_api_entries.extend(partial.http_api_entries)

    def _parse_data_source_mapping_files(self, section_name: str) -> RMLMapping:
        g = self._load_mapping_graph(section_name)
        g = r2rml_to_rml(g)
        g = normalize_mapping_graph(g)
        g = _translate_fnml_to_rml(g)
        g = validate_mapping(g)
        mapping = _graph_to_rml_mapping(g, section_name)
        return mapping

    def _load_mapping_graph(self, section_name: str) -> rdflib.Graph:
        g = rdflib.Graph()
        for f in self.config.get_mappings_files(section_name):
            if f.endswith(('.yarrrml', '.yml', '.yaml')):
                g += load_yarrrml(f)
            else:
                g += load_rml_graph(f)
        return g

    # ------------------------------------------------------------------
    # Internal – preprocessing
    # ------------------------------------------------------------------

    def _preprocess_mappings(self):
        self._drop_duplicates()
        self._complete_rml_source_with_config_file_paths()
        self._complete_source_types()
        self._remove_delimiters_from_mappings()
        self._remove_self_joins_no_condition()

    def _drop_duplicates(self):
        """
        Drop rules that are duplicates of an earlier rule, keeping the first
        occurrence of each.

        Two rules are duplicates only when their triples map, logical source
        and every term map are identical. Anything less than the full term map
        identity would discard rules that generate different triples, and,
        since the surviving rule is whichever the SPARQL parsing query returned
        first, would do so unpredictably.
        """
        seen: set[tuple] = set()
        unique: list[RMLRule] = []
        for rule in self.rml_mapping.rules:
            key = (
                rule.triples_map_id,
                rule.triples_map_type,
                _get_logical_source_identity(rule.logical_source),
                _get_term_map_identity(rule.subject),
                _get_term_map_identity(rule.predicate),
                _get_term_map_identity(rule.object_),
                _get_term_map_identity(rule.graph),
            )
            if key not in seen:
                seen.add(key)
                unique.append(rule)
        self.rml_mapping.rules = unique

    def _complete_rml_source_with_config_file_paths(self):
        for section in self.config.get_data_sources_sections():
            if self.config.has_file_path(section):
                fp = self.config.get_file_path(section)
                for rule in self.rml_mapping.rules:
                    if rule.logical_source.config_section_name == section:
                        rule.logical_source.value_type  = RML_SOURCE
                        rule.logical_source.value = fp

    def _complete_source_types(self):
        # A logical source whose value is the request described with htv:
        # properties is an HTTP API, whatever its reference formulation: the
        # formulation says how the response is read, not where it comes from.
        http_api_sources = {entry.source for entry in self.rml_mapping.http_api_entries}

        for rule in self.rml_mapping.rules:
            section  = rule.logical_source.config_section_name
            ls       = rule.logical_source
            ref_form = ls.reference_formulation

            if str(ls.value) in http_api_sources:
                ls.format_ = HTTPAPI
            elif self.config.has_db_url(section):
                if ref_form and 'SQL' in ref_form.upper():
                    ls.format_ = RDB
                elif ref_form and any(x in ref_form.upper() for x in ('CYPHER', 'GQL')):
                    ls.format_ = PGDB
                else:
                    ls.format_ = RDB
            elif getattr(ls, 'value_type', '') == RML_QUERY:
                ls.format_ = CSV
            elif ls.format_.startswith('{') and ls.value.endswith('}'):
                ls.format_ = PYTHON_SOURCE
            else:
                ext = os.path.splitext(str(ls.value))[1][1:].strip()
                if ref_form and GEOPARQUET in ref_form.upper():
                    ls.format_ = GEOPARQUET
                elif ext.upper() in FILE_SOURCE_TYPES:
                    ls.format_ = ext.upper()
                elif ref_form:
                    ls.format_ = ref_form.replace(RML_NAMESPACE, '').upper()
                else:
                    raise Exception(
                        f'No source type could be determined for rule '
                        f'{rule.triples_map_id}.'
                    )

    def _remove_delimiters_from_mappings(self):
        for rule in self.rml_mapping.rules:
            ls = rule.logical_source
            if getattr(ls, 'value_type', '') == RML_TABLE_NAME:
                ls.value = _get_undelimited_identifier(ls.value)

            if rule.subject.map_type == RML_TEMPLATE:
                rule.subject.map_value = _get_valid_template_identifiers(rule.subject.map_value)
            elif rule.subject.map_type == RML_REFERENCE:
                rule.subject.map_value = _get_undelimited_identifier(rule.subject.map_value)

            if rule.object_ and rule.object_.map_type == RML_TEMPLATE:
                rule.object_.map_value = _get_valid_template_identifiers(rule.object_.map_value)
            elif rule.object_ and rule.object_.map_type == RML_REFERENCE:
                rule.object_.map_value = _get_undelimited_identifier(rule.object_.map_value)

    def _remove_self_joins_no_condition(self):
        for rule in self.rml_mapping.rules:
            if rule.object_ and rule.object_.map_type == RML_PARENT_TRIPLES_MAP:
                parent_rules = [
                    r for r in self.rml_mapping.rules
                    if r.triples_map_id == rule.object_.map_value
                ]
                if not parent_rules:
                    continue
                parent = parent_rules[0]
                if rule.logical_source.value != parent.logical_source.value:
                    continue
                if str(rule.logical_source.iterator) != str(parent.logical_source.iterator):
                    continue

                jcs    = rule.object_.join_conditions
                remove = all(jc.child_value == jc.parent_value for jc in jcs) if jcs else True
                if remove and jcs:
                    rule.object_.map_type        = parent.subject.map_type
                    rule.object_.map_value       = parent.subject.map_value
                    rule.object_.term_type       = parent.subject.term_type
                    rule.object_.join_conditions = []

    def _infer_datatypes(self):
        """
        Infer XSD datatypes for RDB-sourced object maps that carry no explicit
        datatype.  No-op for non-RDB sources and when `infer_sql_datatypes` is
        disabled.
        """
        if not self.config.infer_sql_datatypes:
            return

        # inspect the schema of each table only once
        columns_cache: dict = {}

        for rule in self.rml_mapping.rules:
            if rule.logical_source.format_ != RDB:
                continue
            if rule.object_ is None:
                continue
            if rule.object_.term_type != RML_LITERAL:
                continue  # datatypes only apply to literals
            if rule.object_.lang_datatype is not None:
                continue  # already has an explicit lang/datatype annotation
            if rule.object_.map_type == RML_REFERENCE:
                inferred = get_rdb_reference_datatype(
                    self.config,
                    rule.logical_source,
                    rule.object_.map_value,
                    columns_cache,
                )
                # xsd:string is the datatype of simple literals in RDF 1.1,
                # annotating it explicitly would be redundant
                if inferred and inferred != XSD_STRING:
                    rule.object_.lang_datatype           = RML_DATATYPE_MAP
                    rule.object_.lang_datatype_map_type  = RML_CONSTANT
                    rule.object_.lang_datatype_map_value = inferred

    def validate_mappings(self):
        """
        Validate cross-source uniqueness of triples map identifiers.

        A triples map ID may appear in multiple rules of the *same* data
        source (one row per predicate-object map), but MUST NOT appear in
        more than one data source section.

        Also checks FNML integrity: every ``rml:functionExecution`` reference
        in a subject or object map must resolve to an execution of the
        mapping's FNML execution registry.
        """
        seen: dict[str, str] = {}  # triples_map_id → logical_source.name

        for rule in self.rml_mapping.rules:
            tid = rule.triples_map_id
            src = rule.logical_source.config_section_name
            if tid in seen and seen[tid] != src:
                raise Exception(
                    f'Triples map {tid!r} appears in more than one data source '
                    f'({seen[tid]!r} and {src!r}). Each triples map must belong '
                    f'to exactly one data source.'
                )
            seen[tid] = src

        known_executions = set(self.rml_mapping.fnml_executions)
        for rule in self.rml_mapping.rules:
            for label, tm in (('subject', rule.subject), ('object', rule.object_)):
                if tm and tm.map_type == RML_EXECUTION:
                    if tm.map_value not in known_executions:
                        raise Exception(
                            f'Triples map {rule.triples_map_id!r} references unknown '
                            f'function execution {tm.map_value!r} in {label} map.'
                        )

        self._validate_triple_term_maps()
        self._validate_direction_maps()

    def _validate_triple_term_maps(self):
        """
        Check the RML 1.2 rules on ``rml:tripleTermMap`` that hold for the
        mapping as a whole: every reference resolves, and the directed graph of
        references across all triples maps is acyclic. A cycle would describe a
        triple term that contains itself.
        """
        known_triples_maps = {rule.triples_map_id for rule in self.rml_mapping.rules}
        references: dict[str, set[str]] = defaultdict(set)

        for rule in self.rml_mapping.rules:
            object_map = rule.object_
            if object_map is None or object_map.map_type != RML_TRIPLE_TERM_MAP:
                continue
            if object_map.map_value not in known_triples_maps:
                raise Exception(
                    f'Triples map {rule.triples_map_id!r} has an rml:tripleTermMap '
                    f'pointing to unknown triples map {object_map.map_value!r}.'
                )
            references[rule.triples_map_id].add(object_map.map_value)

        visiting: list[str] = []
        visited: set[str] = set()

        def walk(triples_map_id: str):
            if triples_map_id in visiting:
                cycle = ' -> '.join(visiting[visiting.index(triples_map_id):] + [triples_map_id])
                raise Exception(
                    f'The rml:tripleTermMap references of the mapping form a cycle: '
                    f'{cycle}. This graph must be acyclic.'
                )
            if triples_map_id in visited:
                return
            visiting.append(triples_map_id)
            for referenced in references[triples_map_id]:
                walk(referenced)
            visiting.pop()
            visited.add(triples_map_id)

        for triples_map_id in list(references):
            walk(triples_map_id)

    def _validate_direction_maps(self):
        """
        Check the RML 1.2 rules on base directions: a direction is only
        meaningful next to a language, and a constant one must be a direction
        token that RDF 1.2 defines. Reference- and template-valued direction
        maps are checked per row while the terms are materialized.
        """
        for rule in self.rml_mapping.rules:
            object_map = rule.object_
            if object_map is None or object_map.direction_map_type is None:
                continue

            if object_map.lang_datatype != RML_LANGUAGE_MAP:
                raise Exception(
                    f'The object map of triples map {rule.triples_map_id!r} has a base '
                    f'direction but no language. rml:directionMap and rml:direction '
                    f'require rml:languageMap or rml:language on the same object map.'
                )

            if (
                object_map.direction_map_type == RML_CONSTANT
                and object_map.direction_map_value not in VALID_DIRECTIONS
            ):
                raise Exception(
                    f'Invalid base direction {object_map.direction_map_value!r} in triples '
                    f'map {rule.triples_map_id!r}. A direction must be one of '
                    f'{" or ".join(VALID_DIRECTIONS)}.'
                )
