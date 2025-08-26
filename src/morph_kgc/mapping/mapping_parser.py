__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero", "Ahmad Alobaid"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


from .yarrrml import load_yarrrml
from ..constants import *
from ..utils import *
from ..mapping.mapping_constants import *
from ..mapping.mapping_partitioner import MappingPartitioner
from ..data_source.relational_db import get_rdb_reference_datatype

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

def retrieve_mappings(config):
    mappings_parser = MappingParser(config)

    start_time = time.time()
    rml_df, fnml_df, http_api_df = mappings_parser.parse_mappings()
    LOGGER.info(f'Mappings processed in {get_delta_time(start_time)} seconds.')

    return rml_df, fnml_df, http_api_df


def _r2rml_to_rml(mapping_graph):
    """
    Replaces R2RML rules in the graph with the corresponding RML rules.
    Also replaces rml:AssertedTriplesMap with rml:TriplesMap.
    """

    # add namespace
    mapping_graph.bind('rml', rdflib.term.URIRef(RML_NAMESPACE))

    # add reference formulation and sql version for RDB sources
    query = f'SELECT ?logical_source ?x WHERE {{ ?logical_source <{R2RML_TABLE_NAME}> ?x . }} '
    for logical_source, _ in mapping_graph.query(query):
        mapping_graph.add((logical_source, rdflib.term.URIRef(RML_SQL_VERSION), rdflib.term.URIRef(RML_SQL2008)))
    query = f'SELECT ?logical_source ?x WHERE {{ ?logical_source <{R2RML_SQL_QUERY}> ?x . }} '
    for logical_source, _ in mapping_graph.query(query):
        mapping_graph.add((logical_source, rdflib.term.URIRef(RML_SQL_VERSION), rdflib.term.URIRef(RML_SQL2008)))
        mapping_graph.add((logical_source, rdflib.term.URIRef(RML_REFERENCE_FORMULATION), rdflib.term.URIRef(RML_SQL2008)))

    # replace R2RML properties with the equivalent RML properties
    r2rml_to_rml_dict = {
        R2RML_LOGICAL_TABLE: RML_LOGICAL_SOURCE,
        R2RML_TABLE_NAME: RML_TABLE_NAME,
        R2RML_SQL_QUERY: RML_QUERY,
        R2RML_PARENT_TRIPLES_MAP: RML_PARENT_TRIPLES_MAP,
        R2RML_SUBJECT_MAP: RML_SUBJECT_MAP,
        R2RML_PREDICATE_OBJECT_MAP: RML_PREDICATE_OBJECT_MAP,
        R2RML_PREDICATE_MAP: RML_PREDICATE_MAP,
        R2RML_OBJECT_MAP: RML_OBJECT_MAP,
        R2RML_GRAPH_MAP: RML_GRAPH_MAP,
        R2RML_SUBJECT_SHORTCUT: RML_SUBJECT_SHORTCUT,
        R2RML_PREDICATE_SHORTCUT: RML_PREDICATE_SHORTCUT,
        R2RML_OBJECT_SHORTCUT: RML_OBJECT_SHORTCUT,
        R2RML_GRAPH_SHORTCUT: RML_GRAPH_SHORTCUT,
        R2RML_COLUMN: RML_REFERENCE,
        R2RML_TEMPLATE: RML_TEMPLATE,
        R2RML_CONSTANT: RML_CONSTANT,
        R2RML_CLASS: RML_CLASS,
        R2RML_CHILD: RML_CHILD,
        R2RML_PARENT: RML_PARENT,
        R2RML_JOIN_CONDITION: RML_JOIN_CONDITION,
        R2RML_DATATYPE: RML_DATATYPE_SHORTCUT,
        R2RML_LANGUAGE: RML_LANGUAGE_SHORTCUT,
        R2RML_SQL_VERSION: RML_SQL_VERSION,
        R2RML_TERM_TYPE: RML_TERM_TYPE,
        R2RML_IRI: RML_IRI,
        R2RML_LITERAL: RML_LITERAL,
        R2RML_BLANK_NODE: RML_BLANK_NODE,
        R2RML_SQL2008: RML_SQL2008,
    }
    for r2rml_property, rml_property in r2rml_to_rml_dict.items():
        mapping_graph = replace_predicates_in_graph(mapping_graph, r2rml_property, rml_property)

    # replace R2RML objects with RML objects
    r2rml_to_rml_dict = {
        R2RML_TRIPLES_MAP_CLASS: RML_TRIPLES_MAP_CLASS,
        R2RML_LOGICAL_TABLE_CLASS: RML_LOGICAL_TABLE,
        R2RML_DEFAULT_GRAPH: RML_DEFAULT_GRAPH,
        R2RML_IRI: RML_IRI,
        R2RML_LITERAL: RML_LITERAL,
        R2RML_BLANK_NODE: RML_BLANK_NODE
    }
    for r2rml_object, rml_object in r2rml_to_rml_dict.items():
        mapping_graph = replace_objects_in_graph(mapping_graph, r2rml_object, rml_object)

    # replace rml:AssertedTriplesMap with rml:TriplesMap
    mapping_graph = replace_objects_in_graph(mapping_graph, RML_ASSERTED_TRIPLES_MAP_CLASS, RML_TRIPLES_MAP_CLASS)

    return mapping_graph


def _rml_legacy_to_rml(mapping_graph):
    """
    Replace RML legacy rules in the graph with the corresponding RML rules.
    """
    rml_legacy_to_rml_dict = {
        RML_LEGACY_LOGICAL_SOURCE: RML_LOGICAL_SOURCE,
        RML_LEGACY_SOURCE: RML_SOURCE,
        RML_LEGACY_QUERY: RML_QUERY,
        RML_LEGACY_ITERATOR: RML_ITERATOR,
        RML_LEGACY_REFERENCE: RML_REFERENCE,
        RML_LEGACY_REFERENCE_FORMULATION: RML_REFERENCE_FORMULATION,
        FNML_EXECUTION: RML_EXECUTION,
        FNML_INPUT: RML_INPUT,
        FNML_FUNCTION_MAP: RML_FUNCTION_MAP,
        FNML_RETURN_MAP: RML_RETURN_MAP,
        FNML_PARAMETER_MAP: RML_PARAMETER_MAP,
        FNML_VALUE_MAP: RML_VALUE_MAP,
        FNML_FUNCTION_SHORTCUT: RML_FUNCTION_SHORTCUT,
        FNML_RETURN_SHORTCUT: RML_RETURN_SHORTCUT,
        FNML_PARAMETER_SHORTCUT: RML_PARAMETER_SHORTCUT,
        FNML_VALUE_SHORTCUT: RML_VALUE_SHORTCUT,
        RML_LEGACY_NON_ASSERTED_TRIPLES_MAP_CLASS: RML_NON_ASSERTED_TRIPLES_MAP_CLASS,
        RML_LEGACY_QUOTED_TRIPLES_MAP: RML_QUOTED_TRIPLES_MAP,
        RML_LEGACY_SUBJECT_MAP: RML_SUBJECT_MAP,
        RML_LEGACY_OBJECT_MAP: RML_OBJECT_MAP,
    }

    for rml_legacy_property, rml_property in rml_legacy_to_rml_dict.items():
        mapping_graph = replace_predicates_in_graph(mapping_graph, rml_legacy_property, rml_property)

    return mapping_graph


def _expand_constant_shortcut_properties(mapping_graph):
    """
    Expand constant shortcut properties.
    See R2RML specification (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#constant).
    """

    constant_shortcuts_dict = {
        RML_SUBJECT_SHORTCUT: RML_SUBJECT_MAP,
        RML_PREDICATE_SHORTCUT: RML_PREDICATE_MAP,
        RML_OBJECT_SHORTCUT: RML_OBJECT_MAP,
        RML_LANGUAGE_SHORTCUT: RML_LANGUAGE_MAP,
        RML_DATATYPE_SHORTCUT: RML_DATATYPE_MAP,
        RML_GRAPH_SHORTCUT: RML_GRAPH_MAP,
        RML_FUNCTION_SHORTCUT: RML_FUNCTION_MAP,
        RML_RETURN_SHORTCUT: RML_RETURN_MAP,
        RML_PARAMETER_SHORTCUT: RML_PARAMETER_MAP,
        RML_VALUE_SHORTCUT: RML_VALUE_MAP
    }

    for constant_shortcut, constant_property in constant_shortcuts_dict.items():
        for s, o in mapping_graph.query(f'SELECT ?s ?o WHERE {{?s <{constant_shortcut}> ?o .}}'):
            blanknode = rdflib.BNode()
            mapping_graph.add((s, rdflib.term.URIRef(constant_property), blanknode))
            mapping_graph.add((blanknode, rdflib.term.URIRef(RML_CONSTANT), o))

        mapping_graph.remove((None, rdflib.term.URIRef(constant_shortcut), None))

    return mapping_graph


def _rdf_class_to_pom(mapping_graph):
    """
    Replace rr:class definitions by predicate object maps.
    """

    query = 'SELECT ?tm ?c WHERE { ' \
            f'?tm <{RML_SUBJECT_MAP}> ?sm . ' \
            f'?sm <{RML_CLASS}> ?c . }}'
    for tm, c in mapping_graph.query(query):
        blanknode = rdflib.BNode()
        mapping_graph.add((tm, rdflib.term.URIRef(RML_PREDICATE_OBJECT_MAP), blanknode))
        mapping_graph.add((blanknode, rdflib.term.URIRef(RML_PREDICATE_SHORTCUT), rdflib.RDF.type))
        mapping_graph.add((blanknode, rdflib.term.URIRef(RML_OBJECT_SHORTCUT), c))

    mapping_graph.remove((None, rdflib.term.URIRef(RML_CLASS), None))

    return mapping_graph


def _subject_graph_maps_to_pom(mapping_graph):
    """
    Move graph maps in subject maps to the predicate object maps of subject maps.
    """

    # add the graph maps in the subject maps to every predicate object map of the subject maps
    query = 'SELECT ?sm ?gm ?pom WHERE { ' \
            f'?tm <{RML_SUBJECT_MAP}> ?sm . ' \
            f'?sm <{RML_GRAPH_MAP}> ?gm . ' \
            f'?tm <{RML_PREDICATE_OBJECT_MAP}> ?pom . }}'
    for sm, gm, pom in mapping_graph.query(query):
        mapping_graph.add((pom, rdflib.term.URIRef(RML_GRAPH_MAP), gm))

    # remove the graph maps from the subject maps
    query = 'SELECT ?sm ?gm WHERE { ' \
            f'?tm <{RML_SUBJECT_MAP}> ?sm . ' \
            f'?sm <{RML_GRAPH_MAP}> ?gm . }}'
    for sm, gm in mapping_graph.query(query):
        mapping_graph.remove((sm, rdflib.term.URIRef(RML_GRAPH_MAP), gm))

    return mapping_graph


def _complete_pom_with_default_graph(mapping_graph):
    """
    Complete predicate object maps without graph maps with rr:defaultGraph.
    """

    query = 'SELECT DISTINCT ?tm ?pom WHERE { ' \
            f'?tm <{RML_PREDICATE_OBJECT_MAP}> ?pom . ' \
            f'OPTIONAL {{ ?pom <{RML_GRAPH_MAP}> ?gm . }} . ' \
            'FILTER ( !bound(?gm) ) }'
    for tm, pom in mapping_graph.query(query):
        blanknode = rdflib.BNode()
        mapping_graph.add((pom, rdflib.term.URIRef(RML_GRAPH_MAP), blanknode))
        mapping_graph.add((blanknode, rdflib.term.URIRef(RML_CONSTANT), rdflib.term.URIRef(RML_DEFAULT_GRAPH)))

    return mapping_graph


def _complete_termtypes(mapping_graph):
    """
    Completes term types of mapping rules that do not have rr:termType property as indicated in R2RML specification
    (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#termtype).
    """

    # add missing RDF-star triples termtypes (in the subject and object maps)
    query = 'SELECT DISTINCT ?term_map ?quoted_triples_map WHERE { ' \
            f'?term_map <{RML_QUOTED_TRIPLES_MAP}> ?quoted_triples_map . ' \
            f'OPTIONAL {{ ?term_map <{RML_TERM_TYPE}> ?termtype . }} . ' \
            'FILTER ( !bound(?termtype) ) }'
    for term_map, _ in mapping_graph.query(query):
        mapping_graph.add((term_map, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_RDF_STAR_TRIPLE)))

    # add missing blanknode termtypes in the constant-valued object maps
    query = 'SELECT DISTINCT ?term_map ?constant WHERE { ' \
            f'?term_map <{RML_CONSTANT}> ?constant . ' \
            f'OPTIONAL {{ ?term_map <{RML_TERM_TYPE}> ?termtype . }} . ' \
            'FILTER ( !bound(?termtype) && isBlank(?constant) ) }'
    for term_map, _ in mapping_graph.query(query):
        mapping_graph.add((term_map, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_BLANK_NODE)))

    # add missing literal termtypes in the constant-valued object maps
    query = 'SELECT DISTINCT ?term_map ?constant WHERE { ' \
            f'?term_map <{RML_CONSTANT}> ?constant . ' \
            f'OPTIONAL {{ ?term_map <{RML_TERM_TYPE}> ?termtype . }} . ' \
            'FILTER ( !bound(?termtype) && isLiteral(?constant) ) }'
    for term_map, _ in mapping_graph.query(query):
        mapping_graph.add((term_map, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_LITERAL)))

    # add missing literal termtypes in the object maps
    query = 'SELECT DISTINCT ?om ?pom WHERE { ' \
            f'?pom <{RML_OBJECT_MAP}> ?om . ' \
            f'OPTIONAL {{ ?om <{RML_TERM_TYPE}> ?termtype . }} . ' \
            f'OPTIONAL {{ ?om <{RML_REFERENCE}> ?reference . }} . ' \
            f'OPTIONAL {{ ?om <{RML_EXECUTION}> ?execution . }} . ' \
            f'OPTIONAL {{ ?om <{RML_LANGUAGE_MAP}> ?language . }} . ' \
            f'OPTIONAL {{ ?om <{RML_DATATYPE_MAP}> ?datatype . }} . ' \
            'FILTER ( !bound(?termtype) && (' \
            'bound(?reference) || bound(?execution) || bound(?language) || bound(?datatype) ) ) }'
    for om, _ in mapping_graph.query(query):
        mapping_graph.add((om, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_LITERAL)))

    # complete referencing object maps with the termtype coming from the subject of the parent
    query = 'SELECT DISTINCT ?term_map ?termtype WHERE { ' \
            f'?term_map <{RML_PARENT_TRIPLES_MAP}> ?parent_tm . ' \
            f'?parent_tm <{RML_SUBJECT_MAP}> ?parent_subject_map . ' \
            f'?parent_subject_map <{RML_TERM_TYPE}> ?termtype . }}'
    for term_map, termtype in mapping_graph.query(query):
        mapping_graph.add((term_map, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(termtype)))

    # now all missing termtypes are IRIs
    for term_map_property in [RML_SUBJECT_MAP, RML_PREDICATE_MAP, RML_OBJECT_MAP, RML_GRAPH_MAP]:
        query = 'SELECT DISTINCT ?term_map ?x WHERE { ' \
                f'?x <{term_map_property}> ?term_map . ' \
                f'OPTIONAL {{ ?term_map <{RML_TERM_TYPE}> ?termtype . }} . ' \
                'FILTER ( !bound(?termtype) ) }'
        for term_map, _ in mapping_graph.query(query):
            mapping_graph.add((term_map, rdflib.term.URIRef(RML_TERM_TYPE), rdflib.term.URIRef(RML_IRI)))

    return mapping_graph


def _complete_triples_map_class(mapping_graph):
    """
    Adds rr:TriplesMap typing for triples maps. For rml:NonAssertedTriplesMap remove rr:TriplesMap typing.
    Triples maps without predicate object maps are transformed to non-asserted triples maps as they do no generate
    triples (but can be used in join conditions in other triples maps).
    """

    query = 'SELECT DISTINCT ?triples_map ?logical_source WHERE { ' \
            f'?triples_map <{RML_LOGICAL_SOURCE}> ?logical_source . ' \
            f'OPTIONAL {{ ?triples_map a ?triples_map_class . }} . ' \
            'FILTER ( !bound(?triples_map_class) ) }'
    for triples_map, _ in mapping_graph.query(query):
        mapping_graph.add((triples_map, rdflib.term.URIRef(RDF_TYPE), rdflib.term.URIRef(RML_TRIPLES_MAP_CLASS)))

    # rr:TriplesMap without predicate object maps to rml:NonAssertedTriplesMaps
    query = 'SELECT DISTINCT ?triples_map ?logical_source WHERE { ' \
            f'?triples_map <{RML_LOGICAL_SOURCE}> ?logical_source . ' \
            f'OPTIONAL {{ ?triples_map <{RML_PREDICATE_OBJECT_MAP}> ?pom . }} . ' \
            'FILTER ( !bound(?pom) ) }'
    for triples_map, _ in mapping_graph.query(query):
        mapping_graph.add(
            (triples_map, rdflib.term.URIRef(RDF_TYPE), rdflib.term.URIRef(RML_NON_ASSERTED_TRIPLES_MAP_CLASS)))

    # for rml:NonAssertedTriplesMap remove triples typing them as rr:TriplesMap
    query = 'SELECT DISTINCT ?triples_map ?logical_source WHERE { ' \
            f'?triples_map <{RML_LOGICAL_SOURCE}> ?logical_source . ' \
            f'?triples_map a <{RML_TRIPLES_MAP_CLASS}> . ' \
            f'?triples_map a <{RML_NON_ASSERTED_TRIPLES_MAP_CLASS}> . }}'
    for triples_map, _ in mapping_graph.query(query):
        mapping_graph.remove((triples_map, rdflib.term.URIRef(RDF_TYPE), rdflib.term.URIRef(RML_TRIPLES_MAP_CLASS)))

    return mapping_graph


def _remove_string_datatypes(mapping_graph):
    """
    Removes xsd:string data types. xsd:string is equivalent to not specifying any data type.
    """

    mapping_graph.remove((None, rdflib.term.URIRef(RML_CONSTANT), rdflib.term.URIRef(XSD_STRING)))

    return mapping_graph


def _get_join_conditions_dict(join_query_results):
    """
    Creates a dictionary with the results of the JOIN_CONDITION_PARSING_QUERY. The keys are the identifiers of the
    child triples maps of the join condition. The values of the dictionary are in turn other dictionaries with two
    items, child_value and parent_value, representing a join condition.
    """

    join_conditions_dict = {}

    for join_condition in join_query_results:
        # add the child triples map identifier if it is not in the dictionary
        if join_condition.term_map not in join_conditions_dict:
            join_conditions_dict[join_condition.term_map] = {}

        # add the new join condition (note that several join conditions can apply in a join)
        join_conditions_dict[join_condition.term_map][str(join_condition.join_condition)] = \
            {'child_value': str(join_condition.child_value), 'parent_value': str(join_condition.parent_value)}

    return join_conditions_dict

def _translate_fnml_to_rml_functionmapping(mapping_graph):
    """
    Translates an FNML (Function Mapping Language) mapping to an RML (RDF Mapping Language) function mapping.
    This function processes a given RDF graph containing FNML mappings and converts them into RML-compatible
    function mappings. It queries the graph for function mappings, creates corresponding blank nodes for
    function executions, function maps, and input mappings, and adds these to the graph.
    Args:
        mapping_graph (rdflib.Graph): The RDF graph containing the FNML mappings to be translated.
    Returns:
        rdflib.Graph: The updated RDF graph with the translated RML function mappings.
    Notes:
        - The function uses a SPARQL query to extract FNML function mappings from the input graph.
        - It ensures that mappings are grouped by function caller and function ID, creating new blank nodes
          for each unique combination.
        - The Yarrrml-converter uses the R2RML namespace for certain mappings, which is accounted for in the
          SPARQL query filters.
    """
    FNML_FUNCTION_QUERY = """
PREFIX fnml: <http://semweb.mmlab.be/ns/fnml#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rml: <http://w3id.org/rml/>
PREFIX rr: <http://www.w3.org/ns/r2rml#>

SELECT *
WHERE {
  ?functionCaller fnml:functionValue [ rml:predicateObjectMap [ rml:predicateMap/rml:constant <https://w3id.org/function/ontology#executes> ;
       rml:objectMap/rml:constant ?functionID ], [ rml:predicateMap [ rml:constant ?parameter ] ;
       rml:objectMap [ ?_objectConnector ?_objectValue ] ] ] .
    OPTIONAL {    ?_callingOM fnml:functionValue ?_objectValue}
   
  # The Yarrrml-converter uses the R2RML namespace here, so I do not want to specify it.
  
  FILTER (!STRSTARTS(str(?parameter), str(rr:)))
 # FILTER (!STRSTARTS(str(?objectValue), str(rr:)))
  FILTER(?parameter != <https://w3id.org/function/ontology#executes>)
  FILTER(?_objectConnector != <http://w3id.org/rml/termType>)
  FILTER(?_objectConnector != rdf:type)
  BIND(IF(?_objectConnector = fnml:functionValue, rml:functionExecution, ?_objectConnector) AS ?objectConnector)
  BIND(IF(?_objectConnector = fnml:functionValue, ?_callingOM, ?_objectValue) AS ?objectValue)
  
}
ORDER BY ?functionCaller ?parameter
# """
    
    qres = mapping_graph.query(FNML_FUNCTION_QUERY)
    previous_functionCaller = None
    blank_functionexecution = None
    previous_functionID = None
    
    innerFunctionsToConnect = {}
    for row in qres:
        if previous_functionID != row.functionID:
            previous_functionID = row.functionID
            blanknode_functionmap = rdflib.BNode()
        if previous_functionCaller != row.functionCaller:
            previous_functionCaller = row.functionCaller
            blank_functionexecution = rdflib.BNode()
            mapping_graph.add((rdflib.URIRef(row.functionCaller), rdflib.term.URIRef("functionExecution", RML_NAMESPACE),blank_functionexecution))
            mapping_graph.add((blank_functionexecution, rdflib.URIRef("functionMap", RML_NAMESPACE), blanknode_functionmap))
            mapping_graph.add((blanknode_functionmap, rdflib.URIRef("constant", RML_NAMESPACE), row.functionID)) 
        
        blank_functioninput = rdflib.BNode()
        mapping_graph.add((blank_functionexecution, rdflib.term.URIRef("input", RML_NAMESPACE), blank_functioninput))
        blank_input_value_map = rdflib.BNode()
        mapping_graph.add((blank_functioninput, rdflib.term.URIRef("inputValueMap", RML_NAMESPACE), blank_input_value_map))
        if "functionExecution" in row.objectConnector :
            innerFunctionsToConnect[row.objectValue] = blank_input_value_map
        else:
            mapping_graph.add((blank_input_value_map, rdflib.term.URIRef(row.objectConnector), row.objectValue))
        blank_input_parameter_map = rdflib.BNode()
        mapping_graph.add((blank_functioninput, rdflib.URIRef("parameterMap", RML_NAMESPACE), blank_input_parameter_map))
        mapping_graph.add((blank_input_parameter_map, rdflib.term.URIRef("constant", RML_NAMESPACE), row.parameter))
        for inner_function_om,outer_blanknode in innerFunctionsToConnect.items():
            inner_function_blank = [e for e in mapping_graph.objects(rdflib.URIRef(inner_function_om), rdflib.term.URIRef("functionExecution", RML_NAMESPACE))]
            if(len(inner_function_blank) > 0):
                mapping_graph.add((outer_blanknode, rdflib.term.URIRef("functionExecution", RML_NAMESPACE), inner_function_blank[0]))
                
    mapping_graph.update("""
PREFIX fnml: <http://semweb.mmlab.be/ns/fnml#>
PREFIX rml: <http://w3id.org/rml/>
PREFIX rr: <http://www.w3.org/ns/r2rml#>

DELETE {
  ?functionCaller fnml:functionValue ?functionValue .
  ?functionValue ?functionMap ?POMMaps .
  ?POMMaps ?DeeperMaps ?PredicateAndObjectMaps .
}
WHERE {
  ?functionCaller fnml:functionValue ?functionValue .
  ?functionValue ?functionMap ?POMMaps .
  ?POMMaps ?DeeperMaps ?PredicateAndObjectMaps .
  FILTER(?functionMap != rml:logicalSource)
} """)
    return mapping_graph   
    
def _transform_mappings_into_dataframe(mapping_graph, section_name):
    """
    Builds a Pandas DataFrame from the results obtained from MAPPING_PARSING_QUERY and
    JOIN_CONDITION_PARSING_QUERY for one source.
    """

    # parse the mappings with the parsing queries
    rml_query_results = mapping_graph.query(RML_PARSING_QUERY)
    join_query_results = mapping_graph.query(RML_JOIN_CONDITION_PARSING_QUERY)
    fnml_query_results = mapping_graph.query(FNML_PARSING_QUERY)

    # RML in graph to DataFrame
    rml_df = pd.DataFrame(rml_query_results.bindings)
    rml_df.columns = rml_df.columns.map(str)

    # process mapping rules with joins
    # create a dict with child triples maps in the keys and its join conditions in the values
    join_conditions_dict = _get_join_conditions_dict(join_query_results)
    # map the dict with the join conditions to the mapping rules in the DataFrame
    rml_df['object_join_conditions'] = rml_df['object_map'].map(join_conditions_dict)
    rml_df['subject_join_conditions'] = rml_df['subject_map'].map(join_conditions_dict)
    # needed for later hashing the dataframe
    rml_df['object_join_conditions'] = rml_df['object_join_conditions'].where(
        pd.notna(rml_df['object_join_conditions']), '')
    rml_df['subject_join_conditions'] = rml_df['subject_join_conditions'].where(
        pd.notna(rml_df['subject_join_conditions']), '')
    # convert the join condition dicts to string (can later be converted back to dict)
    rml_df['object_join_conditions'] = rml_df['object_join_conditions'].astype(str)
    rml_df['subject_join_conditions'] = rml_df['subject_join_conditions'].astype(str)

    # convert all values to string
    for i, row in rml_df.iterrows():
        for col in rml_df.columns:
            if pd.notna(row[col]):
                rml_df.at[i, col] = str(row[col])

    # link the mapping rules to their data source name
    rml_df['source_name'] = section_name

    # subject_map and object_map columns were used to handle join conditions, no longer needed
    rml_df = rml_df.drop(columns=['subject_map', 'object_map'])

    # FNML in graph to DataFrame
    fnml_df = pd.DataFrame(fnml_query_results.bindings)
    fnml_df.columns = fnml_df.columns.map(str)
    fnml_df = fnml_df.map(str)

    # ----------------------------------------------------------------------------------------------
    # TEMPORAL FOR HTTP API SUPPORT
    q = """
        prefix rml: <http://w3id.org/rml/>
        prefix htv: <http://www.w3.org/2011/http#>

        SELECT DISTINCT ?source ?absolute_path ?field_name ?field_value
        WHERE {
        ?source htv:absolutePath ?absolute_path .
        OPTIONAL {
        ?source htv:headers ?headers .
        ?hearders htv:fieldName ?field_name .
        ?hearders htv:fieldValue ?field_value .
        } .
        }
        """

    http_api_df = pd.DataFrame(mapping_graph.query(q).bindings)
    http_api_df.columns = http_api_df.columns.map(str)
    http_api_df = http_api_df.map(str)
    # ----------------------------------------------------------------------------------------------

    return rml_df, fnml_df, http_api_df


def _is_delimited_identifier(identifier):
    """
    Checks if an identifier is delimited or not.
    """

    if len(identifier) > 2:
        if identifier[0] == '"' and identifier[len(identifier) - 1] == '"':
            return True
    return False


def _get_undelimited_identifier(identifier):
    """
    Removes delimiters from the identifier if it is delimited.
    """

    identifier = identifier
    if _is_delimited_identifier(identifier):
        return identifier[1:-1]
    return identifier


def _get_valid_template_identifiers(template):
    """
    Removes delimiters from delimited identifiers in a template.
    """

    return template.replace('{"', '{').replace('"}', '}')


def _validate_termtypes(mapping_graph):
    query = 'SELECT DISTINCT ?termtype ?pm WHERE { ' \
            f'?pom <{RML_PREDICATE_MAP}> ?pm . ' \
            f'?pm <{RML_TERM_TYPE}> ?termtype . }}'
    predicate_termtypes = set([str(termtype) for termtype, _ in mapping_graph.query(query)])
    if not (predicate_termtypes <= {RML_IRI}):
        raise ValueError(f'Found an invalid predicate termtype. Found values {predicate_termtypes}. '
                         f'Predicate maps must be {RML_IRI}.')

    query = 'SELECT DISTINCT ?termtype ?gm WHERE { ' \
            f'?pom <{RML_GRAPH_MAP}> ?gm . ' \
            f'?gm <{RML_TERM_TYPE}> ?termtype . }}'
    graph_termtypes = set([str(termtype) for termtype, _ in mapping_graph.query(query)])
    if not (graph_termtypes <= {RML_IRI}):
        raise ValueError(f'Found an invalid graph termtype. Found values {graph_termtypes}. '
                         f'Graph maps must be {RML_IRI}.')

    query = 'SELECT DISTINCT ?termtype ?sm WHERE { ' \
            f'?tm <{RML_SUBJECT_MAP}> ?sm . ' \
            f'?sm <{RML_TERM_TYPE}> ?termtype . }}'
    subject_termtypes = set([str(termtype) for termtype, _ in mapping_graph.query(query)])
    if not (subject_termtypes <= {RML_IRI, RML_BLANK_NODE, RML_RDF_STAR_TRIPLE}):
        raise ValueError(f'Found an invalid subject termtype. Found values {subject_termtypes}. '
                         f'Subject maps must be {RML_IRI}, {RML_BLANK_NODE} or {RML_RDF_STAR_TRIPLE}.')

    query = 'SELECT DISTINCT ?termtype ?om WHERE { ' \
            f'?pom <{RML_OBJECT_MAP}> ?om . ' \
            f'?om <{RML_TERM_TYPE}> ?termtype . }}'
    object_termtypes = set([str(termtype) for termtype, _ in mapping_graph.query(query)])
    if not (object_termtypes <= {RML_IRI, RML_BLANK_NODE, RML_LITERAL, RML_RDF_STAR_TRIPLE}):
        raise ValueError(f'Found an invalid object termtype. Found values {object_termtypes}. Object maps must be '
                         f'{RML_IRI}, {RML_BLANK_NODE}, {RML_LITERAL} or {RML_RDF_STAR_TRIPLE}.')


class MappingParser:

    def __init__(self, config):
        self.rml_df = pd.DataFrame(columns=RML_DATAFRAME_COLUMNS)
        self.fnml_df = pd.DataFrame(columns=FNML_DATAFRAME_COLUMNS)
        self.http_api_df = pd.DataFrame()
        self.config = config

    def __str__(self):
        return str(self.rml_df)

    def __repr__(self):
        return repr(self.rml_df)

    def __len__(self):
        return len(self.rml_df)

    def parse_mappings(self):
        self._get_from_r2_rml()
        self._preprocess_mappings()

        self._infer_datatypes()
        self.validate_mappings()

        LOGGER.info(f'{len(self.rml_df)} mapping rules retrieved.')

        # replace empty strings with NaN
        self.rml_df = self.rml_df.infer_objects(copy=False).replace(r'^\s*$', None, regex=True)

        # generate mapping partitions
        mapping_partitioner = MappingPartitioner(self.rml_df, self.config)
        self.rml_df = mapping_partitioner.partition_mappings()

        return self.rml_df, self.fnml_df, self.http_api_df

    def _get_from_r2_rml(self):
        """
        Parses the mapping files of all data sources in the config file and adds the parsed mappings rules to a
        common DataFrame for all data sources. If parallelization is enabled and multiple data sources are provided,
        each mapping file is parsed in parallel.
        """

        # previously parsing was paralellized
        #if self.config.is_multiprocessing_enabled() and self.config.has_multiple_data_sources():
        #    pool = mp.Pool(self.config.get_number_of_processes())
        #    rml_dfs = pool.map(self._parse_data_source_mapping_files, self.config.get_data_sources_sections())
        #    self.rml_df = pd.concat([self.rml_df, pd.concat(rml_dfs)])
        #else:
        for section_name in self.config.get_data_sources_sections():
            data_source_rml_df, data_source_fnml_df, data_source_http_api_df = self._parse_data_source_mapping_files(section_name)
            self.rml_df = pd.concat([self.rml_df, data_source_rml_df])
            self.fnml_df = pd.concat([self.fnml_df, data_source_fnml_df])
            self.http_api_df = pd.concat([self.http_api_df, data_source_http_api_df])

        self.rml_df = self.rml_df.reset_index(drop=True)
        self.fnml_df = self.fnml_df.reset_index(drop=True)
        self.http_api_df = self.http_api_df.reset_index(drop=True)

    def _parse_data_source_mapping_files(self, section_name):
        """
        Creates a Pandas DataFrame with the mapping rules of a data source. It loads the mapping files in an rdflib
        graph and recognizes the mapping language used. Mappings are translated to RML.
        It performs queries MAPPING_PARSING_QUERY and JOIN_CONDITION_PARSING_QUERY and process the results to build a
        DataFrame with the mapping rules. Also verifies that there are not repeated triples maps in the mappings.
        """

        # create an empty graph
        mapping_graph = rdflib.Graph()

        mapping_file_paths = self.config.get_mappings_files(section_name)
        # load mapping rules to the graph
        for f in mapping_file_paths:
            if f.endswith('.yarrrml') or f.endswith('.yml') or f.endswith('.yaml'):
                mapping_graph += load_yarrrml(f)
            else:
                # mapping is in an RDF serialization
                try:
                    # provide file extension when parsing
                    mapping_graph.parse(f, format=os.path.splitext(f)[1][1:].strip())
                except:
                    # if a file extension such as .rml or .r2rml is used, assume it is turtle (issue #80)
                    mapping_graph.parse(f)

        # convert R2RML to RML
        mapping_graph = _r2rml_to_rml(mapping_graph)
        # convert legacy RML to RML
        mapping_graph = _rml_legacy_to_rml(mapping_graph)
        # convert rr:class to new POMs
        mapping_graph = _rdf_class_to_pom(mapping_graph)
        # expand constant shortcut properties rr:subject, rr:predicate, rr:object and rr:graph
        mapping_graph = _expand_constant_shortcut_properties(mapping_graph)
        # move graph maps in subject maps to the predicate object maps of subject maps
        mapping_graph = _subject_graph_maps_to_pom(mapping_graph)
        # complete predicate object maps without graph maps with rr:defaultGraph
        mapping_graph = _complete_pom_with_default_graph(mapping_graph)
        # if a term as no associated rr:termType, complete it according to R2RML specification
        mapping_graph = _complete_termtypes(mapping_graph)
        # add rr:TriplesMap typing
        mapping_graph = _complete_triples_map_class(mapping_graph)
        
        mapping_graph = _translate_fnml_to_rml_functionmapping(mapping_graph)
        
        # check termtypes are correct
        _validate_termtypes(mapping_graph)

        # create RML and FNML dataframes
        return _transform_mappings_into_dataframe(mapping_graph, section_name)

    def _preprocess_mappings(self):
        # start by removing duplicated triples
        self.rml_df = self.rml_df.drop_duplicates()

        # complete rml:source with file_paths specified in the config file
        self._complete_rml_source_with_config_file_paths()

        # complete source type with config parameters and data file extensions
        self._complete_source_types()

        # ignore the delimited identifiers (this is not conformant with R2MRL specification)
        self._remove_delimiters_from_mappings()

        self._normalize_rml_star()

        self._remove_self_joins_no_condition()

    def _complete_source_types(self):
        """
        Adds a column with the source type and removes reference formulation column.
        The source type is inferred for RDB through the parameter db_url provided in the mapping file.
        If db_url is not provided but the logical source is rml:query, then it is an RML tabular view.
        For data files the source type is inferred from the file extension.
        """

        for i, rml_rule in self.rml_df.iterrows():
            if pd.notna(rml_rule['reference_formulation']) and 'SQL' in rml_rule['reference_formulation'].upper():
                self.rml_df.at[i, 'source_type'] = RDB
            elif pd.notna(rml_rule['reference_formulation']) and 'CYPHER' in rml_rule['reference_formulation'].upper():
                self.rml_df.at[i, 'source_type'] = PGDB
            elif self.config.has_db_url(rml_rule['source_name']):
                # if db_url but no reference formulation, assume it is a relational database
                self.rml_df.at[i, 'source_type'] = RDB
            elif rml_rule['logical_source_type'] == RML_QUERY:
                # it is a query, but it is not a DB (because no db_url), hence it is a tabular view
                # assign CSV (it can also be Apache Parquet but format is automatically inferred)
                self.rml_df.at[i, 'source_type'] = CSV
            elif rml_rule['logical_source_type'] == RML_SOURCE \
                    and self.rml_df.at[i, 'logical_source_value'].startswith('{') \
                    and self.rml_df.at[i, 'logical_source_value'].endswith('}'):
                # it is an in-memory data structure
                self.rml_df.at[i, 'source_type'] = PYTHON_SOURCE
            elif rml_rule['logical_source_type'] == RML_SOURCE:
                # it is a file, infer source type from file extension
                file_extension = os.path.splitext(str(rml_rule['logical_source_value']))[1][1:].strip()
                if file_extension.upper() in FILE_SOURCE_TYPES:
                    self.rml_df.at[i, 'source_type'] = file_extension.upper()
                elif pd.notna(rml_rule['reference_formulation']):
                    # if file extension is not recognized, use reference formulation
                    self.rml_df.at[i, 'source_type'] = rml_rule['reference_formulation'].replace(RML_NAMESPACE, '').upper()
                else:
                    raise Exception('No source type could be retrieved for some mapping rules.')
            else:
                raise Exception('No source type could be retrieved for some mapping rules.')

        self.rml_df.drop(columns='reference_formulation', inplace=True)

    def _complete_rml_source_with_config_file_paths(self):
        """
        Overrides rml:source in the mappings with the file_path parameter in the config file for each data source
        section if provided.
        """

        for section_name in self.config.get_data_sources_sections():
            if self.config.has_file_path(section_name):
                self.rml_df.loc[
                    self.rml_df['source_name'] == section_name, 'logical_source_type'] = RML_SOURCE
                self.rml_df.loc[self.rml_df['source_name'] == section_name, 'logical_source_value'] = \
                    self.config.get_file_path(section_name)

    def _remove_delimiters_from_mappings(self):
        """
        Removes delimiters from all identifiers in the mapping rules in the input DataFrame.
        """

        for i, rml_rule in self.rml_df.iterrows():
            if self.rml_df.at[i, 'logical_source_type'] == RML_TABLE_NAME:
                self.rml_df.at[i, 'logical_source_value'] = _get_undelimited_identifier(
                    rml_rule['logical_source_value'])

            if self.rml_df.at[i, 'subject_map_type'] == RML_TEMPLATE:
                self.rml_df.at[i, 'subject_map_value'] = _get_valid_template_identifiers(
                    rml_rule['subject_map_value'])
            elif self.rml_df.at[i, 'subject_map_type'] == RML_REFERENCE:
                self.rml_df.at[i, 'subject_map_value'] = _get_undelimited_identifier(
                    rml_rule['subject_map_value'])

            if self.rml_df.at[i, 'predicate_map_type'] == RML_TEMPLATE:
                self.rml_df.at[i, 'predicate_map_value'] = _get_valid_template_identifiers(
                    rml_rule['predicate_map_value'])
            elif self.rml_df.at[i, 'predicate_map_type'] == RML_REFERENCE:
                self.rml_df.at[i, 'predicate_map_value'] = _get_undelimited_identifier(
                    rml_rule['predicate_map_value'])

            if self.rml_df.at[i, 'object_map_type'] == RML_TEMPLATE:
                self.rml_df.at[i, 'object_map_value'] = _get_valid_template_identifiers(
                    rml_rule['object_map_value'])
            elif self.rml_df.at[i, 'object_map_type'] == RML_REFERENCE:
                self.rml_df.at[i, 'object_map_value'] = _get_undelimited_identifier(
                    rml_rule['object_map_value'])

            if self.rml_df.at[i, 'graph_map_type'] == RML_TEMPLATE:
                self.rml_df.at[i, 'graph_map_value'] = _get_valid_template_identifiers(
                    rml_rule['graph_map_value'])
            elif self.rml_df.at[i, 'graph_map_type'] == RML_REFERENCE:
                self.rml_df.at[i, 'graph_map_value'] = _get_undelimited_identifier(
                    rml_rule['graph_map_value'])

            # if join_condition is not null and it is not empty
            for join_conditions_pos in ['subject_join_conditions', 'object_join_conditions']:
                if pd.notna(rml_rule[join_conditions_pos]) and rml_rule[join_conditions_pos]:
                    join_conditions = eval(rml_rule[join_conditions_pos])
                    for key, value in join_conditions.items():
                        join_conditions[key]['child_value'] = _get_undelimited_identifier(
                            join_conditions[key]['child_value'])
                        join_conditions[key]['parent_value'] = _get_undelimited_identifier(
                            join_conditions[key]['parent_value'])
                        self.rml_df.at[i, join_conditions_pos] = str(join_conditions)

    def _infer_datatypes(self):
        """
        Get RDF datatypes for rules corresponding to relational data sources if they are not overridden in the mapping
        rules. The inferring of RDF datatypes is defined in R2RML specification
        (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#natural-mapping).
        """

        # return if datatype inferring is disabled in the config
        if not self.config.infer_sql_datatypes():
            return

        for i, rml_rule in self.rml_df.iterrows():
            # datatype inference only applies to relational data sources
            if (rml_rule['source_type'] == RDB) and (
                    # datatype inference only applies to literals
                    str(rml_rule['object_termtype']) == RML_LITERAL) and (
                    # if the literal has a language tag or an overridden datatype, datatype inference does not apply
                    pd.isna(rml_rule['lang_datatype'])):

                if rml_rule['object_map_type'] == RML_REFERENCE:
                    inferred_data_type = get_rdb_reference_datatype(self.config, rml_rule,
                                                                    rml_rule['object_map_value'])

                    if not inferred_data_type:
                        # no data type was inferred
                        continue

                    self.rml_df.at[i, 'lang_datatype'] = RML_DATATYPE_MAP
                    self.rml_df.at[i, 'lang_datatype_map_type'] = RML_CONSTANT
                    self.rml_df.at[i, 'lang_datatype_map_value'] = inferred_data_type
                    if self.rml_df.at[i, 'logical_source_type'] == RML_TABLE_NAME:
                        LOGGER.debug(f"`{inferred_data_type}` datatype inferred for column "
                                      f"`{rml_rule['object_map_value']}` of table "
                                      f"`{rml_rule['logical_source_value']}` "
                                      f"in data source `{rml_rule['source_name']}`.")
                    elif self.rml_df.at[i, 'logical_source_type'] == RML_QUERY:
                        LOGGER.debug(f"`{inferred_data_type}` datatype inferred for reference "
                                      f"`{rml_rule['object_map_value']}` in query "
                                      f"[{rml_rule['logical_source_value']}] "
                                      f"in data source `{rml_rule['source_name']}`.")

    def validate_mappings(self):
        """
        Checks that the mapping rules in the input DataFrame are valid. If something is wrong in the mappings the
        execution is stopped. Specifically it is checked that language tags and
        datatypes are used properly. Also checks that different data sources do not have triples map with the same id.
        """

        """
        # if there is a datatype or language tag then the object map termtype must be a rr:Literal
        if len(self.rml_df.loc[(self.rml_df['object_termtype'] != RML_LITERAL) &
                               pd.notna(self.rml_df['lang_datatype'])]) > 0:
            raise Exception('Found object maps with a language tag or a datatype, '
                            'but that do not have termtype rml:Literal.')

        # check that language tags are valid
        language_tags = set(self.rml_df['object_language'].dropna())
        # the place to look for valid language subtags is the IANA Language Subtag Registry
        # (https://www.iana.org/assignments/language-subtag-registry/language-subtag-registry)
        for language_tag in language_tags:
            # in general, if the language subtag is longer than 3 characters it is not valid
            if len(language_tag.split('-')[0]) > 3:
                raise ValueError(f'Found invalid language tag `{language_tag}`. '
                                 'Language tags must be in the IANA Language Subtag Registry.')
        """

        # check that a triples map id is not repeated in different data sources
        # Get unique source names and triples map identifiers
        aux_rml_df = self.rml_df[['source_name', 'triples_map_id']].drop_duplicates()
        # get repeated triples map identifiers
        repeated_triples_map_ids = get_repeated_elements_in_list(list(aux_rml_df['triples_map_id'].astype(str)))
        # of those repeated identifiers
        repeated_triples_map_ids = [tm_id for tm_id in repeated_triples_map_ids]
        if len(repeated_triples_map_ids) > 0:
            raise Exception('The following triples maps appear in more than one data source: '
                            f'{repeated_triples_map_ids}. '
                            'Check the mapping files, one triple map cannot be repeated in different data sources.')

    def _normalize_rml_star(self):
        num_rules_before_expansion = len(self.rml_df)
        while True:
            self._expand_rml_star()
            if num_rules_before_expansion == len(self.rml_df):
                break
            else:
                num_rules_before_expansion = len(self.rml_df)
                self._expand_rml_star()

    def _expand_rml_star(self):
        # create a unique id for each (normalized) mapping rule
        self.rml_df.insert(0, 'id', self.rml_df.reset_index(drop=True).index.astype(str))
        self.rml_df['id'] = '#TM' + self.rml_df['id']

        # create dicts from triples maps to ids and vice versa
        tm_to_id_list_dict = {}
        tm_to_id_dict = {}
        id_to_tm_dict = dict(zip(self.rml_df['id'], self.rml_df['triples_map_id']))
        for rule_id, rule_tm in id_to_tm_dict.items():
            if rule_tm in tm_to_id_list_dict:
                tm_to_id_list_dict[rule_tm].append(rule_id)
            else:
                tm_to_id_dict[rule_tm] = rule_id
                tm_to_id_list_dict[rule_tm] = [rule_id]

        # for quoted maps and ref object maps, add a new rule for each normalized rule they are referencing
        for position in ['subject', 'object']:
            quoted_tm_df = self.rml_df.loc[self.rml_df[f'{position}_map_type'] == RML_QUOTED_TRIPLES_MAP]
            for i, rml_rule in quoted_tm_df.iterrows():
                for tm_id in tm_to_id_list_dict[rml_rule[f'{position}_map_value']]:
                    rml_rule[f'{position}_map_value'] = tm_id
                    self.rml_df = pd.concat([self.rml_df, rml_rule.to_frame().T], ignore_index=True)

        # replace the old references with to triples maps with the new ids of the tiples maps
        # this generates duplicates with the newly added rules, remove the duplicates
        self.rml_df['subject_map_value'] = self.rml_df['subject_map_value'].map(tm_to_id_dict).fillna(
            self.rml_df['subject_map_value'])
        self.rml_df['object_map_value'] = self.rml_df['object_map_value'].map(tm_to_id_dict).fillna(
            self.rml_df['object_map_value'])
        self.rml_df = self.rml_df.drop_duplicates()

        # replace the old triples map ids with the new ids
        self.rml_df['triples_map_id'] = self.rml_df['id']
        self.rml_df = self.rml_df.drop(columns='id')

    # TODO: deprecate
    def _remove_self_joins_no_condition(self):
        for i, rml_rule in self.rml_df.iterrows():
            if rml_rule['object_map_type'] == RML_PARENT_TRIPLES_MAP:
                parent_triples_map_rule = get_rml_rule(self.rml_df, rml_rule['object_map_value'])
                if rml_rule['logical_source_value'] == parent_triples_map_rule['logical_source_value'] and str(
                        # str() is to be able to compare None
                        rml_rule['iterator']) == str(parent_triples_map_rule['iterator']):

                    remove_join = True
                    # check that all conditions in the join condition have the same references
                    try:
                        join_conditions = eval(rml_rule['object_join_conditions'])
                        for key, join_condition in join_conditions.items():
                            if join_condition['child_value'] != join_condition['parent_value']:
                                remove_join = False
                    except:
                        # eval() has failed because there are no join conditions, the join can be removed
                        remove_join = True

                    if remove_join and pd.notna(rml_rule['object_join_conditions']):
                        self.rml_df.at[i, 'object_map_type'] = parent_triples_map_rule.at['subject_map_type']
                        self.rml_df.at[i, 'object_map_value'] = parent_triples_map_rule.at['subject_map_value']
                        self.rml_df.at[i, 'object_termtype'] = parent_triples_map_rule.at['subject_termtype']
                        self.rml_df.at[i, 'object_join_conditions'] = None
                        LOGGER.debug(f"Removed self-join from mapping rule `{rml_rule['triples_map_id']}`.")
