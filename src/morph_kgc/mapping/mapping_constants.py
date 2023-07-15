__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


##############################################################################
#######################   RML DATAFRAME COLUMNS   ############################
##############################################################################

RML_DATAFRAME_COLUMNS = [
    'source_name', 'triples_map_id', 'triples_map_type', 'logical_source_type', 'logical_source_value', 'iterator',
    'subject_map_type', 'subject_map_value', 'subject_termtype',
    'predicate_map_type', 'predicate_map_value',
    'object_map_type', 'object_map_value', 'object_termtype', 'object_datatype', 'object_language',
    'graph_map_type', 'graph_map_value',
    'subject_join_conditions', 'object_join_conditions'
]


##############################################################################
#######################   FNML DATAFRAME COLUMNS   ############################
##############################################################################

FNML_DATAFRAME_COLUMNS = [
    'function_execution', 'function_map_value', 'parameter_map_value', 'value_map_type', 'value_map_value'
]


##############################################################################
########################   RML PARSING QUERIES   #############################
##############################################################################

RML_PARSING_QUERY = """
    prefix rml: <http://w3id.org/rml/>
    prefix sd: <https://w3id.org/okn/o/sd/>

    SELECT DISTINCT 
        ?triples_map_id ?triples_map_type ?logical_source_type ?logical_source_value ?iterator 
        ?subject_map_type ?subject_map_value ?subject_map ?subject_termtype
        ?predicate_map_type ?predicate_map_value
        ?object_map_type ?object_map_value ?object_map ?object_termtype ?object_datatype ?object_language
        ?graph_map_type ?graph_map_value

    WHERE {
        ?triples_map_id rml:logicalSource ?_source ;
                        a ?triples_map_type .
        OPTIONAL {
            # logical_source is optional because it can be specified with file_path in config (see #119)
            ?_source ?logical_source_type ?logical_source_value .
            OPTIONAL {
                ?logical_source_value sd:name ?logical_source_in_memory_value.
                BIND(CONCAT("{",?logical_source_in_memory_value,"}") AS ?logical_source_value)
            }
            FILTER ( ?logical_source_type IN ( rml:source, rml:tableName, rml:query ) ) .
        }
        OPTIONAL { ?_source rml:iterator ?iterator . }

    # Subject -------------------------------------------------------------------------
        ?triples_map_id rml:subjectMap ?subject_map .
        ?subject_map ?subject_map_type ?subject_map_value .
        FILTER ( ?subject_map_type IN (
                            rml:constant, rml:template, rml:reference, rml:quotedTriplesMap, rml:functionExecution ) ) .
        OPTIONAL { ?subject_map rml:termType ?subject_termtype . }

    # Predicate -----------------------------------------------------------------------
        OPTIONAL {
            ?triples_map_id rml:predicateObjectMap ?_predicate_object_map .
            ?_predicate_object_map rml:predicateMap ?_predicate_map .
            ?_predicate_map ?predicate_map_type ?predicate_map_value .
            FILTER ( ?predicate_map_type IN ( rml:constant, rml:template, rml:reference, rml:functionExecution ) ) .

    # Object --------------------------------------------------------------------------
            OPTIONAL {
                ?_predicate_object_map rml:objectMap ?object_map .
                ?object_map ?object_map_type ?object_map_value .
                FILTER ( ?object_map_type IN (
                            rml:constant, rml:template, rml:reference, rml:quotedTriplesMap, rml:functionExecution ) ) .
                OPTIONAL { ?object_map rml:termType ?object_termtype . }
                OPTIONAL { ?object_map rml:datatype ?object_datatype . }
                OPTIONAL { ?object_map rml:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rml:objectMap ?object_map .
                ?object_map rml:parentTriplesMap ?object_map_value .
                OPTIONAL { ?object_map rml:termType ?object_termtype . }
                BIND ( rml:parentTriplesMap AS ?object_map_type ) .
            }
            OPTIONAL {
                ?_predicate_object_map rml:graphMap ?graph_map .
                ?graph_map ?graph_map_type ?graph_map_value .
                FILTER ( ?graph_map_type IN ( rml:constant, rml:template, rml:reference, rml:functionExecution ) ) .
            }
        }
    }
"""

RML_JOIN_CONDITION_PARSING_QUERY = """
    prefix rml: <http://w3id.org/rml/>

    SELECT DISTINCT ?term_map ?join_condition ?child_value ?parent_value
    WHERE {
        ?term_map rml:joinCondition ?join_condition .
        ?join_condition rml:child ?child_value; rml:parent ?parent_value .
    }
"""


##############################################################################
########################   FNML PARSING QUERY   ###############################
##############################################################################

FNML_PARSING_QUERY = """
    prefix rml: <http://w3id.org/rml/>

    SELECT DISTINCT
        ?function_execution ?function_map_value ?parameter_map_value ?value_map_type ?value_map_value

    WHERE {

    # FuntionMap ----------------------------------------------------------------------

        ?function_execution rml:functionMap ?function_map .        
        ?function_map rml:constant ?function_map_value .

        # return maps are not used in the current implementation, default is first return value

    # Input ---------------------------------------------------------------------------

        OPTIONAL {
            # OPTIONAL because a function can have 0 arguments (e.g., uuid())
            ?function_execution rml:input ?input .

            ?input rml:parameterMap ?parameter_map .
            ?parameter_map rml:constant ?parameter_map_value .

            ?input rml:inputValueMap ?value_map .
            ?value_map ?value_map_type ?value_map_value .
            FILTER ( ?value_map_type IN ( rml:constant, rml:template, rml:reference, rml:functionExecution ) ) .
        }
    }
"""
