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
    'subject_map_type', 'subject_map_value', 'subject_map', 'subject_termtype',
    'predicate_map_type', 'predicate_map_value',
    'object_map_type', 'object_map_value', 'object_map', 'object_termtype', 'object_datatype', 'object_language',
    'graph_map_type', 'graph_map_value',
    'subject_join_conditions', 'object_join_conditions'
]


##############################################################################
#######################   FnO DATAFRAME COLUMNS   ############################
##############################################################################

FNO_DATAFRAME_COLUMNS = [
    'execution', 'parameter_map_type ', 'parameter_map_value', 'parameter_name', 'parameter_type'
]


##############################################################################
########################   RML PARSING QUERIES   #############################
##############################################################################

RML_PARSING_QUERY = """
    prefix rr: <http://www.w3.org/ns/r2rml#>
    prefix rml: <http://semweb.mmlab.be/ns/rml#>
    prefix fnml: <http://semweb.mmlab.be/ns/fnml#>

    SELECT DISTINCT
        ?triples_map_id ?triples_map_type ?logical_source_type ?logical_source_value ?iterator
        ?subject_map_type ?subject_map_value ?subject_map ?subject_termtype
        ?predicate_map_type ?predicate_map_value
        ?object_map_type ?object_map_value ?object_map ?object_termtype ?object_datatype ?object_language
        ?graph_map_type ?graph_map_value

    WHERE {
        ?triples_map_id rml:logicalSource ?_source ;
                        a ?triples_map_type .
        OPTIONAL {  # logical_source is optional because it can be specified with file_path in config (see #119)
            ?_source ?logical_source_type ?logical_source_value .
            FILTER ( ?logical_source_type IN ( rml:source, rr:tableName, rml:query ) ) .
        }
        OPTIONAL { ?_source rml:iterator ?iterator . }

    # Subject -------------------------------------------------------------------------
        ?triples_map_id rml:subjectMap ?subject_map .
        ?subject_map ?subject_map_type ?subject_map_value .
        FILTER ( ?subject_map_type IN ( rr:constant, rr:template, rml:reference, rml:quotedTriplesMap, fnml:execution ) ) .
        OPTIONAL { ?subject_map rr:termType ?subject_termtype . }

    # Predicate -----------------------------------------------------------------------
        OPTIONAL {
            ?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
            ?_predicate_object_map rr:predicateMap ?_predicate_map .
            ?_predicate_map ?predicate_map_type ?predicate_map_value .
            FILTER ( ?predicate_map_type IN ( rr:constant, rr:template, rml:reference ) ) .

    # Object --------------------------------------------------------------------------
            OPTIONAL {
                ?_predicate_object_map rml:objectMap ?object_map .
                ?object_map ?object_map_type ?object_map_value .
                FILTER ( ?object_map_type IN ( rr:constant, rr:template, rml:reference, rml:quotedTriplesMap, fnml:execution ) ) .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rml:objectMap ?object_map .
                ?object_map rr:parentTriplesMap ?object_map_value .
                BIND ( rr:parentTriplesMap AS ?object_map_type ) .
            }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?graph_map .
                ?graph_map ?graph_map_type ?graph_map_value .
                FILTER ( ?graph_map_type IN ( rr:constant, rr:template, rml:reference ) ) .
            }
        }
    }
"""


RML_JOIN_CONDITION_PARSING_QUERY = """
    prefix rr: <http://www.w3.org/ns/r2rml#>

    SELECT DISTINCT ?term_map ?join_condition ?child_value ?parent_value
    WHERE {
        ?term_map rr:joinCondition ?join_condition .
        ?join_condition rr:child ?child_value; rr:parent ?parent_value .
    }
"""


##############################################################################
########################   FnO PARSING QUERY   ###############################
##############################################################################

FNO_PARSING_QUERY = """
    prefix rr: <http://www.w3.org/ns/r2rml#>
    prefix rml: <http://semweb.mmlab.be/ns/rml#>
    prefix fnml: <http://semweb.mmlab.be/ns/fnml#>

    SELECT DISTINCT ?term_map ?function_map_value ?parameter_map_value ?value_map_type ?value_map_value
    WHERE {
    
    # FuntionMap ----------------------------------------------------------------------
        
        ?term_map fnml:functionMap ?function_map .        
        ?function_map ?function_map_type ?function_map_value .
        FILTER ( ?function_map_type IN ( rr:constant, rr:template, rml:reference ) ) .
        OPTIONAL {
            # return maps are not used in the current implementation
            ?term_map fnml:returnMap ?return_map .
            ?return_map ?return_map_type ?return_map_value .
            FILTER ( ?return_map_type IN ( rr:constant, rr:template, rml:reference ) ) .
        }

    # Input ---------------------------------------------------------------------------

        ?term_map fnml:input ?input .

        ?input fnml:parameterMap ?parameter_map .
        ?parameter_map ?parameter_map_type ?parameter_map_value .
        FILTER ( ?parameter_map_type IN ( rr:constant, rr:template, rml:reference ) ) .

        ?input fnml:valueMap ?value_map .
        ?value_map ?value_map_type ?value_map_value .
        FILTER ( ?value_map_type IN ( rr:constant, rr:template, rml:reference, fnml:execution ) ) .
    }
"""