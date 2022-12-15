__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


##############################################################################
#######################   MAPPING DATAFRAME COLUMNS   ########################
##############################################################################

MAPPINGS_DATAFRAME_COLUMNS = [
    'source_name', 'triples_map_id', 'triples_map_type', 'logical_source_type', 'logical_source_value', 'iterator',
    'subject_map_type', 'subject_map_value', 'subject_map', 'subject_termtype',
    'predicate_map_type', 'predicate_map_value',
    'object_map_type', 'object_map_value', 'object_map', 'object_termtype', 'object_datatype', 'object_language',
    'graph_map_type', 'graph_map_value',
    'subject_join_conditions', 'object_join_conditions'
]


##############################################################################
#######################   FUNCTION DATAFRAME COLUMNS   ########################
##############################################################################

FUNCTIONS_DATAFRAME_COLUMNS = [
    'execution', 'parameter_map_type ', 'parameter_map_value', 'parameter_name', 'parameter_type'
]

##############################################################################
########################   MAPPING PARSING QUERIES   #########################
##############################################################################

MAPPING_PARSING_QUERY = """
    prefix rr: <http://www.w3.org/ns/r2rml#>
    prefix rml: <http://semweb.mmlab.be/ns/rml#>

    SELECT DISTINCT
        ?triples_map_id ?triples_map_type ?logical_source_type ?logical_source_value ?iterator
        ?subject_map_type ?subject_map_value ?subject_map ?subject_termtype
        ?predicate_map_type ?predicate_map_value
        ?object_map_type ?object_map_value ?object_map ?object_termtype ?object_datatype ?object_language
        ?graph_map_type ?graph_map_value
        
    WHERE {
        ?triples_map_id rml:logicalSource ?_source .
        ?triples_map_id a ?triples_map_type .
        ?_source ?logical_source_type ?logical_source_value .
        FILTER ( ?logical_source_type IN ( rml:source, rr:tableName, rml:query ) ) .
        OPTIONAL { ?_source rml:iterator ?iterator . }

    # Subject -------------------------------------------------------------------------
        ?triples_map_id rml:subjectMap ?subject_map .
        {
        ?subject_map ?subject_map_type ?subject_map_value .
        FILTER ( ?subject_map_type IN ( rr:constant, rr:template, rml:reference, rml:quotedTriplesMap ) ) .
        } UNION {
                ?subject_map fnml:return ?subject_output .
                ?subject_map fnml:execution ?subject_map_value.
        }
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
                FILTER ( ?object_map_type IN ( rr:constant, rr:template, rml:reference, rr:parentTriplesMap, rml:quotedTriplesMap ) ) .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            } 
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?graph_map .
                ?graph_map ?graph_map_type ?graph_map_value .
                FILTER ( ?graph_map_type IN ( rr:constant, rr:template, rml:reference ) ) .
            }
            OPTIONAL {
                ?_predicate_object_map rml:objectMap ?object_map .
                ?object_map fnml:return  ?object_output .
                ?object_map fnml:execution  ?object_map_value .
                ?object_map ?object_map_type ?object_map_value .
            }
        }
    }
"""


JOIN_CONDITION_PARSING_QUERY = """
    prefix rr: <http://www.w3.org/ns/r2rml#>

    SELECT DISTINCT ?term_map ?join_condition ?child_value ?parent_value
    WHERE {
        ?term_map rr:joinCondition ?join_condition .
        ?join_condition rr:child ?child_value; rr:parent ?parent_value .
    }
"""

##############################################################################
########################   FUNCTION PARSING QUERIES   #########################
##############################################################################

# FUNCTION_PARSING_QUERY = """
# SELECT * WHERE {?x ?y ?z}
# """

FUNCTION_PARSING_QUERY = """
    prefix rr: <http://www.w3.org/ns/r2rml#> 
    prefix rml: <http://semweb.mmlab.be/ns/rml#> 
    prefix fno: <http://w3id.org/function/ontology#> 
    prefix fnml: <http://semweb.mmlab.be/ns/fnml#> 

    SELECT *
    #DISTINCT ?func ?exec ?parameter_map_type ?parameter_map_value ?parameter_uri ?parameter_type
    
      WHERE {  
            ?exec fnml:function ?func. 
      
        # output --------------------------------------------------------
        
        ?_predicate_object_map rr:objectMap ?object_map .
        ?func fno:returns ?output_list.
        ?output_list rdf:first ?parameter_uri.
        BIND(fno:Output AS ?parameter_map_type).        
    }
"""