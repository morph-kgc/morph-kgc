import rdflib, logging
import pandas as pd


mappings_dataframe_columns = [
        'triples_map_id', 'data_source', 'ref_form', 'iterator', 'tablename', 'query', 'jdbcDSN', 'jdbcDriver', 'user',
        'password', 'subject_template', 'subject_reference', 'subject_constant', 'subject_rdf_class',
        'subject_termtype', 'graph', 'predicate_constant', 'predicate_template', 'predicate_reference',
        'predicate_constant_shortcut', 'object_constant', 'object_template', 'object_reference', 'object_termtype',
        'object_datatype', 'object_language', 'object_parent_triples_map', 'join_condition', 'child_value',
        'parent_value', 'object_constant_shortcut', 'predicate_object_graph'
    ]


MAPPING_PARSING_QUERY = """
    prefix rr: <http://www.w3.org/ns/r2rml#> 
    prefix rml: <http://semweb.mmlab.be/ns/rml#> 
    prefix ql: <http://semweb.mmlab.be/ns/ql#> 
    prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> 

    SELECT DISTINCT 
        ?triples_map_id ?data_source ?ref_form ?iterator ?tablename ?query
        ?jdbcDSN ?jdbcDriver ?user ?password
        ?subject_template ?subject_reference ?subject_constant ?subject_rdf_class ?subject_termtype ?graph
        ?predicate_constant ?predicate_template ?predicate_reference ?predicate_constant_shortcut
        ?object_constant ?object_template ?object_reference ?object_termtype ?object_datatype ?object_language
        ?object_parent_triples_map ?join_condition ?child_value ?parent_value ?object_constant_shortcut
        ?predicate_object_graph

    WHERE {
        ?triples_map_id rml:logicalSource ?_source .
        OPTIONAL { ?_source rml:source ?data_source . }
        OPTIONAL { ?_source rml:referenceFormulation ?ref_form . }
        OPTIONAL { ?_source rml:iterator ?iterator . }
        OPTIONAL { ?_source rr:tableName ?tablename . }
        OPTIONAL { ?_source rml:query ?query . }
        OPTIONAL {
            ?_source a d2rq:Database ;
            d2rq:jdbcDSN ?jdbcDSN ;
            d2rq:jdbcDriver ?jdbcDriver ;
            d2rq:username ?user ;
            d2rq:password ?password .
        }

# Subject -------------------------------------------------------------------------
        ?triples_map_id rr:subjectMap ?_subject_map .
        OPTIONAL { ?_subject_map rr:template ?subject_template . }
        OPTIONAL { ?_subject_map rml:reference ?subject_reference . }
        OPTIONAL { ?_subject_map rr:constant ?subject_constant . }
        OPTIONAL { ?_subject_map rr:class ?subject_rdf_class . }
        OPTIONAL { ?_subject_map rr:termType ?subject_termtype . }
        OPTIONAL { ?_subject_map rr:graph ?graph . }
        OPTIONAL {
            ?_subject_map rr:graphMap ?_graph_structure .
            ?_graph_structure rr:constant ?graph .
        }
        OPTIONAL {
            ?_subject_map rr:graphMap ?_graph_structure .
            ?_graph_structure rr:template ?graph .
        }

# Predicate -----------------------------------------------------------------------
        OPTIONAL {
            ?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
            OPTIONAL {
                ?_predicate_object_map rr:predicateMap ?_predicate_map .
                ?_predicate_map rr:constant ?predicate_constant .
            }
            OPTIONAL {
                ?_predicate_object_map rr:predicateMap ?_predicate_map .
                ?_predicate_map rr:template ?predicate_template .
            }
            OPTIONAL {
                ?_predicate_object_map rr:predicateMap ?_predicate_map .
                ?_predicate_map rml:reference ?predicate_reference .
            }
            OPTIONAL { ?_predicate_object_map rr:predicate ?predicate_constant_shortcut . }

# Object --------------------------------------------------------------------------
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?_object_map .
                ?_object_map rr:constant ?object_constant .
                OPTIONAL { ?_object_map rr:termType ?object_termtype . }
                OPTIONAL { ?_object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?_object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?_object_map .
                ?_object_map rr:template ?object_template .
                OPTIONAL { ?_object_map rr:termType ?object_termtype . }
                OPTIONAL { ?_object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?_object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?_object_map .
                ?_object_map rml:reference ?object_reference .
                OPTIONAL { ?_object_map rr:termType ?object_termtype . }
                OPTIONAL { ?_object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?_object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?_object_map .
                ?_object_map rr:parentTriplesMap ?object_parent_triples_map .
                OPTIONAL {
                    ?_object_map rr:joinCondition ?join_condition .
                    ?join_condition rr:child ?child_value;
                                 rr:parent ?parent_value.
                    OPTIONAL { ?_object_map rr:termType ?object_termtype . }
                }
            }
            OPTIONAL {
                ?_predicate_object_map rr:object ?object_constant_shortcut .
                OPTIONAL { ?_object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?_object_map rr:language ?object_language . }
            }
            OPTIONAL { ?_predicate_object_map rr:graph ?predicate_object_graph . }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:constant ?predicate_object_graph .
            }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:template ?predicate_object_graph .
            }
        }
    }
"""


def _parse_mapping_file(mapping_file):
    mapping_graph = rdflib.Graph()

    try:
        '''
            TO DO: guess mapping file format
        '''
        mapping_graph.load(mapping_file, format='n3')
    except Exception as n3_mapping_parse_exception:
        raise Exception(n3_mapping_parse_exception)

    mapping_query_results = mapping_graph.query(MAPPING_PARSING_QUERY)
    mappings_df = _transform_mappings_into_dataframe(mapping_query_results)

    return mappings_df


def _transform_mappings_into_dataframe(mapping_query_results):
    '''
    Transforms the result from a SPARQL query in rdflib to a DataFrame.

    :param mapping_query_results:
    :return:
    '''

    source_mappings_df = pd.DataFrame(columns=mappings_dataframe_columns)

    for mapping_rule in mapping_query_results:
        _append_mapping_rule(source_mappings_df, mapping_rule)

    return source_mappings_df


def _append_mapping_rule(mappings_df, mapping_rule):
    i = len(mappings_df)

    mappings_df.at[i, 'triples_map_id'] = mapping_rule.triples_map_id
    mappings_df.at[i, 'data_source'] = mapping_rule.data_source
    mappings_df.at[i, 'ref_form'] = mapping_rule.ref_form
    mappings_df.at[i, 'iterator'] = mapping_rule.iterator
    mappings_df.at[i, 'tablename'] = mapping_rule.tablename
    mappings_df.at[i, 'query'] = mapping_rule.query
    mappings_df.at[i, 'jdbcDSN'] = mapping_rule.jdbcDSN
    mappings_df.at[i, 'jdbcDriver'] = mapping_rule.jdbcDriver
    mappings_df.at[i, 'user'] = mapping_rule.user
    mappings_df.at[i, 'password'] = mapping_rule.password
    mappings_df.at[i, 'subject_template'] = mapping_rule.subject_template
    mappings_df.at[i, 'subject_reference'] = mapping_rule.subject_reference
    mappings_df.at[i, 'subject_constant'] = mapping_rule.subject_constant
    mappings_df.at[i, 'subject_rdf_class'] = mapping_rule.subject_rdf_class
    mappings_df.at[i, 'subject_termtype'] = mapping_rule.subject_termtype
    mappings_df.at[i, 'graph'] = mapping_rule.graph
    mappings_df.at[i, 'predicate_constant'] = mapping_rule.predicate_constant
    mappings_df.at[i, 'predicate_template'] = mapping_rule.predicate_template
    mappings_df.at[i, 'predicate_reference'] = mapping_rule.predicate_reference
    mappings_df.at[i, 'predicate_constant_shortcut'] = mapping_rule.predicate_constant_shortcut
    mappings_df.at[i, 'object_constant'] = mapping_rule.object_constant
    mappings_df.at[i, 'object_template'] = mapping_rule.object_template
    mappings_df.at[i, 'object_reference'] = mapping_rule.object_reference
    mappings_df.at[i, 'object_termtype'] = mapping_rule.object_termtype
    mappings_df.at[i, 'object_datatype'] = mapping_rule.object_datatype
    mappings_df.at[i, 'object_language'] = mapping_rule.object_language
    mappings_df.at[i, 'object_parent_triples_map'] = mapping_rule.object_parent_triples_map
    mappings_df.at[i, 'join_condition'] = mapping_rule.join_condition
    mappings_df.at[i, 'child_value'] = mapping_rule.child_value
    mappings_df.at[i, 'parent_value'] = mapping_rule.parent_value
    mappings_df.at[i, 'object_constant_shortcut'] = mapping_rule.object_constant_shortcut
    mappings_df.at[i, 'predicate_object_graph'] = mapping_rule.predicate_object_graph


def parse_mappings(data_sources, configuration):
    mappings_df = pd.DataFrame(columns=mappings_dataframe_columns)

    for source_name, source_options in data_sources.items():
        source_mappings_df = _parse_mapping_file(source_options['mapping_file'])
        mappings_df = pd.concat([mappings_df, source_mappings_df])

    '''ensure there are no duplicate rules'''
    num_mapping_rules = len(mappings_df)
    mappings_df.drop_duplicates(inplace=True)
    if len(mappings_df) < num_mapping_rules:
        logging.warning('Duplicated mapping rules were found.')
