import rdflib, logging
import pandas as pd

mappings_dataframe_columns = [
    'triples_map_id', 'data_source', 'ref_form', 'iterator', 'tablename', 'query', 'jdbcDSN', 'jdbcDriver', 'user',
    'password', 'subject_template', 'subject_reference', 'subject_constant', 'subject_constant_shortcut',
    'subject_rdf_class', 'subject_termtype', 'subject_graph', 'predicate_constant', 'predicate_template',
    'predicate_reference', 'predicate_constant_shortcut', 'object_constant', 'object_template', 'object_reference',
    'object_termtype', 'object_datatype', 'object_language', 'object_parent_triples_map', 'join_condition',
    'child_value', 'parent_value', 'object_constant_shortcut', 'predicate_object_graph'
]

"""This query has been reused from SDM-RDFizer (https://github.com/SDM-TIB/SDM-RDFizer). SDM-RDFizer has been developed
by members of the Scientific Data Management Group at TIB. Its development has been coordinated and supervised by 
Maria-Esther Vidal. The implementation has been done by Enrique Iglesias and Guillermo Betancourt under the 
supervision of David Chaves-Fraga, Samaneh Jozashoori, and Kemele Endris.
It has been partially modified by the Ontology Engineering Group (OEG) from Universidad Politécnica de Madrid (UPM)."""
MAPPING_PARSING_QUERY = """
    prefix rr: <http://www.w3.org/ns/r2rml#> 
    prefix rml: <http://semweb.mmlab.be/ns/rml#> 
    prefix ql: <http://semweb.mmlab.be/ns/ql#> 
    prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> 

    SELECT DISTINCT 
        ?triples_map_id ?data_source ?ref_form ?iterator ?tablename ?query
        ?jdbcDSN ?jdbcDriver ?user ?password
        ?subject_template ?subject_reference ?subject_constant ?subject_constant_shortcut
        ?subject_rdf_class ?subject_termtype ?subject_graph
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
        OPTIONAL {
            ?triples_map_id rr:subjectMap ?_subject_map .
            OPTIONAL { ?_subject_map rr:template ?subject_template . }
            OPTIONAL { ?_subject_map rml:reference ?subject_reference . }
            OPTIONAL { ?_subject_map rr:constant ?subject_constant . }
            OPTIONAL { ?_subject_map rr:class ?subject_rdf_class . }
            OPTIONAL { ?_subject_map rr:termType ?subject_termtype . }
            OPTIONAL { ?_subject_map rr:graph ?subject_graph . }
            OPTIONAL {
            ?_subject_map rr:graphMap ?_graph_structure .
            ?_graph_structure rr:constant ?graph .
            }
            OPTIONAL {
                ?_subject_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:template ?graph .
            }
        }
        
        OPTIONAL { ?triples_map_id rr:subject ?subject_constant_shortcut . }

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
    mappings_df.at[i, 'subject_constant_shortcut'] = mapping_rule.subject_constant_shortcut
    mappings_df.at[i, 'subject_rdf_class'] = mapping_rule.subject_rdf_class
    mappings_df.at[i, 'subject_termtype'] = mapping_rule.subject_termtype
    mappings_df.at[i, 'subject_graph'] = mapping_rule.subject_graph
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


def _remove_duplicated_mapping_rules(mappings_df):
    '''ensure there are no duplicate rules'''

    num_mapping_rules = len(mappings_df)
    mappings_df.drop_duplicates(inplace=True)
    if len(mappings_df) < num_mapping_rules:
        logging.warning('Duplicated mapping rules were found. Ignoring duplicated mapping rules.')

    return mappings_df


def _validate_mapping_partitions(mappings_df, mapping_partitions, source_name):
    if 's' in mapping_partitions:
        '''
        Subject is used as partitioning criteria. 
        If there is any subject that is a reference that means it is not a template nor a constant, and it cannot
        be used as partitioning criteria.        
        '''
        if mappings_df['subject_reference'].notna().any():
            raise Exception('Invalid mapping partitions criteria ' + mapping_partitions + ': mappings cannot be '
                            'partitioned by subject because mappings of source ' + source_name +
                            ' contain subject terms that are rr:column or rml:reference.')
    if 'p' in mapping_partitions:
        '''
        Predicate is used as partitioning criteria. 
        If there is any predicate that is a reference that means it is not a template nor a constant, and it cannot
        be used as partitioning criteria.
        '''
        if mappings_df['subject_reference'].notna().any():
            raise Exception('Invalid mapping partitions criteria ' + mapping_partitions +
                            ': mappings cannot be partitioned by predicate because mappings of source ' + source_name +
                            ' contain predicate terms that are rr:column or rml:reference.')


def _get_invariable_part_of_template(template):
    zero_width_space = '\u200B'
    template_for_splitting = template.replace('\{', zero_width_space)
    if '{' in template_for_splitting:
        invariable_part_of_template = template_for_splitting.split('{')[0]
        invariable_part_of_template = invariable_part_of_template.replace(zero_width_space, '\{')
    else:
        raise Exception('Invalid string template ' + template + '. No pairs of unescaped curly braces were found.')

    return invariable_part_of_template


def _get_mapping_partitions_invariable_parts(mappings_df, mapping_partitions):
    mappings_df['subject_invariable_part'] = ''
    mappings_df['predicate_invariable_part'] = ''

    for i, mapping_rule in mappings_df.iterrows():
        if 's' in mapping_partitions:
            if mapping_rule['subject_template']:
                mappings_df.at[i, 'subject_invariable_part'] = \
                    _get_invariable_part_of_template(mapping_rule['subject_template'])
            elif mapping_rule['subject_constant']:
                mappings_df.at[i, 'subject_invariable_part'] = mapping_rule['subject_constant']
            elif mapping_rule['subject_constant_shortcut']:
                mappings_df.at[i, 'subject_invariable_part'] = mapping_rule['subject_constant_shortcut']
            else:
                raise Exception('An invalid subject term was found at triples map ' + mapping_rule['triples_map_id'] +
                                '. Subjects terms must be constants or templates in order to generate valid mapping '
                                'partitions by subject.')
        if 'p' in mapping_partitions:
            if mapping_rule['predicate_constant']:
                mappings_df.at[i, 'predicate_invariable_part'] = mapping_rule['predicate_constant']
            elif mapping_rule['predicate_constant_shortcut']:
                mappings_df.at[i, 'predicate_invariable_part'] = mapping_rule['predicate_constant_shortcut']
            elif mapping_rule['predicate_template']:
                mappings_df.at[i, 'predicate_invariable_part'] = \
                    _get_invariable_part_of_template(mapping_rule['predicate_template'])
            else:
                raise Exception('An invalid predicate term was found at triples map ' + mapping_rule['triples_map_id'] +
                                '. Predicate terms must be constants or templates in order to generate valid mapping '
                                'partitions by predicate.')

    return mappings_df


def _generate_mapping_partitions(mappings_df, mapping_partitions):
    mappings_df = _get_mapping_partitions_invariable_parts(mappings_df, mapping_partitions)
    mappings_df['subject_partition'] = ''
    mappings_df['predicate_partition'] = ''

    '''
        First generate partitions for subject. Then generate the partitions for predicates. Finally merge both to
        get the final partitions.
    '''

    if 's' in mapping_partitions:
        mappings_df.sort_values(by='subject_invariable_part', inplace=True, ascending=True)
        num_partition = 0
        root_last_partition = 'zzyy xxww\u200B'
        for i, mapping_rule in mappings_df.iterrows():
            if mapping_rule['subject_invariable_part'].startswith(root_last_partition):
                mappings_df.at[i, 'subject_partition'] = str(num_partition)
            else:
                num_partition = num_partition + 1
                root_last_partition = mapping_rule['subject_invariable_part']
                mappings_df.at[i, 'subject_partition'] = str(num_partition)
    if 'p' in mapping_partitions:
        mappings_df.sort_values(by='predicate_invariable_part', inplace=True, ascending=True)
        num_partition = 0
        root_last_partition = 'zzyy xxww\u200B'
        for i, mapping_rule in mappings_df.iterrows():
            if mapping_rule['predicate_invariable_part'].startswith(root_last_partition):
                mappings_df.at[i, 'predicate_partition'] = str(num_partition)
            else:
                num_partition = num_partition + 1
                root_last_partition = mapping_rule['predicate_invariable_part']
                mappings_df.at[i, 'predicate_partition'] = str(num_partition)

    ''' if subject and predicate are partitioning criteria separate then with - '''
    if 's' in mapping_partitions and 'p' in mapping_partitions:
        mappings_df['mapping_partition'] = mappings_df['subject_partition'] + '-' + mappings_df['predicate_partition']
    else:
        mappings_df['mapping_partition'] = mappings_df['subject_partition'] + mappings_df['predicate_partition']

    '''' drop the auxiliary columns that are just used to get mapping_partition '''
    mappings_df.drop([
        'subject_partition',
        'predicate_partition',
        'subject_invariable_part',
        'predicate_invariable_part'],
        axis=1, inplace=True)

    logging.info(str(len(set(mappings_df['mapping_partition']))) + ' different mapping partitions were generated.')

    return mappings_df


def parse_mappings(data_sources, configuration):
    mappings_df = pd.DataFrame(columns=mappings_dataframe_columns)

    for source_name, source_options in data_sources.items():
        source_mappings_df = _parse_mapping_file(source_options['mapping_file'])
        '''TO DO: validate mapping rules'''
        _validate_mapping_partitions(source_mappings_df, configuration['mapping_partitions'], source_name)
        logging.info('Mappings for data source ' + str(source_name) + ' successfully parsed.')
        mappings_df = pd.concat([mappings_df, source_mappings_df])

    mappings_df = _remove_duplicated_mapping_rules(mappings_df)
    mappings_df = _generate_mapping_partitions(mappings_df, configuration['mapping_partitions'])

    return mappings_df
