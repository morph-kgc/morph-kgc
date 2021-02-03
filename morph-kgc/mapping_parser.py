""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import rdflib
import logging
import sys
import time
import sql_metadata
import rfc3987
import pandas as pd

from data_sources import relational_source
from args_parser import VALID_ARGUMENTS


RELATIONAL_SOURCE_TYPES = VALID_ARGUMENTS['relational_source_type']


MAPPINGS_DATAFRAME_COLUMNS = [
    'source_name',
    'triples_map_id', 'data_source', 'object_map', 'ref_form', 'iterator', 'tablename', 'query',
    'subject_template', 'subject_reference', 'subject_constant', 'subject_rdf_class', 'subject_termtype',
    'graph_constant', 'graph_reference', 'graph_template',
    'predicate_constant', 'predicate_template', 'predicate_reference',
    'object_constant', 'object_template', 'object_reference', 'object_termtype', 'object_datatype', 'object_language',
    'object_parent_triples_map', 'join_conditions',
    'predicate_object_graph_constant', 'predicate_object_graph_reference', 'predicate_object_graph_template'
]


"""This query has been reused from SDM-RDFizer (https://github.com/SDM-TIB/SDM-RDFizer). SDM-RDFizer has been developed
by members of the Scientific Data Management Group at TIB. Its development has been coordinated and supervised by
Maria-Esther Vidal. The implementation has been done by Enrique Iglesias and Guillermo Betancourt under the
supervision of David Chaves-Fraga, Samaneh Jozashoori, and Kemele Endris.
It has been partially modified by the Ontology Engineering Group (OEG) from Universidad Politécnica de Madrid (UPM)."""
RML_MAPPING_PARSING_QUERY = """
    prefix rr: <http://www.w3.org/ns/r2rml#>
    prefix rml: <http://semweb.mmlab.be/ns/rml#>

    SELECT DISTINCT
        ?triples_map_id ?data_source ?ref_form ?iterator ?tablename ?query ?object_map
        ?subject_template ?subject_reference ?subject_constant
        ?subject_rdf_class ?subject_termtype
        ?graph_constant ?graph_reference ?graph_template
        ?predicate_constant ?predicate_template ?predicate_reference
        ?object_constant ?object_template ?object_reference ?object_termtype ?object_datatype ?object_language
        ?object_parent_triples_map
        ?predicate_object_graph_constant ?predicate_object_graph_reference ?predicate_object_graph_template

    WHERE {
        ?triples_map_id rml:logicalSource ?_source .
        OPTIONAL { ?_source rml:source ?data_source . }
        OPTIONAL { ?_source rml:referenceFormulation ?ref_form . }
        OPTIONAL { ?_source rml:iterator ?iterator . }
        OPTIONAL { ?_source rr:tableName ?tablename . }
        OPTIONAL { ?_source rml:query ?query . }

# Subject -------------------------------------------------------------------------
        OPTIONAL {
            ?triples_map_id rr:subjectMap ?_subject_map .
            OPTIONAL { ?_subject_map rr:template ?subject_template . }
            OPTIONAL { ?_subject_map rml:reference ?subject_reference . }
            OPTIONAL { ?_subject_map rr:constant ?subject_constant . }
            OPTIONAL { ?_subject_map rr:class ?subject_rdf_class . }
            OPTIONAL { ?_subject_map rr:termType ?subject_termtype . }
            OPTIONAL { ?_subject_map rr:graph ?graph_constant . }
            OPTIONAL {
                ?_subject_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:constant ?graph_constant .
            }
            OPTIONAL {
                ?_subject_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:template ?graph_template .
            }
            OPTIONAL {
                ?_subject_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:reference ?graph_reference .
            }
        }
        OPTIONAL { ?triples_map_id rr:subject ?subject_constant . }

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
            OPTIONAL { ?_predicate_object_map rr:predicate ?predicate_constant . }

# Object --------------------------------------------------------------------------
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?object_map .
                ?object_map rr:constant ?object_constant .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?object_map .
                ?object_map rr:template ?object_template .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?object_map .
                ?object_map rml:reference ?object_reference .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?object_map .
                ?object_map rr:parentTriplesMap ?object_parent_triples_map .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:object ?object_constant .
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            }
            OPTIONAL { ?_predicate_object_map rr:graph ?predicate_object_graph_constant . }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:constant ?predicate_object_graph_constant .
            }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:template ?predicate_object_graph_template .
            }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:reference ?predicate_object_graph_reference .
            }
        }
    }
"""


R2RML_MAPPING_PARSING_QUERY = """
    prefix rr: <http://www.w3.org/ns/r2rml#>

# RML compliance: ?data_source ?ref_form ?iterator --------------------------------
    SELECT DISTINCT
        ?triples_map_id ?data_source ?ref_form ?iterator ?tablename ?query ?object_map
        ?subject_template ?subject_reference ?subject_constant
        ?subject_rdf_class ?subject_termtype
        ?graph_constant ?graph_reference ?graph_template
        ?predicate_constant ?predicate_template ?predicate_reference
        ?object_constant ?object_template ?object_reference ?object_termtype ?object_datatype ?object_language
        ?object_parent_triples_map
        ?predicate_object_graph_constant ?predicate_object_graph_reference ?predicate_object_graph_template

    WHERE {
        ?triples_map_id rr:logicalTable ?_source .
        OPTIONAL { ?_source rr:tableName ?tablename . }
        OPTIONAL { ?_source rr:sqlQuery ?query . }

# Subject -------------------------------------------------------------------------
        OPTIONAL {
            ?triples_map_id rr:subjectMap ?_subject_map .
            OPTIONAL { ?_subject_map rr:template ?subject_template . }
            OPTIONAL { ?_subject_map rr:column ?subject_reference . }
            OPTIONAL { ?_subject_map rr:constant ?subject_constant . }
            OPTIONAL { ?_subject_map rr:class ?subject_rdf_class . }
            OPTIONAL { ?_subject_map rr:termType ?subject_termtype . }
            OPTIONAL { ?_subject_map rr:graph ?graph_constant . }
            OPTIONAL {
                ?_subject_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:constant ?graph_constant .
            }
            OPTIONAL {
                ?_subject_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:template ?graph_template .
            }
            OPTIONAL {
                ?_subject_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:column ?graph_reference .
            }
        }
        OPTIONAL { ?triples_map_id rr:subject ?subject_constant . }

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
                ?_predicate_map rr:column ?predicate_reference .
            }
            OPTIONAL { ?_predicate_object_map rr:predicate ?predicate_constant . }

# Object --------------------------------------------------------------------------
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?object_map .
                ?object_map rr:constant ?object_constant .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?object_map .
                ?object_map rr:template ?object_template .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?object_map .
                ?object_map rr:column ?object_reference .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?object_map .
                ?object_map rr:parentTriplesMap ?object_parent_triples_map .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:object ?object_constant .
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            }
            OPTIONAL { ?_predicate_object_map rr:graph ?predicate_object_graph_constant . }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:constant ?predicate_object_graph_constant .
            }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:template ?predicate_object_graph_template .
            }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:column ?predicate_object_graph_reference .
            }
        }
    }
"""


JOIN_CONDITION_PARSING_QUERY = """
    prefix rr: <http://www.w3.org/ns/r2rml#>

    SELECT DISTINCT ?object_map ?join_condition ?child_value ?parent_value
    WHERE {
        ?object_map rr:joinCondition ?join_condition .
        ?join_condition rr:child ?child_value;
                        rr:parent ?parent_value.
    }
"""


def _infer_mapping_language_from_graph(mapping_graph, source_name):
    """
    Recognizes the mapping language of the rules of a source in a mapping graph. Valid mapping languages to recognize
    are R2RML and RML.

    :param mapping_graph: rdflib Graph with the mapping rules
    :type mapping_graph: Graph
    :param source_name: name of the source to which the mapping rules are associated
    :type source_name: str
    :return mapping language of the rules in the graph (R2RML or RML)
    :rtype str
    """

    rml_inferred = False

    # Check if mapping language is RML
    rml_query = '''
        prefix rml: <http://semweb.mmlab.be/ns/rml#>
        SELECT ?s WHERE { ?s rml:logicalSource ?o . } LIMIT 1
    '''
    mapping_language_results = mapping_graph.query(rml_query)
    if len(mapping_language_results) > 0:
        logging.info('RML mapping language inferred for data source ' + source_name + '.')
        rml_inferred = True

    # Check if mapping language is R2RML
    r2rml_query = '''
        prefix rr: <http://www.w3.org/ns/r2rml#>
        SELECT ?s WHERE { ?s rr:logicalTable ?o . } LIMIT 1
    '''
    mapping_language_results = mapping_graph.query(r2rml_query)
    if len(mapping_language_results) > 0:
        logging.info('R2RML mapping language inferred for data source ' + source_name + '.')
        if rml_inferred:
            raise Exception('Both, RML and R2RML were inferred for the mappings for data source ' + source_name + '.')
        else:
            return 'R2RML'

    if rml_inferred:
        return 'RML'

    # If mappings file does not have rml:logicalSource or rr:logicalTable it is not valid
    raise Exception('It was not possible to infer the mapping language for data source ' + source_name +
                    '. Check the corresponding mapping files.')


def _get_join_object_maps_join_conditions(join_query_results):
    """
    Creates a dictionary with the results of the JOIN_CONDITION_PARSING_QUERY. The values of the dictionary are in turn
    other dictionaries with two items with keys, child_value and parent_value, representing a join condition.

    :param join_query_results: result of JOIN_CONDITION_PARSING_QUERY
    :type join_query_results: SPARQLResult
    :return dictionary with join conditions
    :rtype dict
    """

    join_conditions_dict = {}

    for join_condition in join_query_results:
        if join_condition.object_map not in join_conditions_dict:
            join_conditions_dict[join_condition.object_map] = {}

        join_conditions_dict[join_condition.object_map][str(join_condition.join_condition)] = \
            {'child_value': str(join_condition.child_value), 'parent_value': str(join_condition.parent_value)}

    return join_conditions_dict


def _append_mapping_rule(mappings_df, mapping_rule):
    """
    Builds a Pandas DataFrame from the results obtained from [R2]RML_MAPPING_PARSING_QUERY and
    JOIN_CONDITION_PARSING_QUERY for one source.

    :param mappings_df: DataFrame with populated with some mapping rules for a data source
    :type mappings_df: DataFrame
    :param mapping_rule: one result of [R2]RML_MAPPING_PARSING_QUERY result set
    :type mapping_rule: ResultRow
    :return mappings_df additionally populated with mapping_rule
    :rtype DataFrame
    """

    # Get position for the new mapping rule in the DataFrame
    i = len(mappings_df)

    mappings_df.at[i, 'triples_map_id'] = mapping_rule.triples_map_id
    mappings_df.at[i, 'data_source'] = mapping_rule.data_source
    mappings_df.at[i, 'object_map'] = mapping_rule.object_map
    mappings_df.at[i, 'ref_form'] = mapping_rule.ref_form
    mappings_df.at[i, 'iterator'] = mapping_rule.iterator
    mappings_df.at[i, 'tablename'] = mapping_rule.tablename
    mappings_df.at[i, 'query'] = mapping_rule.query
    mappings_df.at[i, 'subject_template'] = mapping_rule.subject_template
    mappings_df.at[i, 'subject_reference'] = mapping_rule.subject_reference
    mappings_df.at[i, 'subject_constant'] = mapping_rule.subject_constant
    mappings_df.at[i, 'subject_rdf_class'] = mapping_rule.subject_rdf_class
    mappings_df.at[i, 'subject_termtype'] = mapping_rule.subject_termtype
    mappings_df.at[i, 'graph_constant'] = mapping_rule.graph_constant
    mappings_df.at[i, 'graph_template'] = mapping_rule.graph_template
    mappings_df.at[i, 'graph_reference'] = mapping_rule.graph_reference
    mappings_df.at[i, 'predicate_constant'] = mapping_rule.predicate_constant
    mappings_df.at[i, 'predicate_template'] = mapping_rule.predicate_template
    mappings_df.at[i, 'predicate_reference'] = mapping_rule.predicate_reference
    mappings_df.at[i, 'object_constant'] = mapping_rule.object_constant
    mappings_df.at[i, 'object_template'] = mapping_rule.object_template
    mappings_df.at[i, 'object_reference'] = mapping_rule.object_reference
    mappings_df.at[i, 'object_termtype'] = mapping_rule.object_termtype
    mappings_df.at[i, 'object_datatype'] = mapping_rule.object_datatype
    mappings_df.at[i, 'object_language'] = mapping_rule.object_language
    mappings_df.at[i, 'object_parent_triples_map'] = mapping_rule.object_parent_triples_map
    mappings_df.at[i, 'predicate_object_graph_constant'] = mapping_rule.predicate_object_graph_constant
    mappings_df.at[i, 'predicate_object_graph_reference'] = mapping_rule.predicate_object_graph_reference
    mappings_df.at[i, 'predicate_object_graph_template'] = mapping_rule.predicate_object_graph_template

    return mappings_df


def _transform_mappings_into_dataframe(mapping_query_results, join_query_results, source_name):
    """
    Builds a Pandas DataFrame from the results obtained from [R2]RML_MAPPING_PARSING_QUERY and
    JOIN_CONDITION_PARSING_QUERY for one source.

    :param mapping_query_results: SPARQL result set of [R2]RML_MAPPING_PARSING_QUERY for source_name
    :type mapping_query_results: SPARQLResult
    :param join_query_results: SPARQL result set of JOIN_CONDITION_PARSING_QUERY for source_name
    :type join_query_results: SPARQLResult
    :param source_name: name of the source to which the mapping rules are associated
    :type source_name: str
    :return DataFrame with mapping rules for source_name
    :rtype DataFrame
    """

    source_mappings_df = pd.DataFrame(columns=MAPPINGS_DATAFRAME_COLUMNS)
    for mapping_rule in mapping_query_results:
        # append each mapping rule to the newly created DataFrame
        source_mappings_df = _append_mapping_rule(source_mappings_df, mapping_rule)

    # process mapping rules with joins
    join_conditions_dict = _get_join_object_maps_join_conditions(join_query_results)
    source_mappings_df['join_conditions'] = source_mappings_df['object_map'].map(join_conditions_dict)
    source_mappings_df['join_conditions'] = source_mappings_df['join_conditions'].where(
        pd.notnull(source_mappings_df['join_conditions']), '')          # needed for later hashing the dataframe
    source_mappings_df['join_conditions'] = source_mappings_df['join_conditions'].astype(str)
    source_mappings_df = source_mappings_df.drop('object_map', axis=1)         # object_map column no longer needed

    # associate the source name to the mapping rules
    source_mappings_df['source_name'] = source_name

    return source_mappings_df


def _parse_mapping_files(config, data_source_name):
    """
    Creates a Pandas DataFrame with the mapping rules for a data source. It loads the mapping files in a rdflib graph
    and recognizes the mapping language used. It performs queries [R2]RML_MAPPING_PARSING_QUERY and
    JOIN_CONDITION_PARSING_QUERY and process the results to build a DataFrame with the mapping rules.

    :param config: ConfigParser object
    :type config: ConfigParser
    :param data_source_name: name of the source to which the mapping rules are associated
    :type data_source_name: str
    :return DataFrame with mapping rules for data_source_name
    :rtype DataFrame
    """

    mapping_graph = rdflib.Graph()
    try:
        for mapping_file in config.get(data_source_name, 'mapping_files').split(','):
            mapping_graph.load(mapping_file.strip(), format='n3')
    except Exception as n3_mapping_parse_exception:
        raise Exception(n3_mapping_parse_exception)

    mapping_language = _infer_mapping_language_from_graph(mapping_graph, data_source_name)

    mapping_parsing_query = ''
    if mapping_language == 'RML':
        mapping_parsing_query = RML_MAPPING_PARSING_QUERY
    elif mapping_language == 'R2RML':
        mapping_parsing_query = R2RML_MAPPING_PARSING_QUERY

    mapping_query_results = mapping_graph.query(mapping_parsing_query)
    join_query_results = mapping_graph.query(JOIN_CONDITION_PARSING_QUERY)

    return _transform_mappings_into_dataframe(mapping_query_results, join_query_results, data_source_name)


def _remove_duplicated_mapping_rules(mappings_df):
    """
    Removes the duplicates of the input DataFrame.

    :param mappings_df: DataFrame populated with mapping rules
    :type mappings_df: DataFrame
    :return mappings_df without duplicates
    :rtype DataFrame
    """

    num_mapping_rules = len(mappings_df)
    mappings_df = mappings_df.drop_duplicates()
    if len(mappings_df) < num_mapping_rules:
        logging.warning('Duplicated mapping rules were found. Ignoring duplicated mapping rules.')

    return mappings_df


def _rdf_class_to_pom(mappings_df):
    """
    Transforms rr:class properties (subject_rdf_class column in the input DataFrame) into POMs. The new mapping rules
    corresponding to rr:class properties are added to the input DataFrame and subject_rdf_class column is removed.

    :param mappings_df: DataFrame populated with mapping rules
    :type mappings_df: DataFrame
    :return mappings_df with additional POMs corresponding to rr:class properties
    :rtype DataFrame
    """

    initial_mapping_df = mappings_df.copy()

    for i, row in initial_mapping_df.iterrows():
        if pd.notna(row['subject_rdf_class']):
            # apped a new mapping rule to mappings_df corresponding to rr:class properties

            j = len(mappings_df)

            mappings_df.at[j, 'source_name'] = row['source_name']
            # add rdf_class_ at the beginning to avoid problems in later processing
            mappings_df.at[j, 'triples_map_id'] = 'rdf_class_' + str(row['triples_map_id'])
            mappings_df.at[j, 'tablename'] = row['tablename']
            mappings_df.at[j, 'query'] = row['query']
            mappings_df.at[j, 'subject_template'] = row['subject_template']
            mappings_df.at[j, 'subject_reference'] = row['subject_reference']
            mappings_df.at[j, 'subject_constant'] = row['subject_constant']
            mappings_df.at[j, 'graph_constant'] = row['graph_constant']
            mappings_df.at[j, 'graph_reference'] = row['graph_reference']
            mappings_df.at[j, 'graph_template'] = row['graph_template']
            mappings_df.at[j, 'subject_termtype'] = row['subject_termtype']
            mappings_df.at[j, 'predicate_constant'] = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
            mappings_df.at[j, 'object_constant'] = row['subject_rdf_class']
            mappings_df.at[j, 'object_termtype'] = 'http://www.w3.org/ns/r2rml#IRI'
            mappings_df.at[j, 'join_conditions'] = ''

    mappings_df = mappings_df.drop('subject_rdf_class', axis=1)     # subject_rdf_class column no longer needed
    mappings_df = mappings_df.drop_duplicates()                     # just to ensure there are no duplicates

    return mappings_df


def _process_pom_graphs(mappings_df):
    """
    Completes mapping rules in the input DataFrame with rr:defaultGraph if any graph term is provided for that mapping
    rule (as indicated in R2RML specification (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#generated-triples)).
    Also simplifies the DataFrame unifying graph terms in one column (graph_constant, graph_template, graph_reference).

    :param mappings_df: DataFrame populated with mapping rules
    :type mappings_df: DataFrame
    :return mappings_df with well defined graph terms
    :rtype DataFrame
    """

    # Use rr:defaultGraph for those mapping rules that do not have any graph term
    for i, mapping_rule in mappings_df.iterrows():
        if pd.isna(mapping_rule['graph_constant']) and pd.isna(mapping_rule['graph_reference']) and \
                pd.isna(mapping_rule['graph_template']):
            if pd.isna(mapping_rule['predicate_object_graph_constant']) and \
                    pd.isna(mapping_rule['predicate_object_graph_reference']) and \
                    pd.isna(mapping_rule['predicate_object_graph_template']):
                mappings_df.at[i, 'graph_constant'] = 'http://www.w3.org/ns/r2rml#defaultGraph'

    # Instead of having two columns for each option of graph term (e.g. graph_constant and
    # predicate_object_graph_constant) have only one to keep it simple. Do this by appending POM graph terms as new
    # mapping rules in the DataFrame.
    aux_mappings_df = mappings_df.copy()
    for i, mapping_rule in aux_mappings_df.iterrows():

        if pd.notna(mapping_rule['predicate_object_graph_constant']):
            j = len(mappings_df)
            mappings_df.loc[j] = mapping_rule
            mappings_df.at[j, 'graph_constant'] = mapping_rule['predicate_object_graph_constant']

        if pd.notna(mapping_rule['predicate_object_graph_template']):
            j = len(mappings_df)
            mappings_df.loc[j] = mapping_rule
            mappings_df.at[j, 'graph_template'] = mapping_rule['predicate_object_graph_template']

        if pd.notna(mapping_rule['predicate_object_graph_reference']):
            j = len(mappings_df)
            mappings_df.loc[j] = mapping_rule
            mappings_df.at[j, 'graph_reference'] = mapping_rule['predicate_object_graph_reference']

    # POM graph columns are no longer needed
    mappings_df = mappings_df.drop(columns=['predicate_object_graph_constant', 'predicate_object_graph_template',
                                            'predicate_object_graph_reference'])
    # Drop where graph_constant, graph_template and graph_reference are null. This is because original mapping rules
    # could not have graph term for subject maps but had it for POM, and because the newly appended mapping rule, the
    # old ones do no longer apply
    mappings_df = mappings_df.dropna(subset=['graph_constant', 'graph_template', 'graph_reference'], how='all')
    mappings_df = mappings_df.drop_duplicates()

    return mappings_df


def _complete_termtypes(mappings_df):
    """
    Completes term types of mapping rules that do not have rr:termType property as indicated in R2RML specification
    (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#termtype).

    :param mappings_df: DataFrame populated with mapping rules
    :type mappings_df: DataFrame
    :return mappings_df with term types for every mapping rule
    :rtype DataFrame
    """

    for i, mapping_rule in mappings_df.iterrows():
        if pd.isna(mapping_rule['subject_termtype']):
            mappings_df.at[i, 'subject_termtype'] = 'http://www.w3.org/ns/r2rml#IRI'
        if pd.isna(mapping_rule['object_termtype']):

            if pd.notna(mapping_rule['object_language']) or pd.notna(mapping_rule['object_datatype']) or \
                    pd.notna(mapping_rule['object_reference']):
                mappings_df.at[i, 'object_termtype'] = 'http://www.w3.org/ns/r2rml#Literal'

            else:
                mappings_df.at[i, 'object_termtype'] = 'http://www.w3.org/ns/r2rml#IRI'

    # Convert to str (instead of rdflib object) to avoid problems later
    mappings_df['subject_termtype'] = mappings_df['subject_termtype'].astype(str)
    mappings_df['object_termtype'] = mappings_df['object_termtype'].astype(str)

    return mappings_df


def _complete_source_types(mappings_df, config):
    """
    Complete the mapping rules in the input DataFrame with their source type

    :param mappings_df: DataFrame populated with mapping rules
    :type mappings_df: DataFrame
    :param config: ConfigParser object
    :type config: ConfigParser
    :return mappings_df with term types for every mapping rule
    :rtype DataFrame
    """

    for i, mapping_rule in mappings_df.iterrows():
        mappings_df.at[i, 'source_type'] = config.get(mapping_rule['source_name'], 'source_type').lower()

    return mappings_df


def _is_delimited(identifier):
    """
    Checks if an identifier is delimited or not.

    :param identifier: identifier in the data sources
    :type identifier: str
    :return whether it is a delimited identifier or not
    :rtype bool
    """

    if len(identifier) > 2:
        if identifier[0] == '"' and identifier[len(identifier) - 1] == '"':
            return True
    return False


def _get_undelimited_identifier(identifier):
    """
    Removes delimiters from the identifier if it is delimited.

    :param identifier: identifier in mapping rules
    :return undelimited identifier
    """

    if pd.notna(identifier):
        identifier = str(identifier)
        if _is_delimited(identifier):
            return identifier[1:-1]
    return identifier


def _get_valid_template_identifiers(template):
    """
    Removes delimiters from delimited identifiers in a template.

    :param template: template term map
    :return template with undelimited identifiers
    """

    if pd.notna(template):
        return template.replace('{"', '{').replace('"}', '}')
    return template


def _remove_delimiters_from_identifiers(mappings_df):
    """
    Removes demiliters from all identifiers in the mapping rules in the input DataFrame.

    :param mappings_df: DataFrame populated with mapping rules
    :type mappings_df: DataFrame
    :return mappings_df without delimiters in identifiers
    :rtype DataFrame
    """

    for i, mapping_rule in mappings_df.iterrows():
        mappings_df.at[i, 'tablename'] = _get_undelimited_identifier(mapping_rule['tablename'])
        mappings_df.at[i, 'subject_template'] = _get_valid_template_identifiers(mapping_rule['subject_template'])
        mappings_df.at[i, 'subject_reference'] = _get_undelimited_identifier(mapping_rule['subject_reference'])
        mappings_df.at[i, 'graph_reference'] = _get_undelimited_identifier(mapping_rule['graph_reference'])
        mappings_df.at[i, 'graph_template'] = _get_valid_template_identifiers(mapping_rule['graph_template'])
        mappings_df.at[i, 'predicate_template'] = _get_valid_template_identifiers(mapping_rule['predicate_template'])
        mappings_df.at[i, 'predicate_reference'] = _get_undelimited_identifier(mapping_rule['predicate_reference'])
        mappings_df.at[i, 'object_template'] = _get_valid_template_identifiers(mapping_rule['object_template'])
        mappings_df.at[i, 'object_reference'] = _get_undelimited_identifier(mapping_rule['object_reference'])

        if mapping_rule['join_conditions']:
            join_conditions = eval(mapping_rule['join_conditions'])
            for key, value in join_conditions.items():
                join_conditions[key]['child_value'] = _get_undelimited_identifier(join_conditions[key]['child_value'])
                join_conditions[key]['parent_value'] = _get_undelimited_identifier(join_conditions[key]['parent_value'])
                mappings_df.at[i, 'join_conditions'] = str(join_conditions)

    return mappings_df


def _infer_datatypes(mappings_df, config):
    """
    Get RDF datatypes for rules corresponding to relational data sources if they are not overridden in the mapping
    rules. The inferring of RDF datatypes is defined in R2RML specification
    (https://www.w3.org/2001/sw/rdb2rdf/r2rml/#natural-mapping).

    :param mappings_df: DataFrame populated with mapping rules
    :type mappings_df: DataFrame
    :param config: ConfigParser object to access DBMSs
    :type config: ConfigParser
    :return mappings_df with inferred datatypes for rules that do not have overridden datatypes
    :rtype DataFrame
    """

    for i, mapping_rule in mappings_df.iterrows():
        # datatype inferring only applies to relational data sources
        if mapping_rule['source_type'] in RELATIONAL_SOURCE_TYPES:
            # datatype inferring only applies to literals
            if mapping_rule['object_termtype'] == 'http://www.w3.org/ns/r2rml#Literal':
                # if the literal has a language tag or an overridden datatype, datatype inference does not apply
                if pd.isna(mapping_rule['object_datatype']) and pd.isna(mapping_rule['object_language']):

                    if pd.notna(mapping_rule['tablename']):
                        mappings_df.at[i, 'object_datatype'] = relational_source.get_column_datatype(
                            config, mapping_rule['source_name'], mapping_rule['tablename'],
                            mapping_rule['object_reference']
                        ).upper()

                    elif pd.notna(mapping_rule['query']):
                        # if mapping rule has a query, get the table names
                        table_names = sql_metadata.get_query_tables(mapping_rule['query'])
                        for table_name in table_names:
                            # for each table in the query check get the datatype of the object reference in that table
                            # if an exception is thrown, then the reference is no a column in that table, and nothing
                            # is done
                            try:
                                mappings_df.at[i, 'object_datatype'] = relational_source.get_column_datatype(
                                    config, mapping_rule['source_name'], table_name, mapping_rule['object_reference']
                                ).upper()
                            except:
                                pass

    return mappings_df


def _validate_parsed_mappings(mappings_df):
    """
    Checks that the mapping rules in the input DataFrame are valid. If something is wrong in the mappings the execution
    is stopped. Specifically it is checked that termtypes are correct, constants and templates are valid IRIs and that
    language tags and datatypes are used properly.

    :param mappings_df: DataFrame populated with mapping rules
    :type mappings_df: DataFrame
    """

    # check termtypes are correct, use subset operation
    if not (set(mappings_df['subject_termtype'].astype(str)) <= {'http://www.w3.org/ns/r2rml#IRI',
                                                                 'http://www.w3.org/ns/r2rml#BlankNode'}):
        raise ValueError('Found an invalid subject termtype. Found values ' + str(
            set(mappings_df['subject_termtype'].astype(str))) + '. Subject maps must be rr:IRI or rr:BlankNode.')
    if not (set(mappings_df['object_termtype'].astype(str)) <= {'http://www.w3.org/ns/r2rml#IRI',
                                                                'http://www.w3.org/ns/r2rml#BlankNode',
                                                                'http://www.w3.org/ns/r2rml#Literal'}):
        raise ValueError('Found an invalid object termtype. Found values ' + str(set(
            mappings_df['subject_termtype'].astype(str))) + '. Object maps must be rr:IRI, rr:BlankNode or rr:Literal.')

    # if there is a datatype or language tag then the object map termtype must be a rr:Literal
    if len(mappings_df.loc[(mappings_df['object_termtype'] != 'http://www.w3.org/ns/r2rml#Literal') & mappings_df[
            'object_datatype'].notnull() & mappings_df['object_language'].notnull()]) > 0:
        raise Exception('Found object maps with a language tag or a datatype, '
                        'but that do not have termtype rr:Literal.')

    # language tags and datatypes cannot be used simultaneously, language tags are used if both are given
    if len(mappings_df.loc[mappings_df['object_language'].notnull() & mappings_df['object_datatype'].notnull()]) > 0:
        logging.warning('Found object maps with a language tag and a datatype. Both of them cannot be used '
                        'simultaneously for the same object map, and the language tag has preference.')

    # check constants are valid IRIs
    constants = list(mappings_df['predicate_constant'].dropna())
    constants.extend(list(mappings_df['graph_constant'].dropna()))
    constants.extend(list(mappings_df.loc[
                              (mappings_df['subject_termtype'] == 'http://www.w3.org/ns/r2rml#IRI') & mappings_df[
                                  'subject_constant'].notnull()]['subject_constant']))
    constants.extend(list(mappings_df.loc[
                              (mappings_df['object_termtype'] == 'http://www.w3.org/ns/r2rml#IRI') & mappings_df[
                                  'object_constant'].notnull()]['object_constant']))
    for constant in set(constants):
        rfc3987.parse(constant, rule='IRI')

    # check templates are valid IRIs
    templates = list(mappings_df['predicate_template'].dropna())
    templates.extend(list(mappings_df['graph_template'].dropna()))
    templates.extend(list(mappings_df.loc[
                              (mappings_df['subject_termtype'] == 'http://www.w3.org/ns/r2rml#IRI') & mappings_df[
                                  'subject_template'].notnull()]['subject_template']))
    templates.extend(list(mappings_df.loc[
                              (mappings_df['object_termtype'] == 'http://www.w3.org/ns/r2rml#IRI') & mappings_df[
                                  'object_template'].notnull()]['object_template']))
    for template in templates:
        # validate that at least the invariable part of the template is a valid IRI
        rfc3987.parse(_get_invariable_part_of_template(template), rule='IRI')


def _parse_mappings(config):
    """
    Parses the mapping files in each source of the input config file. It also checks that the parsed mappings are valid.

    :param config: ConfigParser object
    :type config: ConfigParser
    :return parsed mappings
    :rtype DataFrame
    """

    mappings_df = pd.DataFrame(columns=MAPPINGS_DATAFRAME_COLUMNS)

    # parse mapping files for each data source in the config file and add the parsed mappings rules to a
    # common DataFrame for all data sources
    for config_section in config.sections():
        if config_section != 'CONFIGURATION':
            source_mappings_df = _parse_mapping_files(config, config_section)
            mappings_df = pd.concat([mappings_df, source_mappings_df])
            mappings_df = mappings_df.reset_index(drop=True)
            logging.info('Mappings for data source ' + str(config_section) + ' successfully parsed.')

    # postprocessing of parsed mappings
    mappings_df = _remove_duplicated_mapping_rules(mappings_df)
    mappings_df = _rdf_class_to_pom(mappings_df)
    mappings_df = _process_pom_graphs(mappings_df)
    mappings_df = _complete_termtypes(mappings_df)
    mappings_df = _complete_source_types(mappings_df, config)
    mappings_df = _remove_delimiters_from_identifiers(mappings_df)

    # create a unique id for each mapping rule
    mappings_df.insert(0, 'id', mappings_df.reset_index().index)

    # if infer_datatypes is enabled, infer the RDF datatypes for mapping rules of relational data sources
    if config.getboolean('CONFIGURATION', 'infer_datatypes'):
        mappings_df = _infer_datatypes(mappings_df, config)

    # check that the parsed mapping rules are correct
    _validate_parsed_mappings(mappings_df)

    return mappings_df


def _validate_mapping_partition_criteria(mappings_df, mapping_partition_criteria):
    """
    Checks that the mapping partitioning criteria is valid. A criteria (subject (s), predicate(p), or graph(g)) is not
    valid if there is a mapping rule that uses reference terms to generate terms for that criteria ((s), (p),
    (g)). Any invalid criteria is omitted and a valid partitioning criteria is returned. If `guess` is selected as
    mapping partitioning criteria, then all valid criteria for the mapping rules in the input DataFrame is returned.

    :param mappings_df: DataFrame populated with mapping rules
    :type mappings_df: DataFrame
    :param mapping_partition_criteria: criteria to perform mapping partitioning
    :type mapping_partition_criteria: str
    :return valid criteria to perform mapping partitioning
    :rtype str
    """

    valid_mapping_partition_criteria = ''

    if 'guess' in mapping_partition_criteria:
        # add as mapping partitioning criteria all criteria that is valid for the mapping rules in the DataFrame
        if not mappings_df['subject_reference'].notna().any():
            valid_mapping_partition_criteria += 's'
        if not mappings_df['predicate_reference'].notna().any():
            valid_mapping_partition_criteria += 'p'
        if not mappings_df['graph_reference'].notna().any():
            valid_mapping_partition_criteria += 'g'

    else:
        if 's' in mapping_partition_criteria:
            '''
            Subject is used as partitioning criteria.
            If there is any subject that is a reference that means it is not a template nor a constant, and it cannot
            be used as partitioning criteria. The same for predicate and graph.
            '''
            if mappings_df['subject_reference'].notna().any():
                logging.warning('Invalid mapping partition criteria ' + mapping_partition_criteria +
                                ': mappings cannot be partitioned by subject because mappings contain subject terms '
                                'that are rr:column or rml:reference.')
            else:
                valid_mapping_partition_criteria += 's'

        if 'p' in mapping_partition_criteria:
            if mappings_df['predicate_reference'].notna().any():
                logging.warning('Invalid mapping partition criteria ' + mapping_partition_criteria +
                                ': mappings cannot be partitioned by predicate because mappings contain predicate '
                                'terms that are rr:column or rml:reference.')
            else:
                valid_mapping_partition_criteria += 'p'

        if 'g' in mapping_partition_criteria:
            if mappings_df['graph_reference'].notna().any():
                logging.warning('Invalid mapping partition criteria ' + mapping_partition_criteria +
                                ': mappings cannot be partitioned by graph because mappings '
                                'contain graph terms that are rr:column or rml:reference.')
            else:
                valid_mapping_partition_criteria += 'g'

    if mapping_partition_criteria:
        logging.info('Using ' + valid_mapping_partition_criteria + ' as mapping partition criteria.')
    else:
        logging.info('Not using mapping patitioning.')

    return valid_mapping_partition_criteria


def _get_invariable_part_of_template(template):
    """
    Retrieves the part of the template before the first reference. This part of the template does not depend on
    reference and therefore is invariable. If the template has no references, it is an invalid template, and an
    exception is thrown.

    :param template: template
    :type template: str
    :return invariable part of the template
    :rtype str
    """

    zero_width_space = '\u200B'
    template_for_splitting = template.replace('\\{', zero_width_space)
    if '{' in template_for_splitting:
        invariable_part_of_template = template_for_splitting.split('{')[0]
        invariable_part_of_template = invariable_part_of_template.replace(zero_width_space, '\\{')
    else:
        # no references were found in the template, and therefore the template is invalid
        raise Exception('Invalid template ' + template + '. No pairs of unescaped curly braces were found.')

    return invariable_part_of_template


def _get_mapping_partitions_invariable_parts(mappings_df, mapping_partition_criteria):
    """
    Adds in the input DataFrame new columns for the invariable parts of mapping rules. Columns for the invariable parts
    of subjects, predicates and graphs are added, and they are completed based on the provided mapping partitioning
    criteria.

    :param mappings_df: DataFrame populated with mapping rules
    :type mappings_df: DataFrame
    :param mapping_partition_criteria: criteria to perform mapping partitioning
    :type mapping_partition_criteria: str
    :return DataFrame of mapping rules with additional columns for invariable parts of subjects, predicates and graphs
    :rtype DataFrame
    """

    mappings_df['subject_invariable_part'] = ''
    mappings_df['predicate_invariable_part'] = ''
    mappings_df['graph_invariable_part'] = ''

    for i, mapping_rule in mappings_df.iterrows():

        if 's' in mapping_partition_criteria:
            # subject is selected as partitioning criteria if it is a template or a constant to get the invariable part
            if mapping_rule['subject_template']:
                mappings_df.at[i, 'subject_invariable_part'] = \
                    _get_invariable_part_of_template(mapping_rule['subject_template'])
            elif mapping_rule['subject_constant']:
                mappings_df.at[i, 'subject_invariable_part'] = mapping_rule['subject_constant']
            else:
                raise Exception('An invalid subject term was found at triples map ' + mapping_rule['triples_map_id'] +
                                '. Subjects terms must be constants or templates in order to generate valid mapping '
                                'partitions by subject.')

        if 'p' in mapping_partition_criteria:
            if mapping_rule['predicate_constant']:
                mappings_df.at[i, 'predicate_invariable_part'] = mapping_rule['predicate_constant']
            elif mapping_rule['predicate_template']:
                mappings_df.at[i, 'predicate_invariable_part'] = \
                    _get_invariable_part_of_template(mapping_rule['predicate_template'])
            else:
                raise Exception('An invalid predicate term was found at triples map ' + mapping_rule['triples_map_id'] +
                                '. Predicate terms must be constants or templates in order to generate valid mapping '
                                'partitions by predicate.')

        if 'g' in mapping_partition_criteria:
            if mapping_rule['graph_constant']:
                mappings_df.at[i, 'graph_invariable_part'] = mapping_rule['graph_constant']
            elif mapping_rule['graph_template']:
                mappings_df.at[i, 'graph_invariable_part'] = \
                    _get_invariable_part_of_template(mapping_rule['graph_template'])
            else:
                raise Exception('An invalid graph term was found at triples map ' + mapping_rule['triples_map_id'] +
                                '. Graph terms must be constants or templates in order to generate valid mapping '
                                'partitions by graph.')

    return mappings_df


def _generate_mapping_partitions(mappings_df, mapping_partition_criteria):
    """
    Generates the mapping partitions for the mapping rules in the input DataFrame based on the provided mapping
    partitioning criteria. A new column in the DataFrame is added indicating the mapping partition assigned to every
    mapping rule.

    :param mappings_df: DataFrame populated with mapping rules
    :type mappings_df: DataFrame
    :param mapping_partition_criteria: criteria to perform mapping partitioning
    :type mapping_partition_criteria: str
    :return DataFrame of mapping rules with an additional column with the mapping partition assigned to each rule
    :rtype DataFrame
    """

    mapping_partitions = _validate_mapping_partition_criteria(mappings_df, mapping_partition_criteria)

    mappings_df = _get_mapping_partitions_invariable_parts(mappings_df, mapping_partitions)
    mappings_df['subject_partition'] = ''
    mappings_df['predicate_partition'] = ''
    mappings_df['graph_partition'] = ''

    # generate independent mapping partitions for subjects, predicates and graphs
    if 's' in mapping_partitions:
        # sort the mapping rules based on the subject invariable part
        # an invariable part that starts with another invariable part is placed behind in the ordering
        # E.g. http://example.org/term/something and http://example.org/term: http://example.org/term is placed first
        mappings_df.sort_values(by='subject_invariable_part', inplace=True, ascending=True)

        num_partition = 0
        root_last_partition = 'zzyy xxww\u200B'

        # iterate over the mapping rules and check if the invariable part starts with the invariable part of the
        # previous rule
        # if it does, then it is in the same mapping partition than the previous mapping rule
        # If it does not, then the mapping rule is in a new mapping partition
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

    if 'g' in mapping_partitions:
        mappings_df.sort_values(by='graph_invariable_part', inplace=True, ascending=True)
        num_partition = 0
        root_last_partition = 'zzyy xxww\u200B'
        for i, mapping_rule in mappings_df.iterrows():
            if mapping_rule['graph_invariable_part'].startswith(root_last_partition):
                mappings_df.at[i, 'graph_partition'] = str(num_partition)
            else:
                num_partition = num_partition + 1
                root_last_partition = mapping_rule['graph_invariable_part']
                mappings_df.at[i, 'graph_partition'] = str(num_partition)

    # aggregate the independent mapping partitions generated for subjects, predicates and graphs to generate the final
    # mapping partitions
    if 's' in mapping_partitions and 'p' in mapping_partitions:
        mappings_df['mapping_partition'] = mappings_df['subject_partition'] + '-' + mappings_df['predicate_partition']
    else:
        mappings_df['mapping_partition'] = mappings_df['subject_partition'] + mappings_df['predicate_partition']
    if 'g' in mapping_partitions and 's' not in mapping_partitions and 'p' not in mapping_partitions:
        mappings_df['mapping_partition'] = mappings_df['graph_partition']
    elif 'g' in mapping_partitions:
        mappings_df['mapping_partition'] = mappings_df['mapping_partition'] + '-' + mappings_df['graph_partition']

    # drop the auxiliary columns that were created just to generate the mapping partitions
    mappings_df.drop([
        'subject_partition',
        'predicate_partition',
        'subject_invariable_part',
        'predicate_invariable_part',
        'graph_partition',
        'graph_invariable_part'],
        axis=1, inplace=True)

    if mapping_partitions:
        logging.info(str(len(set(mappings_df['mapping_partition']))) + ' mapping partitions were generated.')

    return mappings_df


def process_mappings(config):
    """
    Manages the mapping processing in the engine. If an input parsed mappings are passed in the config, then they are
    loaded and mapping processing is finished. If no input parsed mappings are provided then the mappings for all
    sources in the config are parsed and mapping partitions are generated. If an output path for the parsed mappings is
    provided in the config, then the parsed mappings are saved to a file and the execution of the engine terminates.

    :param config: ConfigParser configuration object
    :type config: ConfigParser
    :return DataFrame with parsed mapping rules and with mapping partitions for every mapping rule
    :rtype DataFrame
    """

    input_parsed_mappings_path = config.get('CONFIGURATION', 'input_parsed_mappings_path')
    if input_parsed_mappings_path:
        # retrieve parsed mapping from file and finish mapping processing
        mappings_df = pd.read_csv(input_parsed_mappings_path, keep_default_na=False)
        return mappings_df

    # parse mapping files of every data source in the config
    start_parsing = time.time()
    mappings_df = _parse_mappings(config)
    logging.info('Mapping parsing time: ' + "{:.4f}".format((time.time() - start_parsing)) + ' seconds.')

    # generate mapping partitions for every mapping rule based on the criteria provided in the config
    start_mapping_partitions = time.time()
    mappings_df = _generate_mapping_partitions(mappings_df, config.get('CONFIGURATION', 'mapping_partitions'))
    logging.info('Mapping partitions generation time: ' + "{:.4f}".format(
        (time.time() - start_mapping_partitions)) + ' seconds.')

    # use empty strings, avoid None & NaN
    mappings_df = mappings_df.fillna('')

    output_parsed_mappings_path = config.get('CONFIGURATION', 'output_parsed_mappings_path')
    if output_parsed_mappings_path:
        # the parsed mappings are to be saved to a file, and the execution of the engine terminates
        mappings_df.sort_values(by=['id'], axis=0).to_csv(output_parsed_mappings_path, index=False)
        sys.exit()

    return mappings_df
