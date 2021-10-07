__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import pandas as pd
import multiprocessing as mp
import logging
import time

from itertools import repeat
from falcon.uri import encode

from .utils import *
from .constants import *
from .data_source.relational_source import get_sql_data
from .data_source.tabular_source import get_table_data


def _get_references_in_mapping_rule(mapping_rule, only_subject_map=False):
    references = []
    if pd.notna(mapping_rule['subject_template']):
        references.extend(get_references_in_template(str(mapping_rule['subject_template'])))
    elif pd.notna(mapping_rule['subject_reference']):
        references.append(str(mapping_rule['subject_reference']))

    if not only_subject_map:
        if pd.notna(mapping_rule['predicate_template']):
            references.extend(get_references_in_template(str(mapping_rule['predicate_template'])))
        elif pd.notna(mapping_rule['predicate_reference']):
            references.append(str(mapping_rule['predicate_reference']))
        if pd.notna(mapping_rule['object_template']):
            references.extend(get_references_in_template(str(mapping_rule['object_template'])))
        elif pd.notna(mapping_rule['object_reference']):
            references.append(str(mapping_rule['object_reference']))
        if pd.notna(mapping_rule['graph_template']):
            references.extend(get_references_in_template(str(mapping_rule['graph_template'])))
        elif pd.notna(mapping_rule['graph_reference']):
            references.append(str(mapping_rule['graph_reference']))

    return set(references)


def _materialize_template(results_df, template, config, columns_alias='', termtype=R2RML_IRI, language_tag='',
                          datatype=''):
    references = get_references_in_template(str(template))

    if str(termtype).strip() == R2RML_LITERAL:
        results_df['triple'] = results_df['triple'] + '"'
    else:
        results_df['triple'] = results_df['triple'] + '<'

    for reference in references:
        results_df['reference_results'] = results_df[columns_alias + reference]

        if config.only_write_printable_characters():
            results_df['reference_results'] = results_df['reference_results'].apply(lambda x: remove_non_printable_characters(encode(x)))

        if str(termtype).strip() == R2RML_IRI:
            # falcon's encode is faster than urllib's quote
            results_df['reference_results'] = results_df['reference_results'].apply(lambda x: encode(x))
        elif str(termtype).strip() == R2RML_LITERAL:
            results_df['reference_results'] = results_df['reference_results'].str.replace('"', '\\"', regex=False).str.replace('\\', '\\\\"', regex=False)

        splitted_template = template.split('{' + reference + '}')
        results_df['triple'] = results_df['triple'] + splitted_template[0] + results_df['reference_results']
        template = str('{' + reference + '}').join(splitted_template[1:])
    if template:
        # add what remains in the template after the last reference
        results_df['triple'] = results_df['triple'] + template

    if str(termtype).strip() == R2RML_LITERAL:
        results_df['triple'] = results_df['triple'] + '"'
        if pd.notna(language_tag):
            results_df['triple'] = results_df['triple'] + '@' + language_tag + ' '
        elif pd.notna(datatype):
            results_df['triple'] = results_df['triple'] + '^^<' + datatype + '> '
        else:
            results_df['triple'] = results_df['triple'] + ' '
    else:
        results_df['triple'] = results_df['triple'] + '> '

    return results_df


def _materialize_reference(results_df, reference, config, columns_alias='', termtype=R2RML_LITERAL,
                           language_tag='', datatype=''):
    results_df['reference_results'] = results_df[columns_alias + str(reference)]

    if config.only_write_printable_characters():
        results_df['reference_results'] = results_df['reference_results'].apply(lambda x: remove_non_printable_characters(encode(x)))

    if str(termtype).strip() == R2RML_IRI:
        # falcon's encode is faster than urllib's quote
        results_df['reference_results'] = results_df['reference_results'].apply(lambda x: encode(x))
        results_df['triple'] = results_df['triple'] + '<' + results_df['reference_results'] + '> '
    elif str(termtype).strip() == R2RML_LITERAL:
        results_df['reference_results'] = results_df['reference_results'].str.replace('"', '\\"', regex=False).str.replace('\\', '\\\\"', regex=False)
        results_df['triple'] = results_df['triple'] + '"' + results_df['reference_results'] + '"'
        if pd.notna(language_tag):
            results_df['triple'] = results_df['triple'] + '@' + language_tag + ' '
        elif pd.notna(datatype):
            results_df['triple'] = results_df['triple'] + '^^<' + datatype + '> '
        else:
            results_df['triple'] = results_df['triple'] + ' '

    return results_df


def _materialize_constant(results_df, constant, termtype=R2RML_IRI, language_tag='', datatype=''):
    if str(termtype).strip() == R2RML_LITERAL:
        complete_constant = '"' + constant + '"'

        if pd.notna(language_tag):
            complete_constant = complete_constant + '@' + language_tag + ' '
        elif pd.notna(datatype):
            complete_constant = complete_constant + '^^<' + datatype + '> '
        else:
            complete_constant = complete_constant + ' '
    else:
        complete_constant = '<' + str(constant) + '> '

    results_df['triple'] = results_df['triple'] + complete_constant

    return results_df


def _materialize_join_mapping_rule_terms(results_df, mapping_rule, parent_triples_map_rule, config):
    results_df['triple'] = ''
    if pd.notna(mapping_rule['subject_template']):
        results_df = _materialize_template(results_df, mapping_rule['subject_template'], config, termtype=mapping_rule['subject_termtype'], columns_alias='child_')
    elif pd.notna(mapping_rule['subject_constant']):
        results_df = _materialize_constant(results_df, mapping_rule['subject_constant'], termtype=mapping_rule['subject_termtype'])
    elif pd.notna(mapping_rule['subject_reference']):
        results_df = _materialize_reference(results_df, mapping_rule['subject_reference'], config, termtype=mapping_rule['subject_termtype'], columns_alias='child_')
    if pd.notna(mapping_rule['predicate_template']):
        results_df = _materialize_template(results_df, mapping_rule['predicate_template'], config, columns_alias='child_')
    elif pd.notna(mapping_rule['predicate_constant']):
        results_df = _materialize_constant(results_df, mapping_rule['predicate_constant'])
    elif pd.notna(mapping_rule['predicate_reference']):
        results_df = _materialize_reference(results_df, mapping_rule['predicate_reference'], config, termtype=R2RML_IRI, columns_alias='child_')
    if pd.notna(parent_triples_map_rule['subject_template']):
        results_df = _materialize_template(results_df, parent_triples_map_rule['subject_template'], config, termtype=parent_triples_map_rule['subject_termtype'], columns_alias='parent_')
    elif pd.notna(parent_triples_map_rule['subject_constant']):
        results_df = _materialize_constant(results_df, parent_triples_map_rule['subject_constant'], termtype=parent_triples_map_rule['subject_termtype'])
    elif pd.notna(parent_triples_map_rule['subject_reference']):
        results_df = _materialize_reference(results_df, parent_triples_map_rule['subject_reference'], config, termtype=parent_triples_map_rule['subject_termtype'], columns_alias='parent_')
    if pd.notna(mapping_rule['graph_template']):
        results_df = _materialize_template(results_df, mapping_rule['graph_template'], config, columns_alias='child_')
    elif pd.notna(mapping_rule['graph_constant']):
        if pd.notna(mapping_rule['graph_constant'] != R2RML_DEFAULT_GRAPH):
            results_df = _materialize_constant(results_df, mapping_rule['graph_constant'])
    elif pd.notna(mapping_rule['graph_reference']):
        results_df = _materialize_reference(results_df, mapping_rule['graph_reference'], config, termtype=R2RML_IRI, columns_alias='child_')

    return set(results_df['triple'])


def _materialize_mapping_rule_terms(results_df, mapping_rule, config):
    results_df['triple'] = ''
    if pd.notna(mapping_rule['subject_template']):
        results_df = _materialize_template(results_df, mapping_rule['subject_template'], config, termtype=mapping_rule['subject_termtype'])
    elif pd.notna(mapping_rule['subject_constant']):
        results_df = _materialize_constant(results_df, mapping_rule['subject_constant'], termtype=mapping_rule['subject_termtype'])
    elif pd.notna(mapping_rule['subject_reference']):
        results_df = _materialize_reference(results_df, mapping_rule['subject_reference'], config, termtype=mapping_rule['subject_termtype'])
    if pd.notna(mapping_rule['predicate_template']):
        results_df = _materialize_template(results_df, mapping_rule['predicate_template'])
    elif pd.notna(mapping_rule['predicate_constant']):
        results_df = _materialize_constant(results_df, mapping_rule['predicate_constant'])
    elif pd.notna(mapping_rule['predicate_reference']):
        results_df = _materialize_reference(results_df, mapping_rule['predicate_reference'], config, termtype=R2RML_IRI)
    if pd.notna(mapping_rule['object_template']):
        results_df = _materialize_template(results_df, mapping_rule['object_template'], config, termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])
    elif pd.notna(mapping_rule['object_constant']):
        results_df = _materialize_constant(results_df, mapping_rule['object_constant'], termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])
    elif pd.notna(mapping_rule['object_reference']):
        results_df = _materialize_reference(results_df, mapping_rule['object_reference'], config, termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])
    if config.get_output_format() == 'N-QUADS':
        if pd.notna(mapping_rule['graph_template']):
            results_df = _materialize_template(results_df, mapping_rule['graph_template'], config)
        elif pd.notna(mapping_rule['graph_constant']):
            if config.materialize_default_graph() or mapping_rule['graph_constant'] != R2RML_DEFAULT_GRAPH:
                results_df = _materialize_constant(results_df, mapping_rule['graph_constant'])
        elif pd.notna(mapping_rule['graph_reference']):
            results_df = _materialize_reference(results_df, mapping_rule['graph_reference'], config, termtype=R2RML_IRI)

    return set(results_df['triple'])


def _materalize_push_down_sql_join(mapping_rule, parent_triples_map_rule, references, parent_references, config):
    for key, join_condition in eval(mapping_rule['join_conditions']).items():
        parent_references.add(join_condition['parent_value'])
        references.add(join_condition['child_value'])

    triples_rule = set()
    result_chunks = get_sql_data(config, mapping_rule, references, parent_triples_map_rule, parent_references)
    for query_results_chunk_df in result_chunks:
        query_results_chunk_df = dataframe_columns_to_str(query_results_chunk_df)
        # query_results_chunk_df.dropna(axis=0, how='any', inplace=True)
        triples_rule.update(
            _materialize_join_mapping_rule_terms(query_results_chunk_df, mapping_rule, parent_triples_map_rule, config))

    return triples_rule


def _merge_results_chunks(query_results_chunk_df, parent_query_results_chunk_df, mapping_rule):
    child_join_references, parent_join_references = get_references_in_join_condition(mapping_rule)

    child_join_references = ['child_' + reference for reference in child_join_references]
    parent_join_references = ['parent_' + reference for reference in parent_join_references]

    return query_results_chunk_df.merge(parent_query_results_chunk_df,
                                        how='inner',
                                        left_on=child_join_references,
                                        right_on=parent_join_references)


def _materialize_mapping_rule(mapping_rule, subject_maps_df, config):
    triples = set()

    result_chunks = None
    parent_result_chunks = None

    references = _get_references_in_mapping_rule(mapping_rule)

    if pd.notna(mapping_rule['object_parent_triples_map']):
        parent_triples_map_rule = \
            subject_maps_df[subject_maps_df.triples_map_id == mapping_rule['object_parent_triples_map']].iloc[0]
        parent_references = _get_references_in_mapping_rule(parent_triples_map_rule, only_subject_map=True)

        if config.push_down_sql_joins() and \
                mapping_rule['source_type'] == RDB_SOURCE_TYPE and \
                mapping_rule['source_name'] == parent_triples_map_rule['source_name']:
            triples.update(_materalize_push_down_sql_join(mapping_rule, parent_triples_map_rule, references,
                                                          parent_references, config))

        else:
            # add references used in the join condition (not needed when pushing down join to SQL)
            references, parent_references = add_references_in_join_condition(mapping_rule, references,
                                                                                   parent_references)

            if mapping_rule['source_type'] == RDB_SOURCE_TYPE:
                result_chunks = get_sql_data(config, mapping_rule, references)
            elif mapping_rule['source_type'] in TABULAR_SOURCE_TYPES:
                result_chunks = get_table_data(config, mapping_rule, references)

            for query_results_chunk_df in result_chunks:
                query_results_chunk_df = dataframe_columns_to_str(query_results_chunk_df)
                #query_results_chunk_df.replace(config.get_na_values(), np.NaN)
                query_results_chunk_df.dropna(axis=0, how='any', inplace=True)
                query_results_chunk_df = query_results_chunk_df.add_prefix('child_')

                if parent_triples_map_rule['source_type'] == RDB_SOURCE_TYPE:
                    parent_result_chunks = get_sql_data(config, parent_triples_map_rule, parent_references)
                elif parent_triples_map_rule['source_type'] in TABULAR_SOURCE_TYPES:
                    parent_result_chunks = get_table_data(config, parent_triples_map_rule, parent_references)

                for parent_query_results_chunk_df in parent_result_chunks:
                    parent_query_results_chunk_df = dataframe_columns_to_str(parent_query_results_chunk_df)
                    #parent_query_results_chunk_df.replace(config.get_na_values(), np.NaN)
                    parent_query_results_chunk_df.dropna(axis=0, how='any', inplace=True)
                    parent_query_results_chunk_df = parent_query_results_chunk_df.add_prefix('parent_')
                    merged_query_results_chunk_df = _merge_results_chunks(query_results_chunk_df,
                                                                          parent_query_results_chunk_df, mapping_rule)

                    triples.update(
                        _materialize_join_mapping_rule_terms(merged_query_results_chunk_df, mapping_rule,
                                                             parent_triples_map_rule))

    else:
        if mapping_rule['source_type'] in RDB_SOURCE_TYPE:
            result_chunks = get_sql_data(config, mapping_rule, references)
        elif mapping_rule['source_type'] in TABULAR_SOURCE_TYPES:
            result_chunks = get_table_data(config, mapping_rule, references)

        for query_results_chunk_df in result_chunks:
            #query_results_chunk_df.replace(config.get_na_values(), np.NaN)
            query_results_chunk_df.dropna(axis=0, how='any', inplace=True)
            query_results_chunk_df = dataframe_columns_to_str(query_results_chunk_df)
            triples.update(_materialize_mapping_rule_terms(query_results_chunk_df, mapping_rule, config))

    return triples


def _materialize_mapping_partition(mapping_partition, subject_maps_df, config):
    triples = set()
    for i, mapping_rule in mapping_partition.iterrows():
        start_time = time.time()
        triples.update(set(_materialize_mapping_rule(mapping_rule, subject_maps_df, config)))

        logging.debug(str(len(triples)) + ' triples generated for mapping rule `' + str(
            mapping_rule['id']) + '` in ' + get_delta_time(start_time) + ' seconds.')

    triples_to_file(triples, config, mapping_partition.iloc[0]['mapping_partition'])

    return len(triples)


class Materializer:

    def __init__(self, mappings_df, config):
        self.mappings_df = mappings_df
        self.config = config

        self.subject_maps_df = get_subject_maps(mappings_df)
        self.mapping_partitions = [group for _, group in mappings_df.groupby(by='mapping_partition')]

        clean_output_dir(config)

    def __str__(self):
        return str(self.mappings_df)

    def __repr__(self):
        return repr(self.mappings_df)

    def __len__(self):
        return len(self.mappings_df)

    def materialize(self):
        num_triples = 0
        for mapping_partition in self.mapping_partitions:
            num_triples += _materialize_mapping_partition(mapping_partition, self.subject_maps_df, self.config)

        logging.info('Number of triples generated in total: ' + str(num_triples) + '.')

    def materialize_concurrently(self):
        logging.debug('Parallelizing with ' + str(self.config.get_number_of_processes()) + ' cores.')

        pool = mp.Pool(self.config.get_number_of_processes())
        num_triples = sum(pool.starmap(_materialize_mapping_partition,
                                       zip(self.mapping_partitions, repeat(self.subject_maps_df), repeat(self.config))))
        pool.close()
        pool.join()

        logging.info('Number of triples generated in total: ' + str(num_triples) + '.')
