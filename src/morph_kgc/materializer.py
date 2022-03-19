__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import pandas as pd
import multiprocessing as mp
import logging

from itertools import repeat
from falcon.uri import encode_value
from urllib.parse import quote

from .utils import *
from .constants import *
from .data_source.relational_database import get_sql_data, get_rdb_reference_datatype
from .data_source.data_file import get_file_data


def _add_references_in_join_condition(mapping_rule, references, parent_references):
    references_join, parent_references_join = get_references_in_join_condition(mapping_rule)

    references.update(set(references_join))
    parent_references.update(set(parent_references_join))

    return references, parent_references


def _retrieve_rdb_integer_references(config, mapping_rule, references):
    integer_references = []
    for reference in references:
        if XSD_INTEGER == get_rdb_reference_datatype(config, mapping_rule, reference):
            integer_references.append(reference)

    return integer_references


def _preprocess_data(data, mapping_rule, references, config):
    # keep only reference columns in the dataframe
    data = data[list(references)]

    # deal with ORACLE
    if mapping_rule['source_type'] == RDB:
        if config.get_database_url(mapping_rule['source_name']).lower().startswith(ORACLE.lower()):
            data = normalize_oracle_identifier_casing(data, references)

    # remove NULLS for those data formats that do not allow to remove them at reading time
    if config.apply_na_filter():
        data = remove_null_values_from_dataframe(data, config)

    if mapping_rule['source_type'] == RDB:
        # deal with integers
        integer_references = _retrieve_rdb_integer_references(config, mapping_rule, references)
        data[integer_references] = data[integer_references].astype(float).astype(int)

    # data to str
    data = data.astype(str)

    return data


def _get_data(config, mapping_rule, references):
    if mapping_rule['source_type'] == RDB:
        data = get_sql_data(config, mapping_rule, references)
    elif mapping_rule['source_type'] in FILE_SOURCE_TYPES:
        data = get_file_data(mapping_rule, references)
    data = _preprocess_data(data, mapping_rule, references, config)

    return data


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
    # Curly braces that do not enclose column names MUST be escaped by a backslash character (“\”).
    # This also applies to curly braces within column names.
    template = template.replace('\\{', '{').replace('\\}', '}')

    if str(termtype).strip() == R2RML_IRI:
        results_df['triple'] = results_df['triple'] + '<'
    elif str(termtype).strip() == R2RML_LITERAL:
        results_df['triple'] = results_df['triple'] + '"'
    elif str(termtype).strip() == R2RML_BLANK_NODE:
        results_df['triple'] = results_df['triple'] + '_:'

    for reference in references:
        results_df['reference_results'] = results_df[columns_alias + reference]

        if config.only_write_printable_characters():
            results_df['reference_results'] = results_df['reference_results'].apply(lambda x: remove_non_printable_characters(x))

        if str(termtype).strip() == R2RML_IRI:
            if config.get_safe_percent_encoding():
                results_df['reference_results'] = results_df['reference_results'].apply(lambda x: quote(x, safe=config.get_safe_percent_encoding()))
            else:
                results_df['reference_results'] = results_df['reference_results'].apply(lambda x: encode_value(x))
        elif str(termtype).strip() == R2RML_LITERAL:
            results_df['reference_results'] = results_df['reference_results'].str.replace('\\', '\\\\', regex=False).str.replace('\n', '\\n', regex=False).str.replace('\t', '\\t', regex=False).str.replace('\b', '\\b', regex=False).str.replace('\f', '\\f', regex=False).str.replace('\r', '\\r', regex=False).str.replace('"', '\\"', regex=False).str.replace("'", "\\'", regex=False)

        splitted_template = template.split('{' + reference + '}')
        results_df['triple'] = results_df['triple'] + splitted_template[0] + results_df['reference_results']
        template = str('{' + reference + '}').join(splitted_template[1:])
    if template:
        # add what remains in the template after the last reference
        results_df['triple'] = results_df['triple'] + template

    if str(termtype).strip() == R2RML_IRI:
        results_df['triple'] = results_df['triple'] + '> '
    elif str(termtype).strip() == R2RML_LITERAL:
        results_df['triple'] = results_df['triple'] + '"'
        if pd.notna(language_tag):
            results_df['triple'] = results_df['triple'] + '@' + language_tag + ' '
        elif pd.notna(datatype):
            results_df['triple'] = results_df['triple'] + '^^<' + datatype + '> '
        else:
            results_df['triple'] = results_df['triple'] + ' '
    elif str(termtype).strip() == R2RML_BLANK_NODE:
        results_df['triple'] = results_df['triple'] + ' '

    return results_df


def _materialize_reference(results_df, reference, config, columns_alias='', termtype=R2RML_LITERAL,
                           language_tag='', datatype=''):
    results_df['reference_results'] = results_df[columns_alias + str(reference)]

    if config.only_write_printable_characters():
        results_df['reference_results'] = results_df['reference_results'].apply(lambda x: remove_non_printable_characters(x))

    if str(termtype).strip() == R2RML_LITERAL:
        results_df['reference_results'] = results_df['reference_results'].str.replace('\\', '\\\\', regex=False).str.replace('\n', '\\n', regex=False).str.replace('\t', '\\t', regex=False).str.replace('\b', '\\b', regex=False).str.replace('\f', '\\f', regex=False).str.replace('\r', '\\r', regex=False).str.replace('"', '\\"', regex=False).str.replace("'", "\\'", regex=False)
        results_df['triple'] = results_df['triple'] + '"' + results_df['reference_results'] + '"'
        if pd.notna(language_tag):
            results_df['triple'] = results_df['triple'] + '@' + language_tag + ' '
        elif pd.notna(datatype):
            results_df['triple'] = results_df['triple'] + '^^<' + datatype + '> '
        else:
            results_df['triple'] = results_df['triple'] + ' '
    elif str(termtype).strip() == R2RML_IRI:
        # it is assumed that the IRI values will be correct, and they are not percent encoded
        results_df['reference_results'] = results_df['reference_results'].apply(lambda x: x.strip())
        results_df['triple'] = results_df['triple'] + '<' + results_df['reference_results'] + '> '
    elif str(termtype).strip() == R2RML_BLANK_NODE:
        results_df['triple'] = results_df['triple'] + '_:' + results_df['reference_results'] + ' '

    return results_df


def _materialize_constant(results_df, constant, termtype=R2RML_IRI, language_tag='', datatype=''):
    complete_constant = ''
    if str(termtype).strip() == R2RML_IRI:
        complete_constant = '<' + str(constant) + '> '
    elif str(termtype).strip() == R2RML_LITERAL:
        complete_constant = '"' + constant + '"'

        if pd.notna(language_tag):
            complete_constant = complete_constant + '@' + language_tag + ' '
        elif pd.notna(datatype):
            complete_constant = complete_constant + '^^<' + datatype + '> '
        else:
            complete_constant = complete_constant + ' '
    elif str(termtype).strip() == R2RML_BLANK_NODE:
        complete_constant = '_:' + str(constant) + ' '

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
    if config.get_output_format() == NQUADS:
        if pd.notna(mapping_rule['graph_template']):
            results_df = _materialize_template(results_df, mapping_rule['graph_template'], config, columns_alias='child_')
        elif pd.notna(mapping_rule['graph_constant']) and str(mapping_rule['graph_constant']) != R2RML_DEFAULT_GRAPH:
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
        results_df = _materialize_template(results_df, mapping_rule['predicate_template'], config)
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
    if config.get_output_format() == NQUADS:
        if pd.notna(mapping_rule['graph_template']):
            results_df = _materialize_template(results_df, mapping_rule['graph_template'], config)
        elif pd.notna(mapping_rule['graph_constant']) and str(mapping_rule['graph_constant']) != R2RML_DEFAULT_GRAPH:
            results_df = _materialize_constant(results_df, mapping_rule['graph_constant'])
        elif pd.notna(mapping_rule['graph_reference']):
            results_df = _materialize_reference(results_df, mapping_rule['graph_reference'], config, termtype=R2RML_IRI)

    return set(results_df['triple'])


def _merge_data(data, parent_data, mapping_rule):
    child_join_references, parent_join_references = get_references_in_join_condition(mapping_rule)

    child_join_references = ['child_' + reference for reference in child_join_references]
    parent_join_references = ['parent_' + reference for reference in parent_join_references]

    return data.merge(parent_data, how='inner', left_on=child_join_references, right_on=parent_join_references)


def _materialize_mapping_rule(mapping_rule, subject_maps_df, config):
    references = _get_references_in_mapping_rule(mapping_rule)

    if pd.notna(mapping_rule['object_parent_triples_map']):
        parent_triples_map_rule = \
            subject_maps_df[subject_maps_df.triples_map_id == mapping_rule['object_parent_triples_map']].iloc[0]
        parent_references = _get_references_in_mapping_rule(parent_triples_map_rule, only_subject_map=True)

        # add references used in the join condition
        references, parent_references = _add_references_in_join_condition(mapping_rule, references, parent_references)

        data = _get_data(config, mapping_rule, references)
        data = data.add_prefix('child_')

        parent_data = _get_data(config, parent_triples_map_rule, parent_references)
        parent_data = parent_data.add_prefix('parent_')
        merged_data = _merge_data(data, parent_data, mapping_rule)

        triples = _materialize_join_mapping_rule_terms(merged_data, mapping_rule, parent_triples_map_rule, config)
    else:
        data = _get_data(config, mapping_rule, references)
        triples = _materialize_mapping_rule_terms(data, mapping_rule, config)

    return triples


def _materialize_mapping_partition(mapping_partition, subject_maps_df, config):
    triples = set()
    for i, mapping_rule in mapping_partition.iterrows():
        start_time = time.time()
        triples.update(set(_materialize_mapping_rule(mapping_rule, subject_maps_df, config)))

        logging.debug(f"{len(triples)} triples generated for mapping rule `{mapping_rule['id']}` "
                      f"in {get_delta_time(start_time)} seconds.")

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

        logging.info(f'Number of triples generated in total: {num_triples}.')

    def materialize_concurrently(self):
        logging.debug(f'Parallelizing with {self.config.get_number_of_processes()} cores.')

        pool = mp.Pool(self.config.get_number_of_processes())
        num_triples = sum(pool.starmap(_materialize_mapping_partition,
                                       zip(self.mapping_partitions, repeat(self.subject_maps_df), repeat(self.config))))
        pool.close()
        pool.join()

        logging.info(f'Number of triples generated in total: {num_triples}.')
