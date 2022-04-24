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
    references_join, parent_references_join = get_references_in_join_condition(mapping_rule, 'object_join_conditions')

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
    # deal with ORACLE
    if mapping_rule['source_type'] == RDB:
        if config.get_database_url(mapping_rule['source_name']).lower().startswith(ORACLE.lower()):
            data = normalize_oracle_identifier_casing(data, references)

    data = remove_null_values_from_dataframe(data, config, references)

    if mapping_rule['source_type'] == RDB:
        # deal with integers
        integer_references = _retrieve_rdb_integer_references(config, mapping_rule, references)
        data[integer_references] = data[integer_references].astype(float).astype(int)

    # data to str
    data = data.astype(str)

    # remove duplicates
    data = data.drop_duplicates()

    return data


def _get_data(config, mapping_rule, references):
    if mapping_rule['source_type'] == RDB:
        data = get_sql_data(config, mapping_rule, references)
    elif mapping_rule['source_type'] in FILE_SOURCE_TYPES:
        data = get_file_data(mapping_rule, references)

    data = _preprocess_data(data, mapping_rule, references, config)

    return data


def _get_references_in_mapping_rule(mapping_rule, mappings_df, only_subject_map=False):
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

    if pd.notna(mapping_rule['subject_quoted']) and pd.isna(mapping_rule['subject_join_conditions']):
        parent_mapping_rule = get_mapping_rule(mappings_df, mapping_rule['subject_quoted'])
        references.extend(_get_references_in_mapping_rule(parent_mapping_rule, mappings_df))
    if pd.notna(mapping_rule['object_quoted']) and pd.isna(mapping_rule['object_join_conditions']):
        parent_mapping_rule = get_mapping_rule(mappings_df, mapping_rule['object_quoted'])
        references.extend(_get_references_in_mapping_rule(parent_mapping_rule, mappings_df))
    references_subject_join, parent_references_subject_join = get_references_in_join_condition(mapping_rule, 'subject_join_conditions')
    references.extend(references_subject_join)
    references_object_join, parent_references_object_join = get_references_in_join_condition(mapping_rule, 'object_join_conditions')
    references.extend(references_object_join)

    return references


def _materialize_template(results_df, template, config, position, columns_alias='', termtype=R2RML_IRI, language_tag='', datatype=''):
    references = get_references_in_template(str(template))
    # Curly braces that do not enclose column names MUST be escaped by a backslash character (“\”).
    # This also applies to curly braces within column names.
    template = template.replace('\\{', '{').replace('\\}', '}')

    if str(termtype).strip() == R2RML_IRI:
        results_df[position] = '<'
    elif str(termtype).strip() == R2RML_LITERAL:
        results_df[position] = '"'
    elif str(termtype).strip() == R2RML_BLANK_NODE:
        results_df[position] = '_:'

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
        results_df[position] = results_df[position] + splitted_template[0] + results_df['reference_results']
        template = str('{' + reference + '}').join(splitted_template[1:])
    if template:
        # add what remains in the template after the last reference
        results_df[position] = results_df[position] + template

    if str(termtype).strip() == R2RML_IRI:
        results_df[position] = results_df[position] + '>'
    elif str(termtype).strip() == R2RML_LITERAL:
        results_df[position] = results_df[position] + '"'
        if pd.notna(language_tag):
            results_df[position] = results_df[position] + '@' + language_tag
        elif pd.notna(datatype):
            results_df[position] = results_df[position] + '^^<' + datatype + '>'
        else:
            results_df[position] = results_df[position]
    elif str(termtype).strip() == R2RML_BLANK_NODE:
        results_df[position] = results_df[position]

    return results_df


def _materialize_reference(results_df, reference, config, position, columns_alias='', termtype=R2RML_LITERAL, language_tag='', datatype=''):
    results_df['reference_results'] = results_df[columns_alias + str(reference)]

    if config.only_write_printable_characters():
        results_df['reference_results'] = results_df['reference_results'].apply(lambda x: remove_non_printable_characters(x))

    if str(termtype).strip() == R2RML_LITERAL:
        results_df['reference_results'] = results_df['reference_results'].str.replace('\\', '\\\\', regex=False).str.replace('\n', '\\n', regex=False).str.replace('\t', '\\t', regex=False).str.replace('\b', '\\b', regex=False).str.replace('\f', '\\f', regex=False).str.replace('\r', '\\r', regex=False).str.replace('"', '\\"', regex=False).str.replace("'", "\\'", regex=False)
        results_df[position] = '"' + results_df['reference_results'] + '"'
        if pd.notna(language_tag):
            results_df[position] = results_df[position] + '@' + language_tag
        elif pd.notna(datatype):
            results_df[position] = results_df[position] + '^^<' + datatype + '>'
        else:
            results_df[position] = results_df[position]
    elif str(termtype).strip() == R2RML_IRI:
        # it is assumed that the IRI values will be correct, and they are not percent encoded
        results_df['reference_results'] = results_df['reference_results'].apply(lambda x: x.strip())
        results_df[position] = '<' + results_df['reference_results'] + '>'
    elif str(termtype).strip() == R2RML_BLANK_NODE:
        results_df[position] = '_:' + results_df['reference_results']

    return results_df


def _materialize_constant(results_df, constant, position, termtype=R2RML_IRI, language_tag='', datatype=''):
    complete_constant = ''
    if str(termtype).strip() == R2RML_IRI:
        complete_constant = '<' + str(constant) + '>'
    elif str(termtype).strip() == R2RML_LITERAL:
        complete_constant = '"' + constant + '"'

        if pd.notna(language_tag):
            complete_constant = complete_constant + '@' + language_tag
        elif pd.notna(datatype):
            complete_constant = complete_constant + '^^<' + datatype + '>'
        else:
            complete_constant = complete_constant
    elif str(termtype).strip() == R2RML_BLANK_NODE:
        complete_constant = '_:' + str(constant)

    results_df[position] = complete_constant

    return results_df


def _materialize_join_mapping_rule_terms(results_df, mapping_rule, parent_triples_map_rule, config):
    if pd.notna(mapping_rule['subject_template']):
        results_df = _materialize_template(results_df, mapping_rule['subject_template'], config, 'subject', termtype=mapping_rule['subject_termtype'])
    elif pd.notna(mapping_rule['subject_constant']):
        results_df = _materialize_constant(results_df, mapping_rule['subject_constant'], 'subject', termtype=mapping_rule['subject_termtype'])
    elif pd.notna(mapping_rule['subject_reference']):
        results_df = _materialize_reference(results_df, mapping_rule['subject_reference'], config, 'subject', termtype=mapping_rule['subject_termtype'])
    if pd.notna(mapping_rule['predicate_template']):
        results_df = _materialize_template(results_df, mapping_rule['predicate_template'], config, 'predicate')
    elif pd.notna(mapping_rule['predicate_constant']):
        results_df = _materialize_constant(results_df, mapping_rule['predicate_constant'], 'predicate')
    elif pd.notna(mapping_rule['predicate_reference']):
        results_df = _materialize_reference(results_df, mapping_rule['predicate_reference'], config, 'predicate', termtype=R2RML_IRI)
    if pd.notna(parent_triples_map_rule['subject_template']):
        results_df = _materialize_template(results_df, parent_triples_map_rule['subject_template'], config, 'object', termtype=parent_triples_map_rule['subject_termtype'], columns_alias='parent_')
    elif pd.notna(parent_triples_map_rule['subject_constant']):
        results_df = _materialize_constant(results_df, parent_triples_map_rule['subject_constant'], 'object', termtype=parent_triples_map_rule['subject_termtype'])
    elif pd.notna(parent_triples_map_rule['subject_reference']):
        results_df = _materialize_reference(results_df, parent_triples_map_rule['subject_reference'], config, 'object', termtype=parent_triples_map_rule['subject_termtype'], columns_alias='parent_')

    return results_df


def _materialize_mapping_rule_terms(results_df, mapping_rule, config):
    if pd.notna(mapping_rule['subject_template']):
        results_df = _materialize_template(results_df, mapping_rule['subject_template'], config, 'subject', termtype=mapping_rule['subject_termtype'])
    elif pd.notna(mapping_rule['subject_constant']):
        results_df = _materialize_constant(results_df, mapping_rule['subject_constant'], 'subject', termtype=mapping_rule['subject_termtype'])
    elif pd.notna(mapping_rule['subject_reference']):
        results_df = _materialize_reference(results_df, mapping_rule['subject_reference'], config, 'subject', termtype=mapping_rule['subject_termtype'])
    if pd.notna(mapping_rule['predicate_template']):
        results_df = _materialize_template(results_df, mapping_rule['predicate_template'], config, 'predicate')
    elif pd.notna(mapping_rule['predicate_constant']):
        results_df = _materialize_constant(results_df, mapping_rule['predicate_constant'], 'predicate')
    elif pd.notna(mapping_rule['predicate_reference']):
        results_df = _materialize_reference(results_df, mapping_rule['predicate_reference'], config, 'predicate', termtype=R2RML_IRI)
    if pd.notna(mapping_rule['object_template']):
        results_df = _materialize_template(results_df, mapping_rule['object_template'], config, 'object', termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])
    elif pd.notna(mapping_rule['object_constant']):
        results_df = _materialize_constant(results_df, mapping_rule['object_constant'], 'object', termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])
    elif pd.notna(mapping_rule['object_reference']):
        results_df = _materialize_reference(results_df, mapping_rule['object_reference'], config, 'object', termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])

    return results_df


def _merge_data(data, parent_data, mapping_rule, join_condition):
    parent_data = parent_data.add_prefix('parent_')
    child_join_references, parent_join_references = get_references_in_join_condition(mapping_rule, join_condition)
    parent_join_references = ['parent_' + reference for reference in parent_join_references]

    data = data.set_index(child_join_references, drop=False)
    parent_data = parent_data.set_index(parent_join_references, drop=False)

    return data.join(parent_data, how='inner')


def _materialize_mapping_rule(mapping_rule, mappings_df, config, data=None, parent_join_references=set(), nest_level=0):
    references = set(_get_references_in_mapping_rule(mapping_rule, mappings_df))

    references_subject_join, parent_references_subject_join = get_references_in_join_condition(mapping_rule, 'subject_join_conditions')
    references_object_join, parent_references_object_join = get_references_in_join_condition(mapping_rule, 'object_join_conditions')
    references.update(parent_join_references)

    if pd.notna(mapping_rule['subject_quoted']) or pd.notna(mapping_rule['object_quoted']):
        if data is None:
            data = _get_data(config, mapping_rule, references)

        if pd.notna(mapping_rule['subject_quoted']):
            if pd.notna(mapping_rule['subject_join_conditions']):
                references.update(references_subject_join)
                parent_triples_map_rule = get_mapping_rule(mappings_df, mapping_rule['subject_quoted'])
                parent_data = _materialize_mapping_rule(parent_triples_map_rule, mappings_df, config, parent_join_references=parent_references_subject_join, nest_level=nest_level + 1)
                data = _merge_data(data, parent_data, mapping_rule, 'subject_join_conditions')
                data['subject'] = '<< ' + data['parent_triple'] + ' >>'
                data = data.drop(columns=['parent_triple'])
            else:
                parent_triples_map_rule = get_mapping_rule(mappings_df, mapping_rule['subject_quoted'])
                data = _materialize_mapping_rule(parent_triples_map_rule, mappings_df, config, data=data, nest_level=nest_level + 1)
                data['subject'] = '<< ' + data['triple'] + ' >>'
            data['keep_subject'+str(nest_level)] = data['subject']
        if pd.notna(mapping_rule['object_quoted']):
            if pd.notna(mapping_rule['object_join_conditions']):
                references.update(references_object_join)
                parent_triples_map_rule = get_mapping_rule(mappings_df, mapping_rule['object_quoted'])
                parent_data = _materialize_mapping_rule(parent_triples_map_rule, mappings_df, config, parent_join_references=parent_references_object_join, nest_level=nest_level + 1)
                data = _merge_data(data, parent_data, mapping_rule, 'object_join_conditions')
                data['object'] = '<< ' + data['parent_triple'] + ' >>'
                data = data.drop(columns=['parent_triple'])
            else:
                parent_triples_map_rule = get_mapping_rule(mappings_df, mapping_rule['object_quoted'])
                data = _materialize_mapping_rule(parent_triples_map_rule, mappings_df, config, data=data, nest_level=nest_level + 1)
                data['object'] = '<< ' + data['triple'] + ' >>'
            if pd.notna(mapping_rule['subject_quoted']):
                data['subject'] = data['keep_subject'+str(nest_level)]

        if pd.notna(mapping_rule['subject_template']):
            data = _materialize_template(data, mapping_rule['subject_template'], config, 'subject', termtype=mapping_rule['subject_termtype'])
        elif pd.notna(mapping_rule['subject_constant']):
            data = _materialize_constant(data, mapping_rule['subject_constant'], 'subject', termtype=mapping_rule['subject_termtype'])
        elif pd.notna(mapping_rule['subject_reference']):
            data = _materialize_reference(data, mapping_rule['subject_reference'], config, 'subject', termtype=mapping_rule['subject_termtype'])

        if pd.notna(mapping_rule['object_template']):
            data = _materialize_template(data, mapping_rule['object_template'], config, 'object', termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])
        elif pd.notna(mapping_rule['object_constant']):
            data = _materialize_constant(data, mapping_rule['object_constant'], 'object', termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])
        elif pd.notna(mapping_rule['object_reference']):
            data = _materialize_reference(data, mapping_rule['object_reference'], config, 'object', termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])

        if pd.notna(mapping_rule['predicate_template']):
            data = _materialize_template(data, mapping_rule['predicate_template'], config, 'predicate')
        elif pd.notna(mapping_rule['predicate_constant']):
            data = _materialize_constant(data, mapping_rule['predicate_constant'], 'predicate')
        elif pd.notna(mapping_rule['predicate_reference']):
            data = _materialize_reference(data, mapping_rule['predicate_reference'], config, 'predicate', termtype=R2RML_IRI)

    elif pd.notna(mapping_rule['object_parent_triples_map']):
        references.update(references_object_join)
        parent_triples_map_rule = get_mapping_rule(mappings_df, mapping_rule['object_parent_triples_map'])
        parent_references = set(_get_references_in_mapping_rule(parent_triples_map_rule, mappings_df, only_subject_map=True))

        # add references used in the join condition
        references, parent_references = _add_references_in_join_condition(mapping_rule, references, parent_references)

        if data is None:
            data = _get_data(config, mapping_rule, references)
        parent_data = _get_data(config, parent_triples_map_rule, parent_references)
        merged_data = _merge_data(data, parent_data, mapping_rule, 'object_join_conditions')

        data = _materialize_join_mapping_rule_terms(merged_data, mapping_rule, parent_triples_map_rule, config)
    else:
        if data is None:
            data = _get_data(config, mapping_rule, references)
        data = _materialize_mapping_rule_terms(data, mapping_rule, config)

    # TODO: this is slow reduce the number of vectorized operations
    data['triple'] = data['subject'] + ' ' + data['predicate'] + ' ' + data['object']

    if nest_level == 0 and config.get_output_format() == NQUADS:
        if pd.notna(mapping_rule['graph_template']):
            data = _materialize_template(data, mapping_rule['graph_template'], config, 'graph')
        elif pd.notna(mapping_rule['graph_constant']) and str(mapping_rule['graph_constant']) != R2RML_DEFAULT_GRAPH:
            data = _materialize_constant(data, mapping_rule['graph_constant'], 'graph')
        elif pd.notna(mapping_rule['graph_reference']):
            data = _materialize_reference(data, mapping_rule['graph_reference'], config, 'graph', termtype=R2RML_IRI)
        else:
            data['graph'] = ''
        data['triple'] = data['triple'] + ' ' + data['graph']

    data = data.drop(columns=['subject', 'predicate', 'object'], errors='ignore')

    return data


def _materialize_mapping_partition(mapping_partition_df, mappings_df, config):
    triples = set()
    for i, mapping_rule in mapping_partition_df.iterrows():
        start_time = time.time()
        data = _materialize_mapping_rule(mapping_rule, mappings_df, config)
        triples.update(set(data['triple']))

        logging.debug(f"{len(triples)} triples generated for mapping rule `{mapping_rule['id']}` "
                      f"in {get_delta_time(start_time)} seconds.")

    triples_to_file(triples, config, mapping_partition_df.iloc[0]['mapping_partition'])

    return len(triples)


class Materializer:

    def __init__(self, mappings_df, config):
        self.config = config
        self.mappings_df = mappings_df

        # keep only asserted mapping rules
        asserted_mapping_df = mappings_df.loc[mappings_df['triples_map_type'] == R2RML_TRIPLES_MAP_CLASS]
        self.mapping_partitions = [group for _, group in asserted_mapping_df.groupby(by='mapping_partition')]

        remove_output_files(config, self.mappings_df)

    def __str__(self):
        return str(self.mappings_df)

    def __repr__(self):
        return repr(self.mappings_df)

    def __len__(self):
        return len(self.mappings_df)

    def materialize(self):
        num_triples = 0
        for mapping_partition in self.mapping_partitions:
            num_triples += _materialize_mapping_partition(mapping_partition, self.mappings_df, self.config)

        logging.info(f'Number of triples generated in total: {num_triples}.')

    def materialize_concurrently(self):
        logging.debug(f'Parallelizing with {self.config.get_number_of_processes()} cores.')

        pool = mp.Pool(self.config.get_number_of_processes())
        num_triples = sum(pool.starmap(_materialize_mapping_partition,
                                       zip(self.mapping_partitions, repeat(self.mappings_df), repeat(self.config))))
        pool.close()
        pool.join()

        logging.info(f'Number of triples generated in total: {num_triples}.')
