__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


from falcon.uri import encode_value
from urllib.parse import quote

from .utils import *
from .constants import *
from .data_source.relational_db import get_sql_data
from .data_source.property_graph_db import get_pg_data
from .data_source.data_file import get_file_data
from .data_source.python_data import get_ram_data
from .data_source.http_api import get_http_api_data
from .fnml.fnml_executer import execute_fnml

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

def _add_references_in_join_condition(rml_rule, references, parent_references):
    references_join, parent_references_join = get_references_in_join_condition(rml_rule, 'object_join_conditions')

    references.update(set(references_join))
    parent_references.update(set(parent_references_join))

    return references, parent_references


def _preprocess_data(data, rml_rule, references, config):
    # deal with ORACLE
    if rml_rule['source_type'] == RDB:
        if config.get_db_url(rml_rule['source_name']).lower().startswith(ORACLE.lower()):
            data = normalize_oracle_identifier_casing(data, references)

    # TODO: can this be removed?
    data = data.map(str)

    data = remove_null_values_from_dataframe(data, config, references)
    data = data.convert_dtypes(convert_boolean=False)

    # data to str
    data = data.astype(str)

    # remove duplicates
    data = data.drop_duplicates()

    return data


def _get_data(config, rml_rule, references, python_source=None):
    if rml_rule['source_type'] == RDB:
        data = get_sql_data(config, rml_rule, references)
    elif rml_rule['source_type'] == 'HTTPAPI':
        data = get_http_api_data(config, rml_rule, references)
    elif rml_rule['source_type'] == PGDB:
        data = get_pg_data(config, rml_rule, references)
    elif rml_rule['source_type'] in FILE_SOURCE_TYPES:
        data = get_file_data(rml_rule, references)
    elif rml_rule['source_type'] in IN_MEMORY_TYPES:
        data = get_ram_data(rml_rule, references, python_source)

    data = _preprocess_data(data, rml_rule, references, config)

    return data


def _get_references_in_rml_rule(rml_rule, rml_df, fnml_df, only_subject_map=False):
    references = []

    positions = ['subject'] if only_subject_map else ['subject', 'predicate', 'object', 'graph', 'lang_datatype']
    for position in positions:
        if rml_rule[f'{position}_map_type'] == RML_TEMPLATE:
            references.extend(get_references_in_template(rml_rule[f'{position}_map_value']))
        elif rml_rule[f'{position}_map_type'] == RML_REFERENCE:
            references.append(rml_rule[f'{position}_map_value'])
        elif rml_rule[f'{position}_map_type'] == RML_EXECUTION:
            references.extend(get_references_in_fnml_execution(fnml_df, rml_rule[f'{position}_map_value']))

    # term maps with join conditions (referencing and quoted)
    positions = ['subject'] if only_subject_map else ['subject', 'object']
    for position in positions:
        if rml_rule[f'{position}_map_type'] == RML_QUOTED_TRIPLES_MAP and pd.isna(rml_rule[f'{position}_join_conditions']):
            parent_rml_rule = get_rml_rule(rml_df, rml_rule[f'{position}_map_value'])
            references.extend(_get_references_in_rml_rule(parent_rml_rule, rml_df, fnml_df))

        references_join, parent_references_subject_join = get_references_in_join_condition(rml_rule, f'{position}_join_conditions')
        references.extend(references_join)

    return references


def _materialize_template(results_df, template, expression_type, config, position, columns_alias='', termtype='', datatype=''):
    if expression_type == RML_REFERENCE:
        # convert RML reference to template
        template = f'{{{template}}}'

    references = get_references_in_template(template)

    # Curly braces that do not enclose column names MUST be escaped by a backslash character (“\”).
    # This also applies to curly braces within column names.
    template = template.replace('\\{', '{').replace('\\}', '}')
    # formatting according to the termtype is done at the end
    results_df[position] = ''

    for reference in references:
        results_df['reference_results'] = results_df[columns_alias + reference]

        if config.only_write_printable_characters():
            results_df['reference_results'] = results_df['reference_results'].apply(
                lambda x: remove_non_printable_characters(x))

        if termtype.strip() == RML_IRI and expression_type == RML_TEMPLATE:
            if config.get_safe_percent_encoding():
                results_df['reference_results'] = results_df['reference_results'].apply(
                    lambda x: quote(x, safe=config.get_safe_percent_encoding()))
            else:
                results_df['reference_results'] = results_df['reference_results'].apply(lambda x: encode_value(x))
        elif termtype.strip() == RML_LITERAL:
            # Natural Mapping of SQL Values (https://www.w3.org/TR/r2rml/#natural-mapping)
            if datatype == XSD_BOOLEAN:
                results_df['reference_results'] = results_df['reference_results'].str.lower()
            elif datatype == XSD_DATETIME:
                results_df['reference_results'] = results_df['reference_results'].str.replace(' ', 'T', regex=False)
            elif datatype == XSD_INTEGER:
                # Make integers not end with .0
                results_df['reference_results'] = results_df['reference_results'].astype(float).astype(int).astype(str)

            # TODO: this can be avoided for most cases (if '\\' in data_value) | contains pandas method
            # see #321, ",\,\n,\r are always escaped
            results_df['reference_results'] = results_df['reference_results'].str.replace('"', '\\"', regex=False).str.replace('\\', '\\\\', regex=False).str.replace('\n', '\\n', regex=False).str.replace('\r', '\\r', regex=False)
            for char in config.get_literal_escaping_chars():
                if char not in ['"', '\n', '\\', '\r']:
                    if char in ['\n', '\r', '\t', '\b', '\f']:
                        results_df['reference_results'] = results_df['reference_results'].str.replace(char, f'\\{char}', regex=False)
                    else:
                        results_df['reference_results'] = results_df['reference_results'].str.replace(char, f'\\\\{char}', regex=False)

        splitted_template = template.split('{' + reference + '}')
        results_df[position] = results_df[position] + splitted_template[0] + results_df['reference_results']
        template = str('{' + reference + '}').join(splitted_template[1:])
    if template:
        # add what remains in the template after the last reference
        results_df[position] = results_df[position] + template

    if termtype.strip() == RML_IRI:
        results_df[position] = '<' + results_df[position] + '>'
    elif termtype.strip() == RML_BLANK_NODE:
        results_df[position] = '_:' + results_df[position]
    elif termtype.strip() == RML_LITERAL:
        results_df[position] = '"' + results_df[position] + '"'
    else:
        # this case is for language and datatype maps, do nothing
        pass

    return results_df


def _materialize_fnml_execution(results_df, fnml_execution, fnml_df, config, position, termtype=RML_LITERAL, datatype=''):
    results_df = execute_fnml(results_df, fnml_df, fnml_execution, config)

    results_df[fnml_execution] = results_df[fnml_execution].astype(str)

    if config.only_write_printable_characters():
        results_df[fnml_execution] = results_df[fnml_execution].apply(lambda x: remove_non_printable_characters(x))

    if termtype.strip() == RML_LITERAL:
        # Natural Mapping of SQL Values (https://www.w3.org/TR/r2rml/#natural-mapping)
        if datatype == XSD_BOOLEAN:
            results_df[fnml_execution] = results_df[fnml_execution].str.lower()
        elif datatype == XSD_DATETIME:
            results_df[fnml_execution] = results_df[fnml_execution].str.replace(' ', 'T', regex=False)
        # Make integers not end with .0
        elif datatype == XSD_INTEGER:
            results_df[fnml_execution] = results_df[fnml_execution].astype(float).astype(int).astype(str)

        results_df['reference_results'] = results_df['reference_results'].str.replace('"', '\\"', regex=False).str.replace('\\', '\\\\', regex=False).str.replace('\n', '\\n', regex=False).str.replace('\r', '\\r', regex=False)
        for char in config.get_literal_escaping_chars():
            if char not in ['"', '\n', '\\', '\r']:
                if char in ['\n', '\r', '\t', '\b', '\f']:
                    results_df['reference_results'] = results_df['reference_results'].str.replace(char, f'\\{char}', regex=False)
                else:
                    results_df['reference_results'] = results_df['reference_results'].str.replace(char, f'\\\\{char}', regex=False)

        results_df[position] = '"' + results_df[fnml_execution] + '"'
    elif termtype.strip() == RML_IRI:
        # it is assumed that the IRI values will be correct, and they are not percent encoded
        results_df[fnml_execution] = results_df[fnml_execution].apply(lambda x: x.strip())
        results_df[position] = '<' + results_df[fnml_execution] + '>'
    elif termtype.strip() == RML_BLANK_NODE:
        results_df[position] = '_:' + results_df[fnml_execution]

    return results_df


def _materialize_rml_rule_terms(results_df, rml_rule, fnml_df, config, columns_alias=''):
    if rml_rule['subject_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
        results_df = _materialize_template(results_df, rml_rule['subject_map_value'], rml_rule['subject_map_type'], config, 'subject',
                                           termtype=rml_rule['subject_termtype'])
    elif rml_rule['subject_map_type'] == RML_EXECUTION:
        results_df = _materialize_fnml_execution(results_df, rml_rule['subject_map_value'], fnml_df, config, 'subject',
                                                 termtype=rml_rule['subject_termtype'])
    if rml_rule['predicate_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
        results_df = _materialize_template(results_df, rml_rule['predicate_map_value'], rml_rule['predicate_map_type'], config, 'predicate', termtype=RML_IRI)
    elif rml_rule['predicate_map_type'] == RML_EXECUTION:
        results_df = _materialize_fnml_execution(results_df, rml_rule['predicate_map_value'], fnml_df, config,
                                                 'predicate', termtype=RML_IRI)
    if rml_rule['object_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
        results_df = _materialize_template(results_df, rml_rule['object_map_value'], rml_rule['object_map_type'], config, 'object',
                                           columns_alias=columns_alias, termtype=rml_rule['object_termtype'], datatype=rml_rule['lang_datatype_map_value'])
    elif rml_rule['object_map_type'] == RML_EXECUTION:
        results_df = _materialize_fnml_execution(results_df, rml_rule['object_map_value'], fnml_df, config, 'object',
                                                 termtype=rml_rule['object_termtype'], datatype=rml_rule['lang_datatype_map_value'])

    if rml_rule['lang_datatype'] == RML_LANGUAGE_MAP:
        if rml_rule['lang_datatype_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
            results_df = _materialize_template(results_df, rml_rule['lang_datatype_map_value'], rml_rule['lang_datatype_map_type'],
                                               config, 'lang_datatype')
        elif rml_rule['lang_datatype_map_type'] == RML_EXECUTION:
            results_df = _materialize_fnml_execution(results_df, rml_rule['lang_datatype_map_value'], fnml_df, config,
                                                     'lang_datatype')
        results_df['object'] = results_df['object'] + '@' + results_df['lang_datatype']
    elif rml_rule['lang_datatype'] == RML_DATATYPE_MAP:
        if rml_rule['lang_datatype_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE]:
            results_df = _materialize_template(results_df, rml_rule['lang_datatype_map_value'], rml_rule['lang_datatype_map_type'],
                                               config, 'lang_datatype', termtype=RML_IRI)
        elif rml_rule['lang_datatype_map_type'] == RML_EXECUTION:
            results_df = _materialize_fnml_execution(results_df, rml_rule['lang_datatype_map_value'], fnml_df, config,
                                                     'lang_datatype', termtype=RML_IRI)
        results_df['object'] = results_df['object'] + '^^' + results_df['lang_datatype']

    return results_df


def _merge_data(data, parent_data, rml_rule, join_condition):
    parent_data = parent_data.add_prefix('parent_')
    child_join_references, parent_join_references = get_references_in_join_condition(rml_rule, join_condition)
    parent_join_references = ['parent_' + reference for reference in parent_join_references]

    # if there is only one join condition use join, otherwise use merge
    if len(child_join_references) == 1:
        data = data.set_index(child_join_references, drop=False)
        parent_data = parent_data.set_index(parent_join_references, drop=False)
        return data.join(parent_data, how='inner')
    else:
        return data.merge(parent_data, how='inner', left_on=child_join_references, right_on=parent_join_references)


def _materialize_rml_rule(rml_rule, rml_df, fnml_df, config, data=None, parent_join_references=set(), nest_level=0,
                          python_source=None):
    references = set(_get_references_in_rml_rule(rml_rule, rml_df, fnml_df))

    references_subject_join, parent_references_subject_join = get_references_in_join_condition(rml_rule, 'subject_join_conditions')
    references_object_join, parent_references_object_join = get_references_in_join_condition(rml_rule, 'object_join_conditions')
    references.update(parent_join_references)

    # handle the case in which all term maps are constant-valued
    if rml_rule['subject_map_type'] == RML_CONSTANT and rml_rule['predicate_map_type'] == RML_CONSTANT and rml_rule['object_map_type'] == RML_CONSTANT and rml_rule['graph_map_type'] == RML_CONSTANT:
        # create a dataframe with 1 row
        data = pd.DataFrame({'placeholder': ['placeholder']})
        data = _materialize_rml_rule_terms(data, rml_rule, fnml_df, config)

    elif rml_rule['subject_map_type'] == RML_QUOTED_TRIPLES_MAP or rml_rule['object_map_type'] == RML_QUOTED_TRIPLES_MAP:
        if data is None:
            data = _get_data(config, rml_rule, references, python_source)

        if rml_rule['subject_map_type'] == RML_QUOTED_TRIPLES_MAP:
            if pd.notna(rml_rule['subject_join_conditions']):
                references.update(references_subject_join)
                parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['subject_map_value'])
                parent_data = _materialize_rml_rule(parent_triples_map_rule, rml_df, fnml_df, config,
                                                    parent_join_references=parent_references_subject_join,
                                                    nest_level=nest_level + 1)
                data = _merge_data(data, parent_data, rml_rule, 'subject_join_conditions')
                data['subject'] = '<< ' + data['parent_triple'] + ' >>'
                data = data.drop(columns=['parent_triple'])
            else:
                parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['subject_map_value'])
                data = _materialize_rml_rule(parent_triples_map_rule, rml_df, fnml_df, config, data=data,
                                             nest_level=nest_level + 1)
                data['subject'] = '<< ' + data['triple'] + ' >>'
            data['keep_subject' + str(nest_level)] = data['subject']
        if rml_rule['object_map_type'] == RML_QUOTED_TRIPLES_MAP:
            if pd.notna(rml_rule['object_join_conditions']):
                references.update(references_object_join)
                parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['object_map_value'])
                parent_data = _materialize_rml_rule(parent_triples_map_rule, rml_df, fnml_df, config,
                                                    parent_join_references=parent_references_object_join,
                                                    nest_level=nest_level + 1)
                data = _merge_data(data, parent_data, rml_rule, 'object_join_conditions')
                data['object'] = '<< ' + data['parent_triple'] + ' >>'
                data = data.drop(columns=['parent_triple'])
            else:
                parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['object_map_value'])
                data = _materialize_rml_rule(parent_triples_map_rule, rml_df, fnml_df, config, data=data,
                                             nest_level=nest_level + 1)
                data['object'] = '<< ' + data['triple'] + ' >>'
            if rml_rule['subject_map_type'] == RML_QUOTED_TRIPLES_MAP:
                data['subject'] = data['keep_subject' + str(nest_level)]

        data = _materialize_rml_rule_terms(data, rml_rule, fnml_df, config)

    # elif pd.notna(rml_rule['object_parent_triples_map']):
    elif rml_rule['object_map_type'] == RML_PARENT_TRIPLES_MAP:

        references.update(references_object_join)
        # parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['object_parent_triples_map'])
        parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['object_map_value'])
        parent_references = set(
            _get_references_in_rml_rule(parent_triples_map_rule, rml_df, fnml_df, only_subject_map=True))

        # add references used in the join condition
        references, parent_references = _add_references_in_join_condition(rml_rule, references, parent_references)

        if data is None:
            data = _get_data(config, rml_rule, references, python_source)

        parent_data = _get_data(config, parent_triples_map_rule, parent_references, python_source)
        merged_data = _merge_data(data, parent_data, rml_rule, 'object_join_conditions')

        rml_rule['object_map_type'] = parent_triples_map_rule['subject_map_type']
        rml_rule['object_map_value'] = parent_triples_map_rule['subject_map_value']

        data = _materialize_rml_rule_terms(merged_data, rml_rule, fnml_df, config, columns_alias='parent_')
    else:

        if data is None:
            data = _get_data(config, rml_rule, references, python_source)

        data = _materialize_rml_rule_terms(data, rml_rule, fnml_df, config)

    # TODO: this is slow reduce the number of vectorized operations
    data['triple'] = data['subject'] + ' ' + data['predicate'] + ' ' + data['object']

    if nest_level == 0 and config.get_output_format() == NQUADS:
        if rml_rule['graph_map_type'] in [RML_TEMPLATE, RML_CONSTANT, RML_REFERENCE] and rml_rule['graph_map_value'] != RML_DEFAULT_GRAPH:
            data = _materialize_template(data, rml_rule['graph_map_value'], rml_rule['graph_map_type'], config, 'graph', termtype=RML_IRI)
        elif rml_rule['graph_map_type'] == RML_EXECUTION:
            data = _materialize_fnml_execution(data, rml_rule['graph_map_value'], fnml_df, config, 'graph', termtype=RML_IRI)
        else:
            data['graph'] = ''
        data['triple'] = data['triple'] + ' ' + data['graph']

    data = data.drop(columns=['subject', 'predicate', 'object'], errors='ignore')

    return data


def _materialize_mapping_group_to_set(mapping_group_df, rml_df, fnml_df, config, python_source=None):
    triples = set()
    for i, rml_rule in mapping_group_df.iterrows():
        data = _materialize_rml_rule(rml_rule, rml_df, fnml_df, config, python_source=python_source)
        triples.update(set(data['triple']))

    return triples


def _materialize_mapping_group_to_file(mapping_group_df, rml_df, fnml_df, config):
    triples = set()
    for i, rml_rule in mapping_group_df.iterrows():
        start_time = time.time()
        data = _materialize_rml_rule(rml_rule, rml_df, fnml_df, config)
        triples.update(set(data['triple']))

        LOGGER.debug(f"{len(triples)} triples generated for mapping rule `{rml_rule['triples_map_id']}` "
                      f"in {get_delta_time(start_time)} seconds.")

    triples_to_file(triples, config, mapping_group_df.iloc[0]['mapping_partition'])

    return len(triples)


def _materialize_mapping_group_to_kafka(mapping_group_df, rml_df, fnml_df, config, python_source=None):
    triples = set()
    for i, rml_rule in mapping_group_df.iterrows():
        start_time = time.time()
        data = _materialize_rml_rule(rml_rule, rml_df, fnml_df, config, python_source=python_source)
        triples.update(set(data['triple']))

        LOGGER.debug(f"{len(triples)} triples generated for mapping rule `{rml_rule['triples_map_id']}` "
                      f"in {get_delta_time(start_time)} seconds.")

    triples_to_kafka(triples, config)

    return len(triples)
