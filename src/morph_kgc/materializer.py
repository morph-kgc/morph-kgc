__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


from falcon.uri import encode_value
from urllib.parse import quote

from .utils import *
from .constants import *
from .data_source.relational_database import get_sql_data
from .data_source.data_file import get_file_data
from .fno.fno_executer import execute_fno


def _add_references_in_join_condition(rml_rule, references, parent_references):
    references_join, parent_references_join = get_references_in_join_condition(rml_rule, 'object_join_conditions')

    references.update(set(references_join))
    parent_references.update(set(parent_references_join))

    return references, parent_references


def _preprocess_data(data, rml_rule, references, config):
    # deal with ORACLE
    if rml_rule['source_type'] == RDB:
        if config.get_database_url(rml_rule['source_name']).lower().startswith(ORACLE.lower()):
            data = normalize_oracle_identifier_casing(data, references)

    data = remove_null_values_from_dataframe(data, config, references)
    data = data.convert_dtypes(convert_boolean=False)

    # data to str
    data = data.astype(str)

    # remove duplicates
    data = data.drop_duplicates()

    return data


def _get_data(config, rml_rule, references):

    if rml_rule['source_type'] == RDB:
        data = get_sql_data(config, rml_rule, references)
    elif rml_rule['source_type'] in FILE_SOURCE_TYPES:
        data = get_file_data(rml_rule, references)

    data = _preprocess_data(data, rml_rule, references, config)

    return data


def _get_references_in_rml_rule(rml_rule, rml_df, fno_df, only_subject_map=False):
    references = []
    if rml_rule['subject_map_type'] == R2RML_TEMPLATE:
        references.extend(get_references_in_template(rml_rule['subject_map_value']))
    elif rml_rule['subject_map_type'] == RML_REFERENCE:
        references.append(rml_rule['subject_map_value'])
    elif rml_rule['subject_map_type'] == FNML_EXECUTION:
        references.append(rml_rule['subject_map_value'])

    if not only_subject_map:
        if rml_rule['predicate_map_type'] == R2RML_TEMPLATE:
            references.extend(get_references_in_template(rml_rule['predicate_map_value']))
        elif rml_rule['predicate_map_type'] == RML_REFERENCE:
            references.append(rml_rule['predicate_map_value'])
        if rml_rule['object_map_type'] == R2RML_TEMPLATE:
            references.extend(get_references_in_template(rml_rule['object_map_value']))
        elif rml_rule['object_map_type'] == RML_REFERENCE:
            references.append(rml_rule['object_map_value'])
        if rml_rule['graph_map_type'] == R2RML_TEMPLATE:
            references.extend(get_references_in_template(rml_rule['graph_map_value']))
        elif rml_rule['graph_map_type'] == RML_REFERENCE:
            references.append(rml_rule['graph_map_value'])

    if rml_rule['subject_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP and pd.isna(rml_rule['subject_join_conditions']):
        parent_rml_rule = get_rml_rule(rml_df, rml_rule['subject_map_value'])
        references.extend(_get_references_in_rml_rule(parent_rml_rule, rml_df, fno_df))
    if rml_rule['object_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP and pd.isna(rml_rule['object_join_conditions']):
        parent_rml_rule = get_rml_rule(rml_df, rml_rule['object_map_value'])
        references.extend(_get_references_in_rml_rule(parent_rml_rule, rml_df, fno_df))

    references_subject_join, parent_references_subject_join = get_references_in_join_condition(rml_rule, 'subject_join_conditions')
    references.extend(references_subject_join)
    references_object_join, parent_references_object_join = get_references_in_join_condition(rml_rule, 'object_join_conditions')
    references.extend(references_object_join)

    # extract FnO references
    if len(fno_df):
        for position in ['subject', 'predicate', 'object', 'graph']:
            if rml_rule[f'{position}_map_type'] == FNML_EXECUTION:
                references.extend(get_references_in_fno_execution(fno_df, rml_rule[f'{position}_map_value']))

    return references


def _materialize_template(results_df, template, config, position, columns_alias='', termtype=R2RML_IRI, language_tag='',
                          datatype=''):
    references = get_references_in_template(template)

    # Curly braces that do not enclose column names MUST be escaped by a backslash character (“\”).
    # This also applies to curly braces within column names.
    template = template.replace('\\{', '{').replace('\\}', '}')

    if termtype.strip() == R2RML_IRI:
        results_df[position] = '<'
    elif termtype.strip() == R2RML_LITERAL:
        results_df[position] = '"'
    elif termtype.strip() == R2RML_BLANK_NODE:
        results_df[position] = '_:'

    for reference in references:
        results_df['reference_results'] = results_df[columns_alias + reference]

        if config.only_write_printable_characters():
            results_df['reference_results'] = results_df['reference_results'].apply(lambda x: remove_non_printable_characters(x))

        if termtype.strip() == R2RML_IRI:
            if config.get_safe_percent_encoding():
                results_df['reference_results'] = results_df['reference_results'].apply(lambda x: quote(x, safe=config.get_safe_percent_encoding()))
            else:
                results_df['reference_results'] = results_df['reference_results'].apply(lambda x: encode_value(x))
        elif termtype.strip() == R2RML_LITERAL:
            results_df['reference_results'] = results_df['reference_results'].str.replace('\\', '\\\\', regex=False).str.replace('\n', '\\n', regex=False).str.replace('\t', '\\t', regex=False).str.replace('\b', '\\b', regex=False).str.replace('\f', '\\f', regex=False).str.replace('\r', '\\r', regex=False).str.replace('"', '\\"', regex=False).str.replace("'", "\\'", regex=False)

        splitted_template = template.split('{' + reference + '}')
        results_df[position] = results_df[position] + splitted_template[0] + results_df['reference_results']
        template = str('{' + reference + '}').join(splitted_template[1:])
    if template:
        # add what remains in the template after the last reference
        results_df[position] = results_df[position] + template

    if termtype.strip() == R2RML_IRI:
        results_df[position] = results_df[position] + '>'
    elif termtype.strip() == R2RML_LITERAL:
        results_df[position] = results_df[position] + '"'
        if pd.notna(language_tag):
            results_df[position] = results_df[position] + '@' + language_tag
        elif pd.notna(datatype):
            results_df[position] = results_df[position] + '^^<' + datatype + '>'
        else:
            results_df[position] = results_df[position]
    elif termtype.strip() == R2RML_BLANK_NODE:
        results_df[position] = results_df[position]

    return results_df


def _materialize_reference(results_df, reference, config, position, columns_alias='', termtype=R2RML_LITERAL, language_tag='', datatype=''):
    results_df['reference_results'] = results_df[columns_alias + reference]

    if config.only_write_printable_characters():
        results_df['reference_results'] = results_df['reference_results'].apply(lambda x: remove_non_printable_characters(x))

    if termtype.strip() == R2RML_LITERAL:
        # Natural Mapping of SQL Values (https://www.w3.org/TR/r2rml/#natural-mapping)
        if datatype == XSD_BOOLEAN:
            results_df['reference_results'] = results_df['reference_results'].str.lower()
        elif datatype == XSD_DATETIME:
            results_df['reference_results'] = results_df['reference_results'].str.replace(' ', 'T', regex=False)
        # Make integers not end with .0
        elif datatype == XSD_INTEGER:
            results_df['reference_results'] = results_df['reference_results'].astype(float).astype(int).astype(str)

        results_df['reference_results'] = results_df['reference_results'].str.replace('\\', '\\\\', regex=False).str.replace('\n', '\\n', regex=False).str.replace('\t', '\\t', regex=False).str.replace('\b', '\\b', regex=False).str.replace('\f', '\\f', regex=False).str.replace('\r', '\\r', regex=False).str.replace('"', '\\"', regex=False).str.replace("'", "\\'", regex=False)
        results_df[position] = '"' + results_df['reference_results'] + '"'
        if pd.notna(language_tag):
            results_df[position] = results_df[position] + '@' + language_tag
        elif pd.notna(datatype):
            results_df[position] = results_df[position] + '^^<' + datatype + '>'
        else:
            results_df[position] = results_df[position]
    elif termtype.strip() == R2RML_IRI:
        # it is assumed that the IRI values will be correct, and they are not percent encoded
        results_df['reference_results'] = results_df['reference_results'].apply(lambda x: x.strip())
        results_df[position] = '<' + results_df['reference_results'] + '>'
    elif termtype.strip() == R2RML_BLANK_NODE:
        results_df[position] = '_:' + results_df['reference_results']

    return results_df


def _materialize_fno_execution(results_df, fno_execution, fno_df, config, position, columns_alias='', termtype=R2RML_LITERAL, language_tag='', datatype=''):
    # TODO: handle column_alias?

    results_df = execute_fno(results_df, fno_df, fno_execution, config)

    if config.only_write_printable_characters():
        results_df[fno_execution] = results_df[fno_execution].apply(lambda x: remove_non_printable_characters(x))

    if termtype.strip() == R2RML_LITERAL:
        # Natural Mapping of SQL Values (https://www.w3.org/TR/r2rml/#natural-mapping)
        if datatype == XSD_BOOLEAN:
            results_df[fno_execution] = results_df[fno_execution].str.lower()
        elif datatype == XSD_DATETIME:
            results_df[fno_execution] = results_df[fno_execution].str.replace(' ', 'T', regex=False)
        # Make integers not end with .0
        elif datatype == XSD_INTEGER:
            results_df[fno_execution] = results_df[fno_execution].astype(float).astype(int).astype(str)

        results_df[fno_execution] = results_df[fno_execution].str.replace('\\', '\\\\', regex=False).str.replace('\n', '\\n', regex=False).str.replace('\t', '\\t', regex=False).str.replace('\b', '\\b', regex=False).str.replace('\f', '\\f', regex=False).str.replace('\r', '\\r', regex=False).str.replace('"', '\\"', regex=False).str.replace("'", "\\'", regex=False)
        results_df[position] = '"' + results_df[fno_execution] + '"'
        if pd.notna(language_tag):
            results_df[position] = results_df[position] + '@' + language_tag
        elif pd.notna(datatype):
            results_df[position] = results_df[position] + '^^<' + datatype + '>'
        else:
            results_df[position] = results_df[position]
    elif termtype.strip() == R2RML_IRI:
        # it is assumed that the IRI values will be correct, and they are not percent encoded
        results_df[fno_execution] = results_df[fno_execution].apply(lambda x: x.strip())
        results_df[position] = '<' + results_df[fno_execution] + '>'
    elif termtype.strip() == R2RML_BLANK_NODE:
        results_df[position] = '_:' + results_df[fno_execution]

    return results_df


def _materialize_constant(results_df, constant, position, termtype=R2RML_IRI, language_tag='', datatype=''):
    complete_constant = ''
    if termtype.strip() == R2RML_IRI:
        complete_constant = '<' + constant + '>'
    elif termtype.strip() == R2RML_LITERAL:
        complete_constant = '"' + constant + '"'

        if pd.notna(language_tag):
            complete_constant = complete_constant + '@' + language_tag
        elif pd.notna(datatype):
            complete_constant = complete_constant + '^^<' + datatype + '>'
        else:
            complete_constant = complete_constant
    elif termtype.strip() == R2RML_BLANK_NODE:
        complete_constant = '_:' + constant

    results_df[position] = complete_constant

    return results_df


def _materialize_join_rml_rule_terms(results_df, rml_rule, parent_triples_map_rule, config):

    if rml_rule['subject_map_type'] == R2RML_TEMPLATE:
        results_df = _materialize_template(results_df, rml_rule['subject_map_value'], config, 'subject', termtype=rml_rule['subject_termtype'])
    elif rml_rule['subject_map_type'] == R2RML_CONSTANT:
        results_df = _materialize_constant(results_df, rml_rule['subject_map_value'], 'subject', termtype=rml_rule['subject_termtype'])
    elif rml_rule['subject_map_type'] == RML_REFERENCE:
        results_df = _materialize_reference(results_df, rml_rule['subject_map_value'], config, 'subject', termtype=rml_rule['subject_termtype'])
    if rml_rule['predicate_map_type'] == R2RML_TEMPLATE:
        results_df = _materialize_template(results_df, rml_rule['predicate_map_value'], config, 'predicate')
    elif rml_rule['predicate_map_type'] == R2RML_CONSTANT:
        results_df = _materialize_constant(results_df, rml_rule['predicate_map_value'], 'predicate')
    elif rml_rule['predicate_map_type'] == RML_REFERENCE:
        results_df = _materialize_reference(results_df, rml_rule['predicate_map_value'], config, 'predicate', termtype=R2RML_IRI)
    if parent_triples_map_rule['subject_map_type'] == R2RML_TEMPLATE:
        results_df = _materialize_template(results_df, parent_triples_map_rule['subject_map_value'], config, 'object', termtype=parent_triples_map_rule['subject_termtype'], columns_alias='parent_')
    elif parent_triples_map_rule['subject_map_type'] == R2RML_CONSTANT:
        results_df = _materialize_constant(results_df, parent_triples_map_rule['subject_map_value'], 'object', termtype=parent_triples_map_rule['subject_termtype'])
    elif parent_triples_map_rule['subject_map_type'] == RML_REFERENCE:
        results_df = _materialize_reference(results_df, parent_triples_map_rule['subject_map_value'], config, 'object', termtype=parent_triples_map_rule['subject_termtype'], columns_alias='parent_')

    return results_df


def _materialize_rml_rule_terms(results_df, rml_rule, fno_df, config):
    if rml_rule['subject_map_type'] == R2RML_TEMPLATE:
        results_df = _materialize_template(results_df, rml_rule['subject_map_value'], config, 'subject', termtype=rml_rule['subject_termtype'])
    elif rml_rule['subject_map_type'] == R2RML_CONSTANT:
        results_df = _materialize_constant(results_df, rml_rule['subject_map_value'], 'subject', termtype=rml_rule['subject_termtype'])
    elif rml_rule['subject_map_type'] == RML_REFERENCE:
        results_df = _materialize_reference(results_df, rml_rule['subject_map_value'], config, 'subject', termtype=rml_rule['subject_termtype'])
    elif rml_rule['subject_map_type'] == FNML_EXECUTION:
        results_df = _materialize_fno_execution(results_df, rml_rule['subject_map_value'], fno_df, config, 'subject', termtype=rml_rule['subject_termtype'])
    if rml_rule['predicate_map_type'] == R2RML_TEMPLATE:
        results_df = _materialize_template(results_df, rml_rule['predicate_map_value'], config, 'predicate')
    elif rml_rule['predicate_map_type'] == R2RML_CONSTANT:
        results_df = _materialize_constant(results_df, rml_rule['predicate_map_value'], 'predicate')
    elif rml_rule['predicate_map_type'] == RML_REFERENCE:
        results_df = _materialize_reference(results_df, rml_rule['predicate_map_value'], config, 'predicate', termtype=R2RML_IRI)
    elif rml_rule['predicate_map_type'] == FNML_EXECUTION:
        results_df = _materialize_fno_execution(results_df, rml_rule['predicate_map_value'], fno_df, config, 'predicate', termtype=R2RML_IRI)
    if rml_rule['object_map_type'] == R2RML_TEMPLATE:
        results_df = _materialize_template(results_df, rml_rule['object_map_value'], config, 'object', termtype=rml_rule['object_termtype'], language_tag=rml_rule['object_language'], datatype=rml_rule['object_datatype'])
    elif rml_rule['object_map_type'] == R2RML_CONSTANT:
        results_df = _materialize_constant(results_df, rml_rule['object_map_value'], 'object', termtype=rml_rule['object_termtype'], language_tag=rml_rule['object_language'], datatype=rml_rule['object_datatype'])
    elif rml_rule['object_map_type'] == RML_REFERENCE:
        results_df = _materialize_reference(results_df, rml_rule['object_map_value'], config, 'object', termtype=rml_rule['object_termtype'], language_tag=rml_rule['object_language'], datatype=rml_rule['object_datatype'])
    elif rml_rule['object_map_type'] == FNML_EXECUTION:
        results_df = _materialize_fno_execution(results_df, rml_rule['object_map_value'], fno_df, config, 'object', termtype=rml_rule['object_termtype'], language_tag=rml_rule['object_language'], datatype=rml_rule['object_datatype'])

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


def _materialize_rml_rule(rml_rule, rml_df, fno_df, config, data=None, parent_join_references=set(), nest_level=0):
    references = set(_get_references_in_rml_rule(rml_rule, rml_df, fno_df))

    references_subject_join, parent_references_subject_join = get_references_in_join_condition(rml_rule, 'subject_join_conditions')
    references_object_join, parent_references_object_join = get_references_in_join_condition(rml_rule, 'object_join_conditions')
    references.update(parent_join_references)

    if rml_rule['subject_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP or rml_rule['object_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP:
        if data is None:
            data = _get_data(config, rml_rule, references)

        if rml_rule['subject_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP:
            if pd.notna(rml_rule['subject_join_conditions']):
                references.update(references_subject_join)
                parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['subject_map_value'])
                parent_data = _materialize_rml_rule(parent_triples_map_rule, rml_df, fno_df, config, parent_join_references=parent_references_subject_join, nest_level=nest_level + 1)
                data = _merge_data(data, parent_data, rml_rule, 'subject_join_conditions')
                data['subject'] = '<< ' + data['parent_triple'] + ' >>'
                data = data.drop(columns=['parent_triple'])
            else:
                parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['subject_map_value'])
                data = _materialize_rml_rule(parent_triples_map_rule, rml_df, fno_df, config, data=data, nest_level=nest_level + 1)
                data['subject'] = '<< ' + data['triple'] + ' >>'
            data['keep_subject'+str(nest_level)] = data['subject']
        if rml_rule['object_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP:
            if pd.notna(rml_rule['object_join_conditions']):
                references.update(references_object_join)
                parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['object_map_value'])
                parent_data = _materialize_rml_rule(parent_triples_map_rule, rml_df, fno_df, config, parent_join_references=parent_references_object_join, nest_level=nest_level + 1)
                data = _merge_data(data, parent_data, rml_rule, 'object_join_conditions')
                data['object'] = '<< ' + data['parent_triple'] + ' >>'
                data = data.drop(columns=['parent_triple'])
            else:
                parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['object_map_value'])
                data = _materialize_rml_rule(parent_triples_map_rule, rml_df, fno_df, config, data=data, nest_level=nest_level + 1)
                data['object'] = '<< ' + data['triple'] + ' >>'
            if rml_rule['subject_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP:
                data['subject'] = data['keep_subject'+str(nest_level)]

        if rml_rule['subject_map_type'] == R2RML_TEMPLATE:
            data = _materialize_template(data, rml_rule['subject_map_value'], config, 'subject', termtype=rml_rule['subject_termtype'])
        elif rml_rule['subject_map_type'] == R2RML_CONSTANT:
            data = _materialize_constant(data, rml_rule['subject_map_value'], 'subject', termtype=rml_rule['subject_termtype'])
        elif rml_rule['subject_map_type'] == RML_REFERENCE:
            data = _materialize_reference(data, rml_rule['subject_map_value'], config, 'subject', termtype=rml_rule['subject_termtype'])

        if rml_rule['object_map_type'] == R2RML_TEMPLATE:
            data = _materialize_template(data, rml_rule['object_map_value'], config, 'object', termtype=rml_rule['object_termtype'], language_tag=rml_rule['object_language'], datatype=rml_rule['object_datatype'])
        elif rml_rule['object_map_type'] == R2RML_CONSTANT:
            data = _materialize_constant(data, rml_rule['object_map_value'], 'object', termtype=rml_rule['object_termtype'], language_tag=rml_rule['object_language'], datatype=rml_rule['object_datatype'])
        elif rml_rule['object_map_type'] == RML_REFERENCE:
            data = _materialize_reference(data, rml_rule['object_map_value'], config, 'object', termtype=rml_rule['object_termtype'], language_tag=rml_rule['object_language'], datatype=rml_rule['object_datatype'])

        if rml_rule['predicate_map_type'] == R2RML_TEMPLATE:
            data = _materialize_template(data, rml_rule['predicate_map_value'], config, 'predicate')
        elif rml_rule['predicate_map_type'] == R2RML_CONSTANT:
            data = _materialize_constant(data, rml_rule['predicate_map_value'], 'predicate')
        elif rml_rule['predicate_map_type'] == RML_REFERENCE:
            data = _materialize_reference(data, rml_rule['predicate_map_value'], config, 'predicate', termtype=R2RML_IRI)

    # elif pd.notna(rml_rule['object_parent_triples_map']):
    elif rml_rule['object_map_type'] == R2RML_PARENT_TRIPLES_MAP:

        references.update(references_object_join)
        # parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['object_parent_triples_map'])
        parent_triples_map_rule = get_rml_rule(rml_df, rml_rule['object_map_value'])
        parent_references = set(_get_references_in_rml_rule(parent_triples_map_rule, rml_df, fno_df, only_subject_map=True))

        # add references used in the join condition
        references, parent_references = _add_references_in_join_condition(rml_rule, references, parent_references)

        if data is None:
            data = _get_data(config, rml_rule, references)

        parent_data = _get_data(config, parent_triples_map_rule, parent_references)
        merged_data = _merge_data(data, parent_data, rml_rule, 'object_join_conditions')
        data = _materialize_join_rml_rule_terms(merged_data, rml_rule, parent_triples_map_rule, config)
    else:

        if data is None:
            data = _get_data(config, rml_rule, references)

        data = _materialize_rml_rule_terms(data, rml_rule, fno_df, config)

    # TODO: this is slow reduce the number of vectorized operations

    data['triple'] = data['subject'] + ' ' + data['predicate'] + ' ' + data['object']

    if nest_level == 0 and config.get_output_format() == NQUADS:
        if rml_rule['graph_map_type'] == R2RML_TEMPLATE:
            data = _materialize_template(data, rml_rule['graph_map_value'], config, 'graph')
        elif rml_rule['graph_map_type'] == R2RML_CONSTANT and rml_rule['graph_map_value'] != R2RML_DEFAULT_GRAPH:
            data = _materialize_constant(data, rml_rule['graph_map_value'], 'graph')
        elif rml_rule['graph_map_type'] == RML_REFERENCE:
            data = _materialize_reference(data, rml_rule['graph_map_value'], config, 'graph', termtype=R2RML_IRI)
        else:
            data['graph'] = ''
        data['triple'] = data['triple'] + ' ' + data['graph']

    data = data.drop(columns=['subject', 'predicate', 'object'], errors='ignore')

    return data


def _materialize_mapping_group_to_file(mapping_group_df, rml_df, fno_df, config):

    triples = set()
    for i, rml_rule in mapping_group_df.iterrows():
        start_time = time.time()
        data = _materialize_rml_rule(rml_rule, rml_df, fno_df, config)
        triples.update(set(data['triple']))

        logging.debug(f"{len(triples)} triples generated for mapping rule `{rml_rule['triples_map_id']}` "
                      f"in {get_delta_time(start_time)} seconds.")

    triples_to_file(triples, config, mapping_group_df.iloc[0]['mapping_partition'])

    return len(triples)


def _materialize_mapping_group_to_set(mapping_group_df, rml_df, fno_df, config):

    triples = set()
    for i, rml_rule in mapping_group_df.iterrows():
        data = _materialize_rml_rule(rml_rule, rml_df, fno_df, config)
        triples.update(set(data['triple']))

    return triples
