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


def _add_references_in_join_condition(mapping_rule, references, parent_references):
    references_join, parent_references_join = get_references_in_join_condition(mapping_rule, 'object_join_conditions')

    references.update(set(references_join))
    parent_references.update(set(parent_references_join))

    return references, parent_references


def _preprocess_data(data, mapping_rule, references, config):
    # deal with ORACLE
    if mapping_rule['source_type'] == RDB:
        if config.get_database_url(mapping_rule['source_name']).lower().startswith(ORACLE.lower()):
            data = normalize_oracle_identifier_casing(data, references)

    data = remove_null_values_from_dataframe(data, config, references)
    data = data.convert_dtypes(convert_boolean=False)

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
    if mapping_rule['subject_map_type'] == R2RML_TEMPLATE:
        references.extend(get_references_in_template(str(mapping_rule['subject_map_value'])))
    elif mapping_rule['subject_map_type'] == RML_REFERENCE:
        references.append(str(mapping_rule['subject_map_value']))

    if not only_subject_map:
        if mapping_rule['predicate_map_type'] == R2RML_TEMPLATE:
            references.extend(get_references_in_template(str(mapping_rule['predicate_map_value'])))
        elif mapping_rule['predicate_map_type'] == RML_REFERENCE:
            references.append(str(mapping_rule['predicate_map_value']))
        if mapping_rule['object_map_type'] == R2RML_TEMPLATE:
            references.extend(get_references_in_template(str(mapping_rule['object_map_value'])))
        elif mapping_rule['object_map_type'] == RML_REFERENCE:
            references.append(str(mapping_rule['object_map_value']))
        if mapping_rule['graph_map_type'] == R2RML_TEMPLATE:
            references.extend(get_references_in_template(str(mapping_rule['graph_map_value'])))
        elif mapping_rule['graph_map_type'] == RML_REFERENCE:
            references.append(str(mapping_rule['graph_map_value']))

    if mapping_rule['subject_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP and pd.isna(mapping_rule['subject_join_conditions']):

        parent_mapping_rule = get_mapping_rule(mappings_df, mapping_rule['subject_map_value'])
        references.extend(_get_references_in_mapping_rule(parent_mapping_rule, mappings_df))
    if mapping_rule['object_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP and pd.isna(mapping_rule['object_join_conditions']):

        parent_mapping_rule = get_mapping_rule(mappings_df, mapping_rule['object_map_value'])
        references.extend(_get_references_in_mapping_rule(parent_mapping_rule, mappings_df))
    references_subject_join, parent_references_subject_join = get_references_in_join_condition(mapping_rule, 'subject_join_conditions')
    references.extend(references_subject_join)
    references_object_join, parent_references_object_join = get_references_in_join_condition(mapping_rule, 'object_join_conditions')
    references.extend(references_object_join)

    return references


def _materialize_template(results_df, template, config, position, columns_alias='', termtype=R2RML_IRI, language_tag='',
                          datatype=''):
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

    if mapping_rule['subject_map_type'] == R2RML_TEMPLATE:
        results_df = _materialize_template(results_df, mapping_rule['subject_map_value'], config, 'subject', termtype=mapping_rule['subject_termtype'])
    elif mapping_rule['subject_map_type'] == R2RML_CONSTANT:
        results_df = _materialize_constant(results_df, mapping_rule['subject_map_value'], 'subject', termtype=mapping_rule['subject_termtype'])
    elif mapping_rule['subject_map_type'] == RML_REFERENCE:
        results_df = _materialize_reference(results_df, mapping_rule['subject_map_value'], config, 'subject', termtype=mapping_rule['subject_termtype'])
    if mapping_rule['predicate_map_type'] == R2RML_TEMPLATE:
        results_df = _materialize_template(results_df, mapping_rule['predicate_map_value'], config, 'predicate')
    elif mapping_rule['predicate_map_type'] == R2RML_CONSTANT:
        results_df = _materialize_constant(results_df, mapping_rule['predicate_map_value'], 'predicate')
    elif mapping_rule['predicate_map_type'] == RML_REFERENCE:
        results_df = _materialize_reference(results_df, mapping_rule['predicate_map_value'], config, 'predicate', termtype=R2RML_IRI)
    if parent_triples_map_rule['subject_map_type'] == R2RML_TEMPLATE:
        results_df = _materialize_template(results_df, parent_triples_map_rule['subject_map_value'], config, 'object', termtype=parent_triples_map_rule['subject_termtype'], columns_alias='parent_')
    elif parent_triples_map_rule['subject_map_type'] == R2RML_CONSTANT:
        results_df = _materialize_constant(results_df, parent_triples_map_rule['subject_map_value'], 'object', termtype=parent_triples_map_rule['subject_termtype'])
    elif parent_triples_map_rule['subject_map_type'] == RML_REFERENCE:
        results_df = _materialize_reference(results_df, parent_triples_map_rule['subject_map_value'], config, 'object', termtype=parent_triples_map_rule['subject_termtype'], columns_alias='parent_')

    return results_df


def _materialize_mapping_rule_terms(results_df, mapping_rule, config):
    if mapping_rule['subject_map_type'] == R2RML_TEMPLATE:
        results_df = _materialize_template(results_df, mapping_rule['subject_map_value'], config, 'subject', termtype=mapping_rule['subject_termtype'])
    elif mapping_rule['subject_map_type'] == R2RML_CONSTANT:
        results_df = _materialize_constant(results_df, mapping_rule['subject_map_value'], 'subject', termtype=mapping_rule['subject_termtype'])
    elif mapping_rule['subject_map_type'] == RML_REFERENCE:
        results_df = _materialize_reference(results_df, mapping_rule['subject_map_value'], config, 'subject', termtype=mapping_rule['subject_termtype'])
    if mapping_rule['predicate_map_type'] == R2RML_TEMPLATE:
        results_df = _materialize_template(results_df, mapping_rule['predicate_map_value'], config, 'predicate')
    elif mapping_rule['predicate_map_type'] == R2RML_CONSTANT:
        results_df = _materialize_constant(results_df, mapping_rule['predicate_map_value'], 'predicate')
    elif mapping_rule['predicate_map_type'] == RML_REFERENCE:
        results_df = _materialize_reference(results_df, mapping_rule['predicate_map_value'], config, 'predicate', termtype=R2RML_IRI)
    if mapping_rule['object_map_type'] == R2RML_TEMPLATE:
        results_df = _materialize_template(results_df, mapping_rule['object_map_value'], config, 'object', termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])
    elif mapping_rule['object_map_type'] == R2RML_CONSTANT:
        results_df = _materialize_constant(results_df, mapping_rule['object_map_value'], 'object', termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])
    elif mapping_rule['object_map_type'] == RML_REFERENCE:
        results_df = _materialize_reference(results_df, mapping_rule['object_map_value'], config, 'object', termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])

    return results_df


def _merge_data(data, parent_data, mapping_rule, join_condition):
    parent_data = parent_data.add_prefix('parent_')
    child_join_references, parent_join_references = get_references_in_join_condition(mapping_rule, join_condition)
    parent_join_references = ['parent_' + reference for reference in parent_join_references]

    # if there is only one join condition use join, otherwise use merge
    if len(child_join_references) == 1:
        data = data.set_index(child_join_references, drop=False)
        parent_data = parent_data.set_index(parent_join_references, drop=False)
        return data.join(parent_data, how='inner')
    else:
        return data.merge(parent_data, how='inner', left_on=child_join_references, right_on=parent_join_references)


def _materialize_mapping_rule(mapping_rule, mappings_df, config, data=None, parent_join_references=set(), nest_level=0):
    references = set(_get_references_in_mapping_rule(mapping_rule, mappings_df))

    references_subject_join, parent_references_subject_join = get_references_in_join_condition(mapping_rule, 'subject_join_conditions')
    references_object_join, parent_references_object_join = get_references_in_join_condition(mapping_rule, 'object_join_conditions')
    references.update(parent_join_references)

    if mapping_rule['subject_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP or mapping_rule['object_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP:
        if data is None:
            data = _get_data(config, mapping_rule, references)

        if mapping_rule['subject_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP:
            if pd.notna(mapping_rule['subject_join_conditions']):
                references.update(references_subject_join)
                parent_triples_map_rule = get_mapping_rule(mappings_df, mapping_rule['subject_map_value'])
                parent_data = _materialize_mapping_rule(parent_triples_map_rule, mappings_df, config, parent_join_references=parent_references_subject_join, nest_level=nest_level + 1)
                data = _merge_data(data, parent_data, mapping_rule, 'subject_join_conditions')
                data['subject'] = '<< ' + data['parent_triple'] + ' >>'
                data = data.drop(columns=['parent_triple'])
            else:
                parent_triples_map_rule = get_mapping_rule(mappings_df, mapping_rule['subject_map_value'])
                data = _materialize_mapping_rule(parent_triples_map_rule, mappings_df, config, data=data, nest_level=nest_level + 1)
                data['subject'] = '<< ' + data['triple'] + ' >>'
            data['keep_subject'+str(nest_level)] = data['subject']
        if mapping_rule['object_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP:
            if pd.notna(mapping_rule['object_join_conditions']):
                references.update(references_object_join)
                parent_triples_map_rule = get_mapping_rule(mappings_df, mapping_rule['object_map_value'])
                parent_data = _materialize_mapping_rule(parent_triples_map_rule, mappings_df, config, parent_join_references=parent_references_object_join, nest_level=nest_level + 1)
                data = _merge_data(data, parent_data, mapping_rule, 'object_join_conditions')
                data['object'] = '<< ' + data['parent_triple'] + ' >>'
                data = data.drop(columns=['parent_triple'])
            else:
                parent_triples_map_rule = get_mapping_rule(mappings_df, mapping_rule['object_map_value'])
                data = _materialize_mapping_rule(parent_triples_map_rule, mappings_df, config, data=data, nest_level=nest_level + 1)
                data['object'] = '<< ' + data['triple'] + ' >>'
            if mapping_rule['subject_map_type'] == RML_STAR_QUOTED_TRIPLES_MAP:
                data['subject'] = data['keep_subject'+str(nest_level)]

        if mapping_rule['subject_map_type'] == R2RML_TEMPLATE:
            data = _materialize_template(data, mapping_rule['subject_map_value'], config, 'subject', termtype=mapping_rule['subject_termtype'])
        elif mapping_rule['subject_map_type'] == R2RML_CONSTANT:
            data = _materialize_constant(data, mapping_rule['subject_map_value'], 'subject', termtype=mapping_rule['subject_termtype'])
        elif mapping_rule['subject_map_type'] == RML_REFERENCE:
            data = _materialize_reference(data, mapping_rule['subject_map_value'], config, 'subject', termtype=mapping_rule['subject_termtype'])

        if mapping_rule['object_map_type'] == R2RML_TEMPLATE:
            data = _materialize_template(data, mapping_rule['object_map_value'], config, 'object', termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])
        elif mapping_rule['object_map_type'] == R2RML_CONSTANT:
            data = _materialize_constant(data, mapping_rule['object_map_value'], 'object', termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])
        elif mapping_rule['object_map_type'] == RML_REFERENCE:
            data = _materialize_reference(data, mapping_rule['object_map_value'], config, 'object', termtype=mapping_rule['object_termtype'], language_tag=mapping_rule['object_language'], datatype=mapping_rule['object_datatype'])

        if mapping_rule['predicate_map_type'] == R2RML_TEMPLATE:
            data = _materialize_template(data, mapping_rule['predicate_map_value'], config, 'predicate')
        elif mapping_rule['predicate_map_type'] == R2RML_CONSTANT:
            data = _materialize_constant(data, mapping_rule['predicate_map_value'], 'predicate')
        elif mapping_rule['predicate_map_type'] == RML_REFERENCE:
            data = _materialize_reference(data, mapping_rule['predicate_map_value'], config, 'predicate', termtype=R2RML_IRI)

    # elif pd.notna(mapping_rule['object_parent_triples_map']):
    elif mapping_rule['object_map_type'] == R2RML_PARENT_TRIPLES_MAP:

        references.update(references_object_join)
        # parent_triples_map_rule = get_mapping_rule(mappings_df, mapping_rule['object_parent_triples_map'])
        parent_triples_map_rule = get_mapping_rule(mappings_df, mapping_rule['object_map_value'])
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
        if mapping_rule['graph_map_type'] == R2RML_TEMPLATE:
            data = _materialize_template(data, mapping_rule['graph_map_value'], config, 'graph')
        elif mapping_rule['graph_map_type'] == R2RML_CONSTANT and str(mapping_rule['graph_map_value']) != R2RML_DEFAULT_GRAPH:
            data = _materialize_constant(data, mapping_rule['graph_map_value'], 'graph')
        elif mapping_rule['graph_map_type'] == RML_REFERENCE:
            data = _materialize_reference(data, mapping_rule['graph_map_value'], config, 'graph', termtype=R2RML_IRI)
        else:
            data['graph'] = ''
        data['triple'] = data['triple'] + ' ' + data['graph']

    data = data.drop(columns=['subject', 'predicate', 'object'], errors='ignore')

    return data


def _materialize_mapping_group_to_file(mapping_group_df, mappings_df, config):

    triples = set()
    for i, mapping_rule in mapping_group_df.iterrows():
        start_time = time.time()
        data = _materialize_mapping_rule(mapping_rule, mappings_df, config)
        triples.update(set(data['triple']))

        logging.debug(f"{len(triples)} triples generated for mapping rule `{mapping_rule['id']}` "
                      f"in {get_delta_time(start_time)} seconds.")

    triples_to_file(triples, config, mapping_group_df.iloc[0]['mapping_partition'])

    return len(triples)


def _materialize_mapping_group_to_set(mapping_group_df, mappings_df, config):

    triples = set()
    for i, mapping_rule in mapping_group_df.iterrows():
        data = _materialize_mapping_rule(mapping_rule, mappings_df, config)
        triples.update(set(data['triple']))

    return triples
