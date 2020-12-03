import logging, re
import pandas as pd


def _get_references_in_template(template):
    template = template.replace('\{', 'zwy\u200B').replace('\}', 'ywz\u200A')

    references = re.findall('\{([^}]+)', template)
    references = [reference.replace('zwy\u200B', '\{').replace('ywz\u200A', '\}') for reference in references]

    return references


def _get_references_in_mapping_rule(mapping_rule):
    references = []
    if mapping_rule['subject_template']:
        references.extend(_get_references_in_template(str(mapping_rule['subject_template'])))
    elif mapping_rule['subject_reference']:
        references.append(str(mapping_rule['subject_reference']))
    if mapping_rule['predicate_template']:
        references.extend(_get_references_in_template(str(mapping_rule['predicate_template'])))
    elif mapping_rule['predicate_reference']:
        references.append(str(mapping_rule['predicate_reference']))
    if mapping_rule['object_template']:
        references.extend(_get_references_in_template(str(mapping_rule['object_template'])))
    elif mapping_rule['object_reference']:
        references.append(str(mapping_rule['object_reference']))

    return set(references)



def _materialize_mapping_rule(mapping_rule):

    mapping_result = pd.DataFrame(columns=['subject', 'predicate', 'object'])
    references = _get_references_in_mapping_rule(mapping_rule)

    if len(references) > 0:
        query = 'SELECT '
        for reference in references:
            query = query + reference + ', '
        query = query[:-2] + ' FROM ' + mapping_rule['tablename']


def materialize(mappings_df):
    mappings_df.to_csv('out.csv', index=False)
    mapping_partitions = [group for _, group in mappings_df.groupby(by='mapping_partition')]

    for mapping_partition in mapping_partitions:
        for i, mapping_rule in mapping_partition.iterrows():
            mapping_result = _materialize_mapping_rule(mapping_rule)

