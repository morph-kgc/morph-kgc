import logging, re, mysql.connector
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



def _materialize_mapping_rule(mapping_rule, subject_maps_dict, config):

    mapping_result = pd.DataFrame(columns=['triple'])

    query = 'SELECT '
    if config.get('CONFIGURATION', 'remove_duplicates'):
        query = query + 'DISTINCT '

    if mapping_rule['object_parent_triples_map']:
        query = None
    else:
        references = _get_references_in_mapping_rule(mapping_rule)

        if len(references) > 0:
            for reference in references:
                query = query + reference + ', '
            query = query[:-2] + ' FROM ' + mapping_rule['tablename'] + ' WHERE '
            for reference in references:
                query = query + reference + ' IS NOT NULL AND '
            query = query[:-4] + ';'
        else:
            query = None

    print(config.get(str(mapping_rule['source_name']), 'db'))
    db_connection = mysql.connector.connect(
        host=config.get(str(mapping_rule['source_name']), 'host'),
        port=config.get(str(mapping_rule['source_name']), 'port'),
        user=config.get(str(mapping_rule['source_name']), 'user'),
        passwd=config.get(str(mapping_rule['source_name']), 'password'),
        database=config.get(str(mapping_rule['source_name']), 'db')
    )
    try:
        query_results_df = pd.read_sql(query, con=db_connection)
    except:
        print('Something is failing in query')
    db_connection.close()


def _get_subject_maps_dict_from_mappings(mappings_df):
    subject_maps_df = mappings_df[[
        'triples_map_id', 'data_source', 'ref_form', 'iterator', 'tablename', 'query', 'subject_template',
        'subject_reference', 'subject_constant', 'subject_rdf_class', 'subject_termtype', 'subject_graph']
    ]

    subject_maps_df.drop_duplicates(inplace=True)

    subject_maps_dict = {}
    for i, subject_map in subject_maps_df.iterrows():
        subject_maps_dict[subject_map['triples_map_id']] = {
            'triples_map_id': subject_map['triples_map_id'],
            'data_source': subject_map['data_source'],
            'ref_form': subject_map['ref_form'],
            'iterator': subject_map['iterator'],
            'tablename': subject_map['tablename'],
            'query': subject_map['query'],
            'subject_template': subject_map['subject_template'],
            'subject_reference': subject_map['subject_reference'],
            'subject_constant': subject_map['subject_constant'],
            'subject_rdf_class': subject_map['subject_rdf_class'],
            'subject_termtype': subject_map['subject_termtype'],
            'subject_graph': subject_map['subject_graph']
        }

    return subject_maps_dict


def materialize(mappings_df, config):
    subject_maps_dict = _get_subject_maps_dict_from_mappings(mappings_df)
    mapping_partitions = [group for _, group in mappings_df.groupby(by='mapping_partition')]

    for mapping_partition in mapping_partitions:
        for i, mapping_rule in mapping_partition.iterrows():
            mapping_result = _materialize_mapping_rule(mapping_rule, subject_maps_dict, config)

