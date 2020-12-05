import logging
import pandas as pd
import morph_utils


def _get_references_in_mapping_rule(mapping_rule, only_subject_map=False):
    references = []
    if mapping_rule['subject_template']:
        references.extend(morph_utils.get_references_in_template(str(mapping_rule['subject_template'])))
    elif mapping_rule['subject_reference']:
        references.append(str(mapping_rule['subject_reference']))

    if not only_subject_map:
        if mapping_rule['predicate_template']:
            references.extend(morph_utils.get_references_in_template(str(mapping_rule['predicate_template'])))
        elif mapping_rule['predicate_reference']:
            references.append(str(mapping_rule['predicate_reference']))
        if mapping_rule['object_template']:
            references.extend(morph_utils.get_references_in_template(str(mapping_rule['object_template'])))
        elif mapping_rule['object_reference']:
            references.append(str(mapping_rule['object_reference']))

    return set(references)


def _materialize_template(query_results_df, template):
    references = morph_utils.get_references_in_template(str(template))
    for reference in references:
        splitted_template = template.split('{' + reference + '}')
        query_results_df['triple'] = query_results_df['triple'] + '<' + splitted_template[0]
        query_results_df['triple'] = query_results_df['triple'] + query_results_df[reference].astype(str)
        template = str('{' + reference + '}').join(splitted_template[1:])
    query_results_df['triple'] = query_results_df['triple'] + template + '> '

    return query_results_df


def _materialize_reference(query_results_df, reference):
    query_results_df['triple'] = query_results_df['triple'] + '"' + query_results_df[str(reference)].astype(str) + '" '

    return query_results_df


def _materialize_constant(query_results_df, constant):
    query_results_df['triple'] = query_results_df['triple'] + '<' + str(constant) + '> '

    return query_results_df


def _materialize_mapping_rule(mapping_rule, subject_maps_df, config):

    query = 'SELECT '
    if config.getboolean('CONFIGURATION', 'remove_duplicates'):
        query = query + 'DISTINCT '

    if mapping_rule['object_parent_triples_map']:
        child_references = _get_references_in_mapping_rule(mapping_rule)
        parent_triples_map_rule = \
            subject_maps_df[subject_maps_df.triples_map_id==mapping_rule['object_parent_triples_map']].iloc[0]
        parent_references = _get_references_in_mapping_rule(parent_triples_map_rule, only_subject_map=True)

        # SELECT DISTINCT * FROM (SELECT trip_id, stop_id, arrival_time FROM STOP_TIMES WHERE stop_id IS NOT NULL AND trip_id IS NOT NULL AND arrival_time IS NOT NULL) AS child, (SELECT stop_id FROM STOPS WHERE stop_id IS NOT NULL) AS parent WHERE child.stop_id=parent.stop_id;

        return set()
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

        db_connection = morph_utils.relational_db_connection(config, str(mapping_rule['source_name']))

        try:
            query_results_df = pd.read_sql(query, con=db_connection)
        except:
            raise Except('Query ' + query + ' has failed to execute.')
        db_connection.close()

        query_results_df['triple'] = ''
        if mapping_rule['subject_template']:
            query_results_df = _materialize_template(query_results_df, mapping_rule['subject_template'])
        elif mapping_rule['subject_constant']:
            query_results_df = _materialize_constant(query_results_df, mapping_rule['subject_constant'])
        elif mapping_rule['subject_reference']:
            query_results_df = _materialize_reference(query_results_df, mapping_rule['subject_reference'])
        if mapping_rule['predicate_template']:
            query_results_df = _materialize_template(query_results_df, mapping_rule['predicate_template'])
        elif mapping_rule['predicate_constant']:
            query_results_df = _materialize_constant(query_results_df, mapping_rule['predicate_constant'])
        elif mapping_rule['predicate_reference']:
            query_results_df = _materialize_reference(query_results_df, mapping_rule['predicate_reference'])
        if mapping_rule['object_template']:
            query_results_df = _materialize_template(query_results_df, mapping_rule['object_template'])
        elif mapping_rule['object_constant']:
            query_results_df = _materialize_constant(query_results_df, mapping_rule['object_constant'])
        elif mapping_rule['object_reference']:
            query_results_df = _materialize_reference(query_results_df, mapping_rule['object_reference'])

        return query_results_df['triple']



def _get_subject_maps_dict_from_mappings(mappings_df):
    subject_maps_df = mappings_df[[
        'triples_map_id', 'data_source', 'ref_form', 'iterator', 'tablename', 'query', 'subject_template',
        'subject_reference', 'subject_constant', 'subject_rdf_class', 'subject_termtype', 'subject_graph']
    ]

    subject_maps_df = subject_maps_df.drop_duplicates()

    if len(list(subject_maps_df['triples_map_id'])) > len(set(subject_maps_df['triples_map_id'])):
        raise Exception('One or more triples maps have incongruencies in subject maps.')

    return subject_maps_df


def materialize(mappings_df, config):
    subject_maps_df = _get_subject_maps_dict_from_mappings(mappings_df)
    mapping_partitions = [group for _, group in mappings_df.groupby(by='mapping_partition')]

    for mapping_partition in mapping_partitions:
        triples = set()
        for i, mapping_rule in mapping_partition.iterrows():
            result_triples = _materialize_mapping_rule(mapping_rule, subject_maps_df, config)
            triples.update(set(result_triples))