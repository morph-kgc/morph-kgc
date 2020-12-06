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


def _materialize_template(query_results_df, template, columns_alias=''):
    references = morph_utils.get_references_in_template(str(template))

    query_results_df['triple'] = query_results_df['triple'] + '<'
    for reference in references:
        splitted_template = template.split('{' + reference + '}')
        query_results_df['triple'] = query_results_df['triple'] + splitted_template[0]
        query_results_df['triple'] = query_results_df['triple'] + \
                                     query_results_df[columns_alias + reference].astype(str)
        template = str('{' + reference + '}').join(splitted_template[1:])
    query_results_df['triple'] = query_results_df['triple'] + template + '> '

    return query_results_df


def _materialize_reference(query_results_df, reference, columns_alias=''):
    query_results_df['triple'] = query_results_df['triple'] + '"' + \
                                 query_results_df[columns_alias + str(reference)].astype(str) + '" '

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

        for key, join_condition in eval(mapping_rule['join_conditions']).items():
            parent_references.add(join_condition['parent_value'])
            child_references.add(join_condition['child_value'])

        child_query = 'SELECT '
        if len(child_references) > 0:
            for reference in child_references:
                child_query = child_query + reference + ' AS child_' + reference + ', '
            child_query = child_query[:-2] + ' FROM ' + mapping_rule['tablename'] + ' WHERE '
            for reference in child_references:
                child_query = child_query + reference + ' IS NOT NULL AND '
            child_query = child_query[:-5]
        else:
            child_query = None

        parent_query = 'SELECT '
        if len(parent_references) > 0:
            for reference in parent_references:
                parent_query = parent_query + reference + ' AS parent_' + reference + ', '
            parent_query = parent_query[:-2] + ' FROM ' + parent_triples_map_rule['tablename'] + ' WHERE '
            for reference in parent_references:
                parent_query = parent_query + reference + ' IS NOT NULL AND '
            parent_query = parent_query[:-5]
        else:
            parent_query = None

        query = query + '* FROM (' + child_query + ') AS child, (' + parent_query + ') AS parent WHERE '
        for key, join_condition in eval(mapping_rule['join_conditions']).items():
            query = query + 'child.child_' + join_condition['child_value'] + \
                    '=parent.parent_' + join_condition['parent_value'] + ' AND '
        query = query[:-4] + ';'

        logging.info(query)

        db_connection = morph_utils.relational_db_connection(config, str(mapping_rule['source_name']))
        try:
            query_results_df = pd.read_sql(query, con=db_connection)
        except:
            raise Except('Query ' + query + ' has failed to execute.')
        db_connection.close()

        query_results_df['triple'] = ''
        if mapping_rule['subject_template']:
            query_results_df = _materialize_template(
                query_results_df, mapping_rule['subject_template'], columns_alias='child_')
        elif mapping_rule['subject_constant']:
            query_results_df = _materialize_constant(query_results_df, mapping_rule['subject_constant'])
        elif mapping_rule['subject_reference']:
            query_results_df = _materialize_reference(
                query_results_df, mapping_rule['subject_reference'], columns_alias='child_')
        if mapping_rule['predicate_template']:
            query_results_df = _materialize_template(
                query_results_df, mapping_rule['predicate_template'], columns_alias='child_')
        elif mapping_rule['predicate_constant']:
            query_results_df = _materialize_constant(query_results_df, mapping_rule['predicate_constant'])
        elif mapping_rule['predicate_reference']:
            query_results_df = _materialize_reference(
                query_results_df, mapping_rule['predicate_reference'], columns_alias='child_')
        if parent_triples_map_rule['subject_template']:
            query_results_df = _materialize_template(
                query_results_df, parent_triples_map_rule['subject_template'], columns_alias='parent_')
        elif parent_triples_map_rule['subject_constant']:
            query_results_df = _materialize_constant(query_results_df, parent_triples_map_rule['subject_constant'])
        elif parent_triples_map_rule['subject_reference']:
            query_results_df = _materialize_reference(
                query_results_df, parent_triples_map_rule['subject_reference'], columns_alias='parent_')

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

        file1 = open("result.txt", "w")
        for r in list(triples):
            file1.write(r + '.\n')
        file1.close()

        print(len(triples))
