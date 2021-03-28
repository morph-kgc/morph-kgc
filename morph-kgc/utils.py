""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import re
import os
import shutil
import logging
import rdflib
import constants
import time


def get_repeated_elements_in_list(input_list):
    """
    Finds repeated elements in a list.

    :param input_list: list of elements
    :type input_list: list
    :return list with the repeated elements in input_list
    :rtype list
    """

    elem_count = {}
    for elem in input_list:
        if elem in elem_count:
            elem_count[elem] = elem_count[elem] + 1
        else:
            elem_count[elem] = 1

    repeated_elems = []
    for elem, elem_count in elem_count.items():
        if elem_count > 1:
            repeated_elems.append(elem)

    return repeated_elems


def get_subject_maps(mappings_df):
    """
    Retrieves subject maps from mapping rules in the input DataFrame. No repeated subject maps are returned.

    :param mappings_df: DataFrame populated with mapping rules
    :type mappings_df: DataFrame
    :return DataFrame with subject maps in mappings_df
    :rtype DataFrame
    """

    subject_maps_df = mappings_df[[
        'triples_map_id', 'source_name', 'source_type', 'data_source', 'ref_form', 'iterator', 'tablename',
        'query', 'subject_template', 'subject_reference', 'subject_constant', 'subject_termtype']
    ]

    subject_maps_df = subject_maps_df.drop_duplicates()

    if len(list(subject_maps_df['triples_map_id'])) > len(set(subject_maps_df['triples_map_id'])):
        logging.critical('One or more triples maps have incongruities in subject maps.')

    return subject_maps_df


def get_references_in_template(template):
    """
    Retrieves all reference identifiers in a template-valued term map. References are returned in order of appearance
    in the template.

    :param template: template-valued term map
    :type template: str
    :return list of references in template
    :rtype list
    """

    return re.findall('\\{([^}]+)', template)


def triples_to_file(triples, config, mapping_partition=''):
    """
    Write triples to file. If mapping_partition is provided it is used as file name. File extension is inferred from
    output_format in config. If mapping_partition is provided and final results will be in a unique file, the triples
    are written to a temporary directory.

    :param triples: set of triples to write to file
    :type triples: set
    :param config: config object
    :type config: ConfigParser
    :param mapping_partition: name of the mapping partition associated to the triples
    :type mapping_partition: str
    """

    if config.get('CONFIGURATION', 'output_file'):
        file_path = os.path.join(config.get('CONFIGURATION', 'output_dir'), config.get('CONFIGURATION', 'output_file'))
        # remove file extension, we will set it based on the output format
        if file_path.endswith('.nt') or file_path.endswith('.nq'):
            file_path = file_path[:-3]
    elif mapping_partition:
        file_path = os.path.join(config.get('CONFIGURATION', 'output_dir'), mapping_partition)
    else:
        file_path = os.path.join(config.get('CONFIGURATION', 'output_dir'), 'result')

    if config.get('CONFIGURATION', 'output_format') == 'ntriples':
        file_path += '.nt'
    elif config.get('CONFIGURATION', 'output_format') == 'nquads':
        file_path += '.nq'

    f = open(file_path, 'a')
    if config.getboolean('CONFIGURATION', 'only_printable_characters'):
        for triple in triples:
            # REMOVING NON PRINTABLE CHARACTERS THIS WAY IS VERY SLOW!
            f.write(''.join(c for c in triple if c.isprintable()) + '.\n')
    else:
        for triple in triples:
            f.write(triple + '.\n')
    f.close()


def clean_output_dir(config):
    """
    Removes all files and directories within output_dir in config depending on clean_output_dir parameter. The output
    file, if provided in config, is always deleted.

    :param config: config object
    :type config: ConfigParser
    """

    output_dir = config.get('CONFIGURATION', 'output_dir')
    output_file = config.get('CONFIGURATION', 'output_file')
    if output_file:
        if os.path.exists(os.path.join(output_dir, output_file)):
            # always delete output file, so that generated triples are not appended to it
            os.remove(os.path.join(output_dir, output_file))

    if config.getboolean('CONFIGURATION', 'clean_output_dir'):
        for obj in os.listdir(output_dir):
            obj_path = os.path.join(output_dir, obj)
            if os.path.isdir(obj_path):
                shutil.rmtree(obj_path)
            else:
                os.remove(obj_path)

        logging.debug('Cleaned output directory.')


def dataframe_columns_to_str(df):
    """
    Converts all the columns in the input dataframe to str.

    :param df: dataframe that might have non str columns
    :type df: DataFrame
    :return dataframe with all columns converted to str
    :rtype DataFrame
    """

    for col_name in list(df.columns):
        df[col_name] = df[col_name].astype(str)

    return df


def get_invariable_part_of_template(template):
    """
    Retrieves the part of the template before the first reference. This part of the template does not depend on
    reference and therefore is invariable. If the template has no references, it is an invalid template, and an
    exception is thrown.

    :param template: template
    :type template: str
    :return invariable part of the template
    :rtype str
    """

    template_for_splitting = template.replace('\\{', constants.AUXILIAR_UNIQUE_REPLACING_STRING)
    if '{' in template_for_splitting:
        invariable_part_of_template = template_for_splitting.split('{')[0]
        invariable_part_of_template = invariable_part_of_template.replace(constants.AUXILIAR_UNIQUE_REPLACING_STRING,
                                                                          '\\{')
    else:
        # no references were found in the template, and therefore the template is invalid
        raise Exception("Invalid template `" + template + "`. No pairs of unescaped curly braces were found.")

    return invariable_part_of_template


def replace_predicates_in_graph(graph, predicate_to_remove, predicate_to_add):
    """
    Replaces in a graph the predicates predicate_to_remove with the predicate predicate_to_add.
    """

    # get the triples with the predicate to be replaced
    r2rml_sources_query = 'SELECT ?s ?o WHERE {?s <' + predicate_to_remove + '> ?o .}'
    logical_sources = graph.query(r2rml_sources_query)

    # for each triple to be replaced a similar one (same subject and object) but with the new predicate
    for s, o in logical_sources:
        graph.add((s, rdflib.term.URIRef(predicate_to_add), o))

    # remove all triples in the graph that have the old predicate
    graph.remove((None, rdflib.term.URIRef(predicate_to_remove), None))

    return graph


def get_mapping_rule_from_triples_map_id(mappings, parent_triples_map_id):
    """
    Get the parent triples map of mapping rule with the given id
    """

    parent_triples_map = mappings[mappings['triples_map_id'] == parent_triples_map_id]

    return parent_triples_map.iloc[0]


def get_delta_time(start_time):
    return "{:.3f}".format((time.time() - start_time))


def add_references_in_join_condition(mapping_rule, references, parent_references):
    references_join, parent_references_join = get_references_in_join_condition(mapping_rule)

    references.update(set(references_join))
    parent_references.update(set(parent_references_join))

    return references, parent_references


def get_references_in_join_condition(mapping_rule):
    references = list()
    parent_references = list()

    join_conditions = eval(mapping_rule['join_conditions'])
    for join_condition in join_conditions.values():
        references.append(join_condition['child_value'])
        parent_references.append(join_condition['parent_value'])

    return references, parent_references

