""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
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


def get_valid_dir_path(dir_path):
    """
    Checks that a directory exists. If the directory does not exist, it creates the directories in the path.

    :param dir_path: the path to the directory
    :type dir_path: str
    :return valid path to the directory
    :rtype str
    """

    dir_path = str(dir_path).strip()
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    return dir_path


def get_valid_file_path(file_path):
    """
    Checks that directories in a file path exist. If they do not exist, it creates the directories. Also generates
    a valid file name.

    :param file_path: the path to the file
    :type file_path: str
    :return the path to the file
    :rtype str
    """

    file_path = str(file_path).strip()
    if not os.path.exists(os.path.dirname(file_path)):
        if os.path.dirname(file_path):
            os.makedirs(os.path.dirname(file_path))

    file_path = os.path.join(os.path.dirname(file_path), get_valid_file_name(os.path.basename(file_path)))

    return file_path


def get_valid_file_name(file_name):
    """
    Generates a valid file name from an input file name.

    :param file_name: the original file name
    :type file_name: str
    :return the valid file name
    :rtype str
    """

    file_name = str(file_name).strip()
    file_name.replace(' ', '_')
    file_name = re.sub(r'(?u)[^-\w.]', '', file_name)

    return file_name


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
        'id', 'triples_map_id', 'source_name', 'source_type', 'data_source', 'ref_form', 'iterator', 'tablename',
        'query', 'subject_template', 'subject_reference', 'subject_constant', 'subject_termtype']
    ]

    subject_maps_df = subject_maps_df.drop_duplicates()

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

    f = open(config.get_output_file_path(mapping_partition), 'a')
    if config.only_write_printable_characters():
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

    output_dir = config.get_output_dir()
    output_file = config.get_output_file()
    if output_file:
        if os.path.exists(os.path.join(output_dir, output_file)):
            # always delete output file, so that generated triples are not appended to it
            os.remove(os.path.join(output_dir, output_file))

    if config.clean_output_dir():
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
