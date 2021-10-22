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
import time
import numpy as np
import pandas as pd


def configure_logger(logging_level, logging_file):
    """
    Configures the logger. If a logging file is provided, the logs messages are redirected to it, if not they are
    redirected to stdout. Messages are logged according to the provided logging level.
    """

    logging_level_string_to_numeric = {
        'CRITICAL': logging.CRITICAL,
        'ERROR': logging.ERROR,
        'WARNING': logging.WARNING,
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG,
        'NOTSET': logging.NOTSET,
    }

    if logging_file:
        logging.basicConfig(filename=logging_file,
                            format='%(levelname)s | %(asctime)s | %(message)s',
                            filemode='w',
                            level=logging_level_string_to_numeric[logging_level.upper()])
    else:
        logging.basicConfig(format='%(levelname)s | %(asctime)s | %(message)s',
                            level=logging_level_string_to_numeric[logging_level.upper()])


def get_valid_dir_path(dir_path):
    """
    Checks that a directory exists. If the directory does not exist, it creates the directories in the path.
    """

    dir_path = str(dir_path).strip()
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    return dir_path


def get_valid_file_path(file_path):
    """
    Checks that directories in a file path exist. If they do not exist, it creates the directories. Also generates
    a valid file name.
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
    """

    file_name = str(file_name).strip()
    file_name.replace(' ', '_')
    file_name = re.sub(r'(?u)[^-\w.]', '', file_name)

    return file_name


def get_repeated_elements_in_list(input_list):
    """
    Finds repeated elements in a list. Returns a list with the repeated elements.
    """

    elem_count = {}
    for elem in input_list:
        if elem in elem_count:
            elem_count[elem] += 1
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
    """

    subject_maps_df = mappings_df[[
        'id', 'triples_map_id', 'source_name', 'source_type', 'data_source', 'iterator', 'tablename',
        'query', 'subject_template', 'subject_reference', 'subject_constant', 'subject_termtype']
    ]

    subject_maps_df = subject_maps_df.drop_duplicates()

    return subject_maps_df


def get_references_in_template(template):
    """
    Retrieves all reference identifiers in a template-valued term map. References are returned in order of appearance
    """

    return re.findall('\\{([^}]+)', template)


def triples_to_file(triples, config, mapping_partition=''):
    """
    Writes triples to file.
    """

    f = open(config.get_output_file_path(mapping_partition), 'a')
    for triple in triples:
        f.write(triple + '.\n')
    f.close()


def remove_non_printable_characters(string):
    """
    Eliminates from the input string all the characters that are not printable.
    """

    return ''.join(char for char in string if char.isprintable())


def clean_output_dir(config):
    """
    Removes all files and directories within output_dir in config depending on clean_output_dir parameter. The output
    file, if provided in config, is always deleted.
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


def replace_predicates_in_graph(graph, predicate_to_remove, predicate_to_add):
    """
    Replaces the predicates predicate_to_remove in a graph with the predicate predicate_to_add.
    """

    # get the triples with the predicate to be replaced
    r2_rml_sources_query = 'SELECT ?s ?o WHERE {?s <' + predicate_to_remove + '> ?o .}'
    subjects_objects_matched = graph.query(r2_rml_sources_query)

    # for each triple to be replaced add a similar one (same subject and object) but with the new predicate
    for s, o in subjects_objects_matched:
        graph.add((s, rdflib.term.URIRef(predicate_to_add), o))

    # remove all triples in the graph that have the old predicate
    graph.remove((None, rdflib.term.URIRef(predicate_to_remove), None))

    return graph


def get_mapping_rule_from_triples_map_id(mappings, parent_triples_map_id):
    """
    Get the parent triples map of the mapping rule with the given id
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

    # if join_condition is not null and it is not empty
    if pd.notna(mapping_rule['join_conditions']) and mapping_rule['join_conditions']:
        join_conditions = eval(mapping_rule['join_conditions'])
        for join_condition in join_conditions.values():
            references.append(join_condition['child_value'])
            parent_references.append(join_condition['parent_value'])

    return references, parent_references


def remove_file_extension(file_path):
    # only removes the last file extension (if there are multiple dots in file_path)
    if len(os.path.splitext(file_path)) > 1:
        return os.path.splitext(file_path)[0]

    return file_path


def normalize_oracle_identifier_casing(dataframe, references):
    """
    This renames the columns of a DataFrame generated when querying Oracle. This is necessary as Oracle identifier
    casing is inconsistent with SQLAlchemy (https://docs.sqlalchemy.org/en/14/dialects/oracle.html#identifier-casing).
    This function addresses issue #37 (https://github.com/oeg-upm/Morph-KGC/issues/37).
    """

    lowercase_references = [reference.lower() for reference in references]
    identifier_normalization_dict = dict(zip(lowercase_references, references))

    # rename those columns matching lowercase references
    dataframe.rename(columns=identifier_normalization_dict, inplace=True)

    return dataframe


def remove_null_values_from_dataframe(dataframe, config):
    dataframe.replace(config.get_na_values(), np.NaN, inplace=True)
    dataframe.dropna(axis=0, how='any', inplace=True)

    return dataframe
