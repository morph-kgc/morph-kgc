__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import re
import os
import logging
import rdflib
import time
import numpy as np
import pandas as pd
import multiprocessing as mp

from itertools import product
from .constants import AUXILIAR_UNIQUE_REPLACING_STRING


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

    # do not log messages from jsonpath-python library
    logging.getLogger('jsonpath').setLevel(logging.CRITICAL)


def create_dirs_in_path(file_path):
    """
    Checks that directories in a file path exist. If they do not exist, it creates the directories.
    """

    file_path = str(file_path).strip()
    if not os.path.exists(os.path.dirname(file_path)):
        if os.path.dirname(file_path):
            os.makedirs(os.path.dirname(file_path))


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


def get_mapping_rule(mappings_df, triples_map_id):
    """
    Retrieves mapping rule from mapping rules in the input DataFrame by its triples map id.
    """
    mapping_rule = mappings_df[mappings_df['triples_map_id'] == triples_map_id].iloc[0]
    return mapping_rule


def get_references_in_template(template):
    """
    Retrieves all reference identifiers in a template-valued term map. References are returned in order of appearance
    """

    # Curly braces that do not enclose column names MUST be escaped by a backslash character (“\”).
    # This also applies to curly braces within column names.
    template = template.replace('\\{', AUXILIAR_UNIQUE_REPLACING_STRING).replace('\\}', AUXILIAR_UNIQUE_REPLACING_STRING)
    references = re.findall('\\{([^}]+)', template)
    references = [reference.replace(AUXILIAR_UNIQUE_REPLACING_STRING, '\\{').replace(AUXILIAR_UNIQUE_REPLACING_STRING, '\\}') for reference in references]

    return references


def triples_to_file(triples, config, mapping_group=None):
    """
    Writes triples to file.
    """

    lock = mp.Lock()    # necessary for issue #65
    with lock:
        f = open(config.get_output_file_path(mapping_group), 'a', encoding='utf-8')
        for triple in triples:
            f.write(f'{triple} .\n')
        f.flush()
        os.fsync(f.fileno())
        f.close()


def remove_non_printable_characters(string):
    """
    Eliminates from the input string all the characters that are not printable.
    """

    return ''.join(char for char in string if char.isprintable())


def prepare_output_files(config, mappings_df):
    """
    Remove the files that will be used to store the final knowledge graph. If a file path contains directories that do
    not exist, they are created.
    """

    output_dir = config.get_output_dir()
    if output_dir:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        mapping_groups_names = set(mappings_df['mapping_partition'])
        for mapping_group_name in mapping_groups_names:
            mapping_group_file_path = config.get_output_file_path(mapping_group_name)
            if os.path.exists(mapping_group_file_path):
                # always delete output file, so that generated triples are not appended to it
                os.remove(mapping_group_file_path)
    else:
        output_file = config.get_output_file_path()
        create_dirs_in_path(output_file)

        if os.path.exists(output_file):
            # always delete output file, so that generated triples are not appended to it
            os.remove(output_file)


def replace_predicates_in_graph(graph, predicate_to_remove, predicate_to_add):
    """
    Replaces the predicates predicate_to_remove in a graph with the predicate predicate_to_add.
    """

    # get the triples with the predicate to be replaced
    r2_rml_sources_query = f'SELECT ?s ?o WHERE {{?s <{predicate_to_remove}> ?o .}}'
    subjects_objects_matched = graph.query(r2_rml_sources_query)

    # for each triple to be replaced add a similar one (same subject and object) but with the new predicate
    for s, o in subjects_objects_matched:
        graph.add((s, rdflib.term.URIRef(predicate_to_add), o))

    # remove all triples in the graph that have the old predicate
    graph.remove((None, rdflib.term.URIRef(predicate_to_remove), None))

    return graph


def get_delta_time(start_time):
    return "{:.3f}".format((time.time() - start_time))


def get_references_in_join_condition(mapping_rule, join_conditions):
    references = list()
    parent_references = list()

    # if join_condition is not null and it is not empty
    if pd.notna(mapping_rule[join_conditions]) and mapping_rule[join_conditions]:
        join_conditions = eval(mapping_rule[join_conditions])
        for join_condition in join_conditions.values():
            references.append(join_condition['child_value'])
            parent_references.append(join_condition['parent_value'])

    return references, parent_references


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


def remove_null_values_from_dataframe(dataframe, config, references):
    # data to str to be able to perform string replacement
    dataframe = dataframe.astype(str)

    if config.get_na_values():  # if there is some NULL values to replace
        dataframe.replace(config.get_na_values(), np.NaN, inplace=True)
        dataframe.dropna(axis=0, how='any', subset=references, inplace=True)

    return dataframe


def normalize_hierarchical_data(data):
    """
    This is taken from
    https://stackoverflow.com/questions/36731480/flatten-nested-json-dict-list-into-list-to-prepare-to-write-into-db#answer-43173998
    """
    if isinstance(data, dict):
        keys = data.keys()
        values = (normalize_hierarchical_data(i) for i in data.values())
        for i in product(*values):
            yield (dict(zip(keys, i)))
    elif isinstance(data, list):
        for i in data:
            yield from normalize_hierarchical_data(i)
    else:
        yield data
