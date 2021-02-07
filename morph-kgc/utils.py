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
        'triples_map_id', 'data_source', 'ref_form', 'iterator', 'tablename', 'query', 'subject_template',
        'subject_reference', 'subject_constant', 'subject_termtype',
        'graph_constant', 'graph_reference', 'graph_template']
    ]

    subject_maps_df = subject_maps_df.drop_duplicates()

    if len(list(subject_maps_df['triples_map_id'])) > len(set(subject_maps_df['triples_map_id'])):
        raise Exception('One or more triples maps have incongruencies in subject maps.')

    return subject_maps_df


def get_references_in_template(template):
    """
    Retrieves all reference identifiers in a template-valued term map.

    :param template: template-valued term map
    :type template: str
    :return list of references in template
    :rtype list
    """

    references = re.findall('\\{([^}]+)', template)

    return list(set(references))


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
