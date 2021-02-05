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

    if mapping_partition:
        # use mapping partition as file name or get the file name from config
        if config.get('CONFIGURATION', 'output_file'):
            # if final output will be in just one file use a tmp dir for the intermediate files created
            file_path = os.path.join(config.get('CONFIGURATION', 'output_dir'), 'tmp', mapping_partition)
        else:
            file_path = os.path.join(config.get('CONFIGURATION', 'output_dir'), mapping_partition)
    else:
        # no mapping partitions
        if config.get('CONFIGURATION', 'output_file'):
            file_path = os.path.join(config.get('CONFIGURATION', 'output_dir'),
                                     config.get('CONFIGURATION', 'output_file'))
            # remove file extension, we will set it based on the output format
            if file_path.endswith('.nt') or file_path.endswith('.nq'):
                file_path = file_path[:-3]
        else:
            # if no output file, use 'result' as file name
            file_path = os.path.join(config.get('CONFIGURATION', 'output_dir'), 'result')

    if config.get('CONFIGURATION', 'output_format') == 'ntriples':
        file_path += '.nt'
    elif config.get('CONFIGURATION', 'output_format') == 'nquads':
        file_path += '.nq'

    f = open(file_path, 'w')
    if config.getboolean('CONFIGURATION', 'only_printable_characters'):
        for triple in triples:
            # REMOVING NON PRINTABLE CHARACTERS THIS WAY IS VERY SLOW!
            f.write(''.join(c for c in triple if c.isprintable()) + '.\n')
    else:
        for triple in triples:
            f.write(triple + '.\n')
    f.close()

    if mapping_partition:
        logging.info(str(len(triples)) + " triples generated for mapping partition " + mapping_partition + ".")


def unify_triple_files(config):
    """
    Unifies in a unique file all the temporary files with triples created during materialization.

    :param config: config object
    :type config: ConfigParser
    """

    # if there is output_file then unify, if not, there is nothing to unify
    output_dir = config.get('CONFIGURATION', 'output_dir')
    if config.get('CONFIGURATION', 'output_file'):
        with open(os.path.join(output_dir, config.get('CONFIGURATION', 'output_file')), 'wb') as wfd:
            for f in os.listdir(os.path.join(output_dir, 'tmp')):
                with open(os.path.join(output_dir, 'tmp', f), 'rb') as fd:
                    shutil.copyfileobj(fd, wfd)
                    os.remove(os.path.join(output_dir, 'tmp', f))
        os.rmdir(os.path.join(output_dir, 'tmp'))

        logging.debug('Unified temporary files for mapping partitions results.')


def prepare_output_dir(config, num_mapping_partitions):
    """
    Removes all files and directories withing output_dir in config. Also generates temporary directory if necessary.

    :param config: config object
    :type config: ConfigParser
    :param num_mapping_partitions: number of mapping partitions used during materialization
    :type num_mapping_partitions: int
    """

    output_dir = config.get('CONFIGURATION', 'output_dir')

    for obj in os.listdir(output_dir):
        obj_path = os.path.join(output_dir, obj)
        if os.path.isdir(obj_path):
            shutil.rmtree(obj_path)
        else:
            os.remove(obj_path)

    logging.debug('Cleaned output directory.')

    if num_mapping_partitions > 1 and config.get('CONFIGURATION', 'output_file'):
        # if mapping partitions and unique output file, then temporary dir will be necessary
        os.makedirs(os.path.join(output_dir, 'tmp'))
        logging.debug('Using temporary directory for mapping partitions results.')


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
