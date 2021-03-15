""" Morph-KGC """

__version__ = "0.0.1"

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import argparse
import os
import re
import logging
import multiprocessing as mp

from configparser import ConfigParser, ExtendedInterpolation

from data_sources import relational_source


ARGUMENTS_DEFAULT = {
    'output_dir': 'output',
    'output_file': 'result',
    'output_format': 'nquads',
    'remove_duplicates': 'yes',
    'clean_output_dir': 'yes',
    'mapping_partitions': 'guess',
    'input_parsed_mappings_path': '',
    'output_parsed_mappings_path': '',
    'logs_file': '',
    'logging_level': 'info',
    'push_down_sql_distincts': 'no',
    'number_of_processes': mp.cpu_count(),
    'process_start_method': 'default',
    'async': 'no',
    'chunksize': 100000,
    'infer_datatypes': 'yes',
    'coerce_float': 'no',
    'only_printable_characters': 'no'
}


VALID_ARGUMENTS = {
    'output_format': ['ntriples', 'nquads'],
    'mapping_partitions': 'spog',
    'relational_source_type': ['mysql', 'postgresql', 'oracle', 'sqlserver'],
    'file_source_type': [],
    'process_start_method': ['default', 'spawn', 'fork', 'forkserver'],
    'logging_level': ['notset', 'debug', 'info', 'warning', 'error', 'critical']
}


def _configure_logger(config):
    """
    Configures the logger based on input arguments. If no logging argument is provided, log level is set to WARNING.

    :param config: the ConfigParser object
    :type config: ConfigParser
    """

    # get the logging level numeric value
    logging_level = config.get('CONFIGURATION', 'logging_level')
    if logging_level == 'critical':
        logging_level = logging.CRITICAL
    elif logging_level == 'error':
        logging_level = logging.ERROR
    elif logging_level == 'warning':
        logging_level = logging.WARNING
    elif logging_level == 'info':
        logging_level = logging.INFO
    elif logging_level == 'debug':
        logging_level = logging.DEBUG
    elif logging_level == 'notset':
        logging_level = logging.NOTSET

    if config.get('CONFIGURATION', 'logs_file') == '':
        logging.basicConfig(format='%(levelname)s | %(asctime)s | %(message)s', level=logging_level)
    else:
        logging.basicConfig(filename=config.get('CONFIGURATION', 'logs_file'),
                            format='%(levelname)s | %(asctime)s | %(message)s', filemode='w',
                            level=logging_level)


def _log_parsed_configuration_and_data_sources(config):
    """
    Logs configuration and data sources parsed from the command line arguments and the config file.

    :param config: ConfigParser object
    :type config: ConfigParser
    """

    logging.debug('CONFIGURATION: ' + str(dict(config.items('CONFIGURATION'))))

    data_sources = {}
    for section in config.sections():
        if section != 'CONFIGURATION':
            ''' if section is not configuration then it is a data source.
                Mind that DEFAULT section is not triggered with config.sections(). '''
            data_sources[section] = dict(config.items(section))

    logging.debug('DATA SOURCES: ' + str(data_sources))


def _dir_path(dir_path):
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


def _file_path(file_path):
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

    file_path = os.path.join(os.path.dirname(file_path), _file_name(os.path.basename(file_path)))

    return file_path


def _file_name(file_name):
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


def _existing_file_path(file_path):
    """
    Checks if a file exists.

    :param file_path: the file path
    :type file_path: str
    :return the file path
    :rtype str
    """

    file_path = str(file_path).strip()
    if not os.path.isfile(file_path):
        raise argparse.ArgumentTypeError("%r is not a valid file path." % file_path)

    return file_path


def _natural_number(number, including_zero=False):
    """
    Generates a natural number from a given number.

    :param number: number
    :type number: str
    :param including_zero: whether to consider zero a valid natural number.
    :type including_zero: bool
    :return natural number
    :rtype int
    """

    whole_number = int(number)
    if including_zero and whole_number < 0:
        raise ValueError("value must be '0' or greater.")
    elif not including_zero and whole_number <= 0:
        raise ValueError("Value must be greater than '0'.")

    return whole_number


def _validate_config_data_sources_sections(config):
    """
    Validates that the data sources section in the config file are correct.

    :param config: config object
    :type config: ConfigParser
    :return config object with validated data sources sections
    :rtype configparser
    """
    for section in config.sections():
        if section != 'CONFIGURATION':
            # if section is not CONFIGURATION then it is a data source
            # mind that DEFAULT section is not triggered with config.sections().

            config.set(section, 'source_type', config.get(section, 'source_type').lower())
            if config.get(section, 'source_type') in VALID_ARGUMENTS['relational_source_type']:
                # to check that required parameters are provided and that the connection is ok
                relational_source.relational_db_connection(config, section)
            else:
                raise ValueError("'source_type' value '" + config.get(section, 'source_type') + "' provided in section '" + section +
                                 "' of config file is not valid. 'source_type' value must be in: " + str(VALID_ARGUMENTS['relational_source_type']) + '.')

    return config


def _validate_config_configuration_section(config):
    """
    Validates that the configuration section in the config file is correct.

    :param config: config object
    :type config: ConfigParser
    :return config object with validated configuration section
    :rtype configparser
    """

    output_format = config.get('CONFIGURATION', 'output_format')
    output_format = str(output_format).lower()
    if output_format not in VALID_ARGUMENTS['output_format']:
        raise ValueError("Value for option 'output_format' in config must be in: " + str(VALID_ARGUMENTS['output_format']))
    config.set('CONFIGURATION', 'output_format', output_format)

    config.set('CONFIGURATION', 'output_dir', _dir_path(config.get('CONFIGURATION', 'output_dir')))
    config.set('CONFIGURATION', 'output_file', _file_name(config.get('CONFIGURATION', 'output_file')))

    mapping_partitions = config.get('CONFIGURATION', 'mapping_partitions')
    mapping_partitions = str(mapping_partitions).lower()
    if mapping_partitions != 'guess' and not (set(mapping_partitions) <= set(VALID_ARGUMENTS['mapping_partitions'])):
        raise ValueError('Option mapping_partitions must be `guess`, empty, or a subset of `' +
                         VALID_ARGUMENTS['mapping_partitions'] + '`.')
    config.set('CONFIGURATION', 'mapping_partitions', mapping_partitions)

    config.set('CONFIGURATION', 'number_of_processes',
               str(_natural_number(config.get('CONFIGURATION', 'number_of_processes'))))
    config.set('CONFIGURATION', 'chunksize', str(_natural_number(config.get('CONFIGURATION', 'chunksize'))))
    config.getboolean('CONFIGURATION', 'coerce_float')
    config.getboolean('CONFIGURATION', 'only_printable_characters')
    config.getboolean('CONFIGURATION', 'infer_datatypes')
    config.getboolean('CONFIGURATION', 'push_down_sql_distincts')
    config.getboolean('CONFIGURATION', 'remove_duplicates')
    config.getboolean('CONFIGURATION', 'clean_output_dir')
    config.getboolean('CONFIGURATION', 'async')

    config.set('CONFIGURATION', 'output_parsed_mappings_path',
               _file_path(config.get('CONFIGURATION', 'output_parsed_mappings_path')))
    config.set('CONFIGURATION', 'logs_file', _file_path(config.get('CONFIGURATION', 'logs_file')))

    config.set('CONFIGURATION', 'logging_level', config.get('CONFIGURATION', 'logging_level').lower())
    if config.get('CONFIGURATION', 'logging_level') not in VALID_ARGUMENTS['logging_level']:
        raise ValueError("Value for option 'logging_level' in config must be in: " + str(VALID_ARGUMENTS['logging_level']))

    config.set('CONFIGURATION', 'process_start_method', config.get('CONFIGURATION', 'process_start_method').lower())
    if config.get('CONFIGURATION', 'process_start_method') not in VALID_ARGUMENTS['process_start_method']:
        raise ValueError("Value for option 'process_start_method' in config must be in: " + str(VALID_ARGUMENTS['process_start_method']))
    return config


def _complete_config_file_with_defaults(config):
    """
    Completes missing options in the config file with default options.

    :param config: the ConfigParser object
    :type config: ConfigParser
    :return ConfigParser object extended with default options
    :rtype configparser
    """

    # create section CONFIGURATION if it does not exist in the config file
    if not config.has_section('CONFIGURATION'):
        config.add_section('CONFIGURATION')

    # if parameters are not provided in the config file, take them from arguments
    # mind that ConfigParser store options as strings
    if not config.has_option('CONFIGURATION', 'output_dir'):
        config.set('CONFIGURATION', 'output_dir', ARGUMENTS_DEFAULT['output_dir'])
    if not config.has_option('CONFIGURATION', 'output_file'):
        config.set('CONFIGURATION', 'output_file', ARGUMENTS_DEFAULT['output_file'])
    if not config.has_option('CONFIGURATION', 'output_format'):
        config.set('CONFIGURATION', 'output_format', ARGUMENTS_DEFAULT['output_format'])
    elif config.get('CONFIGURATION', 'output_format') == '':
        config.set('CONFIGURATION', 'output_format', str(ARGUMENTS_DEFAULT['output_format']))
    if not config.has_option('CONFIGURATION', 'push_down_sql_distincts'):
        config.set('CONFIGURATION', 'push_down_sql_distincts', ARGUMENTS_DEFAULT['push_down_sql_distincts'])
    elif config.get('CONFIGURATION', 'push_down_sql_distincts') == '':
        config.set('CONFIGURATION', 'push_down_sql_distincts', str(ARGUMENTS_DEFAULT['push_down_sql_distincts']))
    if not config.has_option('CONFIGURATION', 'mapping_partitions'):
        config.set('CONFIGURATION', 'mapping_partitions', ARGUMENTS_DEFAULT['mapping_partitions'])
    if not config.has_option('CONFIGURATION', 'number_of_processes'):
        config.set('CONFIGURATION', 'number_of_processes', str(ARGUMENTS_DEFAULT['number_of_processes']))
    elif config.get('CONFIGURATION', 'number_of_processes') == '':
        config.set('CONFIGURATION', 'number_of_processes', str(ARGUMENTS_DEFAULT['number_of_processes']))
    if not config.has_option('CONFIGURATION', 'async'):
        config.set('CONFIGURATION', 'async', str(ARGUMENTS_DEFAULT['async']))
    elif config.get('CONFIGURATION', 'async') == '':
        config.set('CONFIGURATION', 'async', str(ARGUMENTS_DEFAULT['async']))
    if not config.has_option('CONFIGURATION', 'remove_duplicates'):
        config.set('CONFIGURATION', 'remove_duplicates', str(ARGUMENTS_DEFAULT['remove_duplicates']))
    elif config.get('CONFIGURATION', 'remove_duplicates') == '':
        config.set('CONFIGURATION', 'remove_duplicates', str(ARGUMENTS_DEFAULT['remove_duplicates']))
    if not config.has_option('CONFIGURATION', 'clean_output_dir'):
        config.set('CONFIGURATION', 'clean_output_dir', str(ARGUMENTS_DEFAULT['clean_output_dir']))
    elif config.get('CONFIGURATION', 'clean_output_dir') == '':
        config.set('CONFIGURATION', 'clean_output_dir', str(ARGUMENTS_DEFAULT['clean_output_dir']))
    if not config.has_option('CONFIGURATION', 'chunksize'):
        config.set('CONFIGURATION', 'chunksize', str(ARGUMENTS_DEFAULT['chunksize']))
    elif config.get('CONFIGURATION', 'chunksize') == '':
        config.set('CONFIGURATION', 'chunksize', str(ARGUMENTS_DEFAULT['chunksize']))
    if not config.has_option('CONFIGURATION', 'coerce_float'):
        config.set('CONFIGURATION', 'coerce_float', ARGUMENTS_DEFAULT['coerce_float'])
    elif config.get('CONFIGURATION', 'coerce_float') == '':
        config.set('CONFIGURATION', 'coerce_float', str(ARGUMENTS_DEFAULT['coerce_float']))
    if not config.has_option('CONFIGURATION', 'only_printable_characters'):
        config.set('CONFIGURATION', 'only_printable_characters', ARGUMENTS_DEFAULT['only_printable_characters'])
    elif config.get('CONFIGURATION', 'only_printable_characters') == '':
        config.set('CONFIGURATION', 'only_printable_characters', str(ARGUMENTS_DEFAULT['only_printable_characters']))
    if not config.has_option('CONFIGURATION', 'infer_datatypes'):
        config.set('CONFIGURATION', 'infer_datatypes', ARGUMENTS_DEFAULT['infer_datatypes'])
    elif config.get('CONFIGURATION', 'infer_datatypes') == '':
        config.set('CONFIGURATION', 'infer_datatypes', str(ARGUMENTS_DEFAULT['infer_datatypes']))
    if not config.has_option('CONFIGURATION', 'input_parsed_mappings_path'):
        config.set('CONFIGURATION', 'input_parsed_mappings_path', ARGUMENTS_DEFAULT['input_parsed_mappings_path'])
    if not config.has_option('CONFIGURATION', 'output_parsed_mappings_path'):
        config.set('CONFIGURATION', 'output_parsed_mappings_path', ARGUMENTS_DEFAULT['output_parsed_mappings_path'])
    if not config.has_option('CONFIGURATION', 'logs_file'):
        config.set('CONFIGURATION', 'logs_file', ARGUMENTS_DEFAULT['logs_file'])
    if not config.has_option('CONFIGURATION', 'logging_level'):
        config.set('CONFIGURATION', 'logging_level', ARGUMENTS_DEFAULT['logging_level'])
    elif config.get('CONFIGURATION', 'logging_level') == '':
        config.set('CONFIGURATION', 'logging_level', str(ARGUMENTS_DEFAULT['logging_level']))
    if not config.has_option('CONFIGURATION', 'process_start_method'):
        config.set('CONFIGURATION', 'process_start_method', str(ARGUMENTS_DEFAULT['process_start_method']))
    elif config.get('CONFIGURATION', 'process_start_method') == '':
        config.set('CONFIGURATION', 'process_start_method', str(ARGUMENTS_DEFAULT['process_start_method']))

    return config


def _parse_arguments():
    """
    Parses command line arguments of the engine.

    :return parsed arguments
    """

    parser = argparse.ArgumentParser(
        description='Generate knowledge graphs from heterogeneous data sources.',
        epilog=__copyright__,
        allow_abbrev=False,
        argument_default=argparse.SUPPRESS
    )

    parser.add_argument('config', type=_existing_file_path, help='path to the configuration file')
    parser.add_argument('-v', '--version', action='version', version='Morph-KGC ' + __version__ + ' | ' + __copyright__)

    return parser.parse_args()


def parse_config():
    """
    Parses command line arguments and the config file. It also validates that provided values are correct.
    Arguments in the config file have more priority than command line arguments, if specified, command line
    arguments will overwrite config file ones. Logger is configured.

    :return config object populated with command line arguments and config file arguments,
    :rtype configparser
    """

    args = _parse_arguments()

    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read(args.config)
    config = _complete_config_file_with_defaults(config)

    config = _validate_config_configuration_section(config)
    _configure_logger(config)
    config = _validate_config_data_sources_sections(config)

    _log_parsed_configuration_and_data_sources(config)

    return config
