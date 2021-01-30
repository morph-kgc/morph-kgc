""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__version__ = "0.1"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"
__status__ = 'Prototype'


import argparse
import os
import re
import logging
import multiprocessing as mp

from configparser import ConfigParser, ExtendedInterpolation


ARGUMENTS_DEFAULT = {
    'output_dir': 'output',
    'output_file': '',
    'output_format': 'ntriples',
    'mapping_partitions': '',
    'push_down_distincts': 'no',
    'number_of_processes': 0,
    'chunksize': 0,
    'infer_datatypes': 'yes',
    'coerce_float': 'no'
}

VALID_ARGUMENTS = {
    'output_format': ['ntriples', 'nquads'],
    'mapping_partitions': ['', 's', 'p', 'g', 'sp', 'sg', 'pg', 'spg'],
    'source_type': ['mysql', 'postgresql', 'oracle', 'sqlserver']
}


def _configure_logger(config, level=logging.INFO):
    """
    Configures the logger based on input arguments. If no logging argument is provided, log level is set to WARNING.

    :param config: the ConfigParser object
    :type config: configparser
    :param level: logging level to use
    """

    if config.has_option('CONFIGURATION', 'logs'):
        logging.basicConfig(filename=config.get('CONFIGURATION', 'logs'),
                            format='%(levelname)s | %(asctime)s | %(message)s', filemode='w', level=level)
    else:
        logging.basicConfig(level=logging.WARNING)


def _log_parsed_configuration_and_data_sources(config):
    """
    Logs configuration and data sources parsed from the command line arguments and the config file.

    :param config: ConfigParser object
    :type config: configparser
    """

    logging.info('CONFIGURATION: ' + str(dict(config.items('CONFIGURATION'))))

    data_sources = {}
    for section in config.sections():
        if section != 'CONFIGURATION':
            ''' if section is not configuration then it is a data source.
                Mind that DEFAULT section is not triggered with config.sections(). '''
            data_sources[section] = dict(config.items(section))

    logging.info('DATA SOURCES: ' + str(data_sources))


def _dir_path(dir_path):
    """
    Checks that a directory exists. If the directory does not exist, it creates the directories in the path.

    :param dir_path: the path to the directory
    :type dir_path: str
    :return valid path to the directory
    :rtype str
    """

    dir_path = str(dir_path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    return dir_path


def _file_path(file_path):
    """
    Checks that directories in a file path exist. If they do not exist, it creates the directories.

    :param file_path: the path to the file
    :type file_path: str
    :return the path to the file
    :rtype str
    """

    file_path = str(file_path).strip()
    if not os.path.exists(os.path.dirname(file_path)):
        if os.path.dirname(file_path):
            os.makedirs(os.path.dirname(file_path))

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


def _processes_number(number):
    """
    Generates a natural number from a given number. Number is converted to int.
    In case of been 0, the number of cores in the system is generated.

    :param number: number
    :type number: str
    :return natural number
    :rtype int
    """

    whole_number = int(number)
    if whole_number < 0:
        raise ValueError
    elif whole_number == 0:
        whole_number = mp.cpu_count()

    return whole_number


def _natural_number_including_zero(number):
    """
    Generates a natural number (including zero) from a given number.

    :param number: number
    :type number: str
    :return natural number (including zero)
    :rtype int
    """

    whole_number = int(number)
    if whole_number < 0:
        raise ValueError

    return whole_number


def _validate_config_data_sources_sections(config):
    for section in config.sections():
        if section !=  'CONFIGURATION':
            ''' if section is not configuration then it is a data source.
                Mind that DEFAULT section is not triggered with config.sections(). '''

            mapping_files = config.get(section, 'mapping_files')
            for mapping_file in mapping_files.split(','):
                if not os.path.exists(mapping_file.strip()):
                    raise FileNotFoundError('mapping_file=' + str(mapping_file) + ' in section ' + section +
                                            ' of config file could not be found.')

            if config.get(section, 'source_type').lower() in VALID_ARGUMENTS['source_type']:
                config.set(section, 'source_type', config.get(section, 'source_type').lower())
                # config.get to check that required parameters are provided
                config.get(section, 'user')
                config.get(section, 'password')
                config.get(section, 'host')
                config.get(section, 'port')
                config.get(section, 'db')
            else:
                raise ValueError('source_type=' + config.get(section, 'source_type') + ' in section ' + section +
                                 ' of config file is not valid.')

    return config


def _validate_config_configuration_section(config):
    """
    Validates that the configuration section in the config file is correct.

    :param config: config object
    :type config: configparser
    :return config object with validated configuration section
    :rtype configparser
    """

    config.set('CONFIGURATION', 'output_dir', _dir_path(config.get('CONFIGURATION', 'output_dir')))

    # output_file has no default value, it is needed to check if it is in the config
    if config.has_option('CONFIGURATION', 'output_file'):
        config.set('CONFIGURATION', 'output_file', _file_name(config.get('CONFIGURATION', 'output_file')))

    output_format = config.get('CONFIGURATION', 'output_format')
    output_format = str(output_format).lower().strip()
    if output_format not in VALID_ARGUMENTS['output_format']:
        raise ValueError('Option output_format must be in: ' + VALID_ARGUMENTS['output_format'])
    config.set('CONFIGURATION', 'output_format', output_format)

    config.getboolean('CONFIGURATION', 'push_down_distincts')

    mapping_partitions = config.get('CONFIGURATION', 'mapping_partitions')
    mapping_partitions = str(mapping_partitions).lower().strip()
    if mapping_partitions not in VALID_ARGUMENTS['mapping_partitions']:
        raise ValueError('Option mapping_partitions must be in: ' + str(VALID_ARGUMENTS['mapping_partitions']))
    elif output_format == 'nquads' and 'g' in mapping_partitions:
        raise Exception('Option mapping_partitions is "' + mapping_partitions + '", but graphs cannot be used as '
                        'mapping partition criteria if output_format is nquads.')
    config.set('CONFIGURATION', 'mapping_partitions', mapping_partitions)

    config.set('CONFIGURATION', 'number_of_processes',
               str(_processes_number(config.get('CONFIGURATION', 'number_of_processes'))))

    config.set('CONFIGURATION', 'chunksize',
               str(_natural_number_including_zero(config.get('CONFIGURATION', 'chunksize'))))

    config.getboolean('CONFIGURATION', 'coerce_float')
    config.getboolean('CONFIGURATION', 'infer_datatypes')

    # logs has no default value, it is needed to check if it is in the config
    if config.has_option('CONFIGURATION', 'logs'):
        config.set('CONFIGURATION', 'logs', _file_path(config.get('CONFIGURATION', 'logs')))

    return config


def _complete_config_file_with_args(config, args):
    """
    Completes missing options in the config file with the options provided via arguments.
    Options specified in the config file are prioritized, i.e., if the option is specified in
    the config file the option in the arguments is ignored.

    :param config: the ConfigParser object
    :type config: argparse
    :param args: the argparse object
    :type args: configparser
    :return ConfigParser object extended with information from arguments
    :rtype configparser
    """

    ''' Create section CONFIGURATION if it does not exist in the config file '''
    if not config.has_section('CONFIGURATION'):
        config.add_section('CONFIGURATION')

    ''' If parameters are not provided in the config file, take them from arguments.
        mind that ConfigParser store options as strings'''
    if not config.has_option('CONFIGURATION', 'output_dir'):
        config.set('CONFIGURATION', 'output_dir', args.output_dir)
    if not config.has_option('CONFIGURATION', 'output_file'):
        config.set('CONFIGURATION', 'output_file', args.output_file)
    if not config.has_option('CONFIGURATION', 'output_format'):
        config.set('CONFIGURATION', 'output_format', args.output_format)
    if not config.has_option('CONFIGURATION', 'push_down_distincts'):
        config.set('CONFIGURATION', 'push_down_distincts', ARGUMENTS_DEFAULT['push_down_distincts'])
        config.set('CONFIGURATION', 'push_down_distincts', ARGUMENTS_DEFAULT['push_down_distincts'])
    if not config.has_option('CONFIGURATION', 'mapping_partitions'):
        config.set('CONFIGURATION', 'mapping_partitions', args.mapping_partitions)
    if not config.has_option('CONFIGURATION', 'number_of_processes'):
        config.set('CONFIGURATION', 'number_of_processes', str(ARGUMENTS_DEFAULT['number_of_processes']))
    if not config.has_option('CONFIGURATION', 'chunksize'):
        config.set('CONFIGURATION', 'chunksize', str(ARGUMENTS_DEFAULT['chunksize']))
    if not config.has_option('CONFIGURATION', 'coerce_float'):
        config.set('CONFIGURATION', 'coerce_float', ARGUMENTS_DEFAULT['coerce_float'])
    if not config.has_option('CONFIGURATION', 'infer_datatypes'):
        config.set('CONFIGURATION', 'infer_datatypes', ARGUMENTS_DEFAULT['infer_datatypes'])
    if not config.has_option('CONFIGURATION', 'logs'):
        if 'logs' in args:
            config.set('CONFIGURATION', 'logs', args.logs)

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

    parser.add_argument('config', type=_existing_file_path,
                        help='Path to the configuration file.')
    parser.add_argument('-d', '--output_dir', default=ARGUMENTS_DEFAULT['output_dir'], type=str,
                        help='Path to the directory storing the results.')
    parser.add_argument('-o', '--output_file', default=ARGUMENTS_DEFAULT['output_file'], type=str,
                        help='If a file name is specified, all the results will be stored in this file. '
                             'If no file is specified the results will be stored in multiple files.')
    parser.add_argument('-f', '--output_format', default=ARGUMENTS_DEFAULT['output_format'], type=str,
                        choices=VALID_ARGUMENTS['output_format'],
                        help='Output serialization format.')
    parser.add_argument('-p', '--mapping_partitions', nargs='?', default=ARGUMENTS_DEFAULT['mapping_partitions'],
                        const='sp', choices=VALID_ARGUMENTS['mapping_partitions'],
                        help='Partitioning criteria for mappings. s for using subjects, p for using predicates, '
                             'g for using graphs.')
    parser.add_argument('-l', '--logs', nargs='?', const='', type=str,
                        help='File path to write logs to. If no path is provided logs are redirected to stdout.')
    parser.add_argument('-v', '--version', action='version', version='Morph-KGC' + __version__ + ' | ' +
                                                                     __copyright__)

    return parser.parse_args()


def parse_config():
    """
    Parses command line arguments and the config file. It also validates that values are correct.
    Arguments in the config file have more priority than command line arguments, if specified, command line
    arguments will overwrite config file ones. Logger is configured.

    :return config object populated with command line arguments and config file arguments,
    :rtype configparser
    """

    args = _parse_arguments()

    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read(args.config)
    config = _complete_config_file_with_args(config, args)

    config = _validate_config_configuration_section(config)
    _configure_logger(config)
    config = _validate_config_data_sources_sections(config)

    _log_parsed_configuration_and_data_sources(config)

    return config
