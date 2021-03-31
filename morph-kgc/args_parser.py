""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import argparse
import logging
import constants
import os

from config import Config
from configparser import ExtendedInterpolation


def configure_logger(logging_level, logs_file):
    logging_level_string_to_numeric = {
        'critical': logging.CRITICAL,
        'error': logging.ERROR,
        'warning': logging.WARNING,
        'info': logging.INFO,
        'debug': logging.DEBUG,
        'notset': logging.NOTSET,
    }

    if logs_file:
        logging.basicConfig(filename=logs_file,
                            format='%(levelname)s | %(asctime)s | %(message)s',
                            filemode='w',
                            level=logging_level_string_to_numeric[logging_level])
    else:
        logging.basicConfig(format='%(levelname)s | %(asctime)s | %(message)s',
                            level=logging_level_string_to_numeric[logging_level])


def _existing_file_path(file_path):
    """
    Checks if a file exists.
    """

    file_path = str(file_path).strip()
    if not os.path.isfile(file_path):
        raise argparse.ArgumentTypeError("%r is not a valid file path." % file_path)

    return file_path


def _parse_arguments():
    """
    Parses command line arguments of the engine.
    """

    parser = argparse.ArgumentParser(
        description='Generate Knowledge Graphs from Heterogeneous Data Sources.',
        epilog=constants.__copyright__,
        allow_abbrev=False,
        argument_default=argparse.SUPPRESS
    )

    parser.add_argument('config', type=_existing_file_path, help='path to the configuration file')
    parser.add_argument('-v', '--version', action='version',
                        version='Morph-KGC ' + constants.__version__ + ' | ' + constants.__copyright__)

    return parser.parse_args()


def parse_config():
    """
    Parses command line arguments and the config file. It also validates that provided values are correct.
    Arguments in the config file have more priority than command line arguments, if specified, command line
    arguments will overwrite config file ones. Logger is configured.
    """

    args = _parse_arguments()

    config = Config(interpolation=ExtendedInterpolation())
    config.read(args.config)

    config.complete_configuration_with_defaults()
    config.validate_configuration_section()
    config.validate_data_source_sections()

    configure_logger(config.get_configuration_option('logging_level'), config.get_configuration_option('logs_file'))

    config.log_info()

    return config
