__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import argparse
import os

from configparser import ExtendedInterpolation

from .utils import configure_logger
from .config import Config
from ._version import __version__


def _existing_file_path(file_path):
    file_path = str(file_path)
    if not os.path.isfile(file_path):
        raise argparse.ArgumentTypeError(f'{file_path} is not a valid file path.')

    return file_path


def _parse_arguments():
    """
    Parses command line arguments.
    """

    parser = argparse.ArgumentParser(
        description='Generate Knowledge Graphs from Heterogeneous Data Sources.',
        epilog="Copyright © 2020 Julián Arenas-Guerrero",
        allow_abbrev=False,
        prog='python3 -m morph_kgc',
        argument_default=argparse.SUPPRESS
    )

    parser.add_argument('config', type=_existing_file_path, help='path to the configuration file')
    parser.add_argument('-v', '--version', action='version',
                        version=f'Morph-KGC {__version__} | Copyright © 2020 Julián Arenas-Guerrero')

    return parser.parse_args()


def _parse_config(config):
    """
    Parses the config file. Logger is configured.
    """

    config.complete_configuration_with_defaults()
    config.validate_configuration_section()

    configure_logger(config.get_logging_level(), config.get_logging_file())
    config.log_config_info()

    return config


def load_config_from_command_line():
    """
    Parses command line arguments.
    """

    args = _parse_arguments()

    config = Config(interpolation=ExtendedInterpolation())
    config.read(args.config)

    config = _parse_config(config)

    return config


def load_config_from_argument(config_entry):
    """
    Parses a config argument. It can be a file path or a string.
    """

    config = Config(interpolation=ExtendedInterpolation())
    if os.path.isfile(config_entry):
        config.read(config_entry)
    else:
        # it is a string
        config.read_string(config_entry)

    config = _parse_config(config)

    return config
