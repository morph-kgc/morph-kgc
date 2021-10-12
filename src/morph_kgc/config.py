__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import errno
import os
import logging

from configparser import ConfigParser

from .constants import *
from .utils import get_valid_dir_path, get_valid_file_name, get_valid_file_path, remove_file_extension


CONFIGURATION_SECTION = 'CONFIGURATION'


##############################################################################
########################   CONFIGURATION PARAMETERS   ########################
##############################################################################

NA_FILTER = 'na_filter'
NA_VALUES = 'na_values'

OUTPUT_DIR = 'output_dir'
OUTPUT_FILE = 'output_file'
OUTPUT_FORMAT = 'output_format'
CLEAN_OUTPUT_DIR = 'clean_output_dir'
ONLY_PRINTABLE_CHARACTERS = 'only_printable_characters'

MAPPING_PARTITION = 'mapping_partition'
INFER_SQL_DATATYPES = 'infer_sql_datatypes'

CHUNKSIZE = 'chunksize'

NUMBER_OF_PROCESSES = 'number_of_processes'

LOGGING_LEVEL = 'logging_level'
LOGGING_FILE = 'logging_file'

ORACLE_CLIENT_LIB_DIR = 'oracle_client_lib_dir'
ORACLE_CLIENT_CONFIG_DIR = 'oracle_client_config_dir'

READ_PARSED_MAPPINGS_PATH = 'read_parsed_mappings_path'
WRITE_PARSED_MAPPINGS_PATH = 'write_parsed_mappings_path'


##############################################################################
#########################   DATA SOURCE PARAMETERS   #########################
##############################################################################

SOURCE_TYPE = 'source_type'
MAPPINGS = 'mappings'
DATABASE_URL = 'db_url'


##############################################################################
########################   PARAMETERS DEFAULT VALUES   #######################
##############################################################################

# input parameters that are not to be completed with default value if they are empty
CONFIGURATION_OPTIONS_EMPTY_VALID = {
            OUTPUT_FILE: DEFAULT_OUTPUT_FILE,
            NA_VALUES: DEFAULT_NA_VALUES,
            READ_PARSED_MAPPINGS_PATH: DEFAULT_READ_PARSED_MAPPINGS_PATH,
            WRITE_PARSED_MAPPINGS_PATH: DEFAULT_WRITE_PARSED_MAPPINGS_PATH,
            MAPPING_PARTITION: PARTIAL_AGGREGATIONS_PARTITIONING,
            LOGGING_FILE: DEFAULT_LOGGING_FILE,
            ORACLE_CLIENT_LIB_DIR: DEFAULT_ORACLE_CLIENT_LIB_DIR,
            ORACLE_CLIENT_CONFIG_DIR: DEFAULT_ORACLE_CLIENT_LIB_DIR,
        }


# input parameters that are to be replaced with the default vale if they are empty
CONFIGURATION_OPTIONS_EMPTY_NON_VALID = {
            OUTPUT_DIR: DEFAULT_OUTPUT_DIR,
            OUTPUT_FORMAT: DEFAULT_OUTPUT_FORMAT,
            CLEAN_OUTPUT_DIR: DEFAULT_CLEAN_OUTPUT_DIR,
            ONLY_PRINTABLE_CHARACTERS: DEFAULT_ONLY_PRINTABLE_CHARACTERS,
            INFER_SQL_DATATYPES: DEFAULT_INFER_SQL_DATATYPES,
            CHUNKSIZE: DEFAULT_CHUNKSIZE,
            LOGGING_LEVEL: DEFAULT_LOGGING_LEVEL,
            NA_FILTER: DEFAULT_NA_FILTER,
            NUMBER_OF_PROCESSES: DEFAULT_NUMBER_OF_PROCESSES
        }


def _is_option_provided(config, option, empty_value_is_valid=False):
    """
    Checks whether a parameter is provided in the config. If empty value is not valid then the option will be considered
    as not provided if no value is provided for the option.
    """

    if not config.has_configuration_option(option):
        return False
    elif (config.get_configuration_option(option) == '') and (empty_value_is_valid is False):
        return False
    return True


class Config(ConfigParser):

    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)

        self.configuration_section = CONFIGURATION_SECTION

    def complete_configuration_with_defaults(self):
        """
        Sets the parameters that are not specified in the config file to their corresponding default values.
        If there is no CONFIGURATION section in the config file it is also added.
        """

        # add configuration section if its does not exist
        if not self.has_section(self.configuration_section):
            self.add_section(self.configuration_section)

        # set default values for parameters that accept empty values, i.e. if the parameter is provided but no value
        # is provided the default value is not set, if the parameter itself is not provided the default parameter is set
        for configuration_option, configuration_option_default in CONFIGURATION_OPTIONS_EMPTY_VALID.items():
            if not _is_option_provided(self, configuration_option, empty_value_is_valid=True):
                self.set(self.configuration_section, configuration_option, str(configuration_option_default))
        # set default values for parameters that do not accept empty values, i.e. if the parameter is provided but no
        # value is provided the default value is set, if the parameter itself is not provided the default parameter is
        # also set
        for configuration_option, configuration_option_default in CONFIGURATION_OPTIONS_EMPTY_NON_VALID.items():
            if not _is_option_provided(self, configuration_option):
                self.set(self.configuration_section, configuration_option, str(configuration_option_default))

    def validate_configuration_section(self):
        # OUTPUT DIR & FILE, WRITE PARSED MAPPINGS FILE, LOGS FILE
        self.set_output_dir(get_valid_dir_path(self.get_output_dir()))
        self.set_output_file(get_valid_file_name(self.get_output_file()))
        self.set_parsed_mappings_write_path(get_valid_file_path(self.get_parsed_mappings_write_path()))
        self.set_logging_file(get_valid_file_path(self.get_logging_file()))

        # OUTPUT FORMAT
        output_format = str(self.get_output_format()).upper()
        self.set_output_format(output_format)
        if output_format not in VALID_OUTPUT_FORMATS:
            raise ValueError(OUTPUT_FORMAT + ' value `' + self.get_output_format() +
                             '` is not valid. It must be in: ' + str(VALID_OUTPUT_FORMATS) + '.')

        # LOGGING LEVEL
        logging_level = str(self.get_logging_level()).upper()
        self.set_logging_level(logging_level)
        if logging_level not in VALID_LOGGING_LEVEL:
            raise ValueError(LOGGING_LEVEL + ' value `' + self.get_logging_level() +
                             '` is not valid. It must be in: ' + str(VALID_LOGGING_LEVEL) + '.')

        # MAPPING PARTITIONING
        mapping_partitioning = str(self.get_mapping_partition()).upper()
        self.set_mapping_partition(mapping_partitioning)
        if mapping_partitioning not in NO_PARTITIONING + [PARTIAL_AGGREGATIONS_PARTITIONING] + [
                MAXIMAL_PARTITIONING]:
            raise ValueError(
                MAPPING_PARTITION + ' value `' + self.get_mapping_partition() + '` is not valid. It must be in: ' + str(
                    [MAXIMAL_PARTITIONING] + [
                        PARTIAL_AGGREGATIONS_PARTITIONING] + NO_PARTITIONING) + '.')

    def validate_data_source_sections(self):
        # SOURCE TYPE
        for section in self.get_data_sources_sections():
            if self.has_source_type(section):
                self.set(section, SOURCE_TYPE, self.get_source_type(section).upper())
                if self.get_source_type(section) not in DATA_SOURCE_TYPES:
                    raise ValueError(SOURCE_TYPE + ' value `' + self.get_source_type(section) + ' is not valid. '
                                     'It must be in: ' + str(DATA_SOURCE_TYPES) + '.')
            else:
                if self.has_database_url(section):
                    self.set(section, SOURCE_TYPE, RDB)

    def log_config_info(self):
        logging.debug('CONFIGURATION: ' + str(dict(self.items(self.configuration_section))))

        for data_source_section in self.get_data_sources_sections():
            logging.debug('DATA SOURCE `' + data_source_section + '`: ' + str(dict(self.items(data_source_section))))

    #################################################################################
    #######################   CONFIGURATION SECTION METHODS   #######################
    #################################################################################

    def has_configuration_option(self, option):
        return self.has_option(self.configuration_section, option)

    def has_multiple_data_sources(self):
        return len(self.get_data_sources_sections()) > 1

    def is_multiprocessing_enabled(self):
        return self.getint(self.configuration_section, NUMBER_OF_PROCESSES) > 1

    def is_read_parsed_mappings_file_provided(self):
        if self.get(self.configuration_section, READ_PARSED_MAPPINGS_PATH):
            return True
        else:
            return False

    def is_write_parsed_mappings_file_provided(self):
        if self.get(self.configuration_section, WRITE_PARSED_MAPPINGS_PATH):
            return True
        else:
            return False

    def is_oracle_client_lib_dir_provided(self):
        if self.get(self.configuration_section, ORACLE_CLIENT_LIB_DIR):
            return True
        else:
            return False

    def is_oracle_client_config_dir_provided(self):
        if self.get(self.configuration_section, ORACLE_CLIENT_CONFIG_DIR):
            return True
        else:
            return False

    def infer_sql_datatypes(self):
        return self.getboolean(self.configuration_section, INFER_SQL_DATATYPES)

    def clean_output_dir(self):
        return self.getboolean(self.configuration_section, CLEAN_OUTPUT_DIR)

    def only_write_printable_characters(self):
        return self.getboolean(self.configuration_section, ONLY_PRINTABLE_CHARACTERS)

    def get_configuration_option(self, option):
        return self.get(self.configuration_section, option)

    def get_number_of_processes(self):
        return self.getint(self.configuration_section, NUMBER_OF_PROCESSES)

    def get_logging_level(self):
        return self.get(self.configuration_section, LOGGING_LEVEL)

    def get_logging_file(self):
        return self.get(self.configuration_section, LOGGING_FILE)

    def get_parsed_mappings_read_path(self):
        return self.get(self.configuration_section, READ_PARSED_MAPPINGS_PATH)

    def get_parsed_mappings_write_path(self):
        return self.get(self.configuration_section, WRITE_PARSED_MAPPINGS_PATH)

    def get_oracle_client_lib_dir(self):
        return self.get(self.configuration_section, ORACLE_CLIENT_LIB_DIR)

    def get_oracle_client_config_dir(self):
        return self.get(self.configuration_section, ORACLE_CLIENT_CONFIG_DIR)

    def get_mapping_partition(self):
        return self.get(self.configuration_section, MAPPING_PARTITION)

    def get_output_dir(self):
        return self.get(self.configuration_section, OUTPUT_DIR)

    def get_output_file(self):
        return self.get(self.configuration_section, OUTPUT_FILE)

    def get_output_format(self):
        return self.get(self.configuration_section, OUTPUT_FORMAT)

    def get_chunksize(self):
        return self.getint(self.configuration_section, CHUNKSIZE)

    def apply_na_filter(self):
        return self.getboolean(self.configuration_section, NA_FILTER)

    def get_na_values(self):
        if self.apply_na_filter():
            return list(set(self.get(self.configuration_section, NA_VALUES).split(',')))
        else:
            return []

    def get_output_file_path(self, mapping_partition=None):
        if self.get_output_file():
            file_path = os.path.join(self.get_output_dir(), self.get_output_file())
            # remove file extension, we will set it based on the output format
            file_path = remove_file_extension(file_path)
        elif mapping_partition:
            file_path = os.path.join(self.get_output_dir(), mapping_partition)
        else:
            # neither output_file was specified nor mapping partition are used. Use default output_file.
            file_path = os.path.join(self.get_output_dir(), OUTPUT_FILE)

        # add file extension
        file_path += OUTPUT_FORMAT_FILE_EXTENSION[self.get_output_format()]

        return file_path

    def set_mapping_partition(self, mapping_partitioning):
        self.set(self.configuration_section, MAPPING_PARTITION, mapping_partitioning.upper())

    def set_output_dir(self, output_dir):
        self.set(self.configuration_section, OUTPUT_DIR, output_dir)

    def set_output_file(self, output_file):
        self.set(self.configuration_section, OUTPUT_FILE, output_file)

    def set_parsed_mappings_write_path(self, parsed_mappings_write_path):
        self.set(self.configuration_section, WRITE_PARSED_MAPPINGS_PATH, parsed_mappings_write_path)

    def set_logging_file(self, logging_file):
        self.set(self.configuration_section, LOGGING_FILE, logging_file)

    def set_logging_level(self, logging_level):
        self.set(self.configuration_section, LOGGING_LEVEL, logging_level)

    def set_output_format(self, output_format):
        self.set(self.configuration_section, OUTPUT_FORMAT, output_format)

    ################################################################################
    #######################   DATA SOURCE SECTIONS METHODS   #######################
    ################################################################################

    def get_data_sources_sections(self):
        return list(set(self.sections()) - {self.configuration_section})

    def get_source_type(self, source_section):
        return self.get(source_section, SOURCE_TYPE)

    def has_source_type(self, source_section):
        return self.has_option(source_section, SOURCE_TYPE)

    def get_mappings_files(self, source_section):
        mapping_file_paths = []

        for mapping_path in self.get(source_section, MAPPINGS).split(','):
            # if it is a file load the mapping triples to the graph
            if os.path.isfile(mapping_path):
                mapping_file_paths.append(mapping_path)
            # if it is a directory process all the mapping files within the root of the directory
            elif os.path.isdir(mapping_path):
                for mapping_file_name in os.listdir(mapping_path):
                    mapping_file = os.path.join(mapping_path, mapping_file_name)
                    if os.path.isfile(mapping_file):
                        mapping_file_paths.append(mapping_file)
            else:
                raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), mapping_path)

        return mapping_file_paths

    def get_database_url(self, source_section):
        return self.get(source_section, DATABASE_URL)

    def has_database_url(self, source_section):
        return self.has_option(source_section, DATABASE_URL)
