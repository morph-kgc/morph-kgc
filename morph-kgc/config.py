""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os
import constants
import logging
import utils

from configparser import ConfigParser


CONFIGURATION_SECTION = 'CONFIGURATION'

OUTPUT_DIR = 'output_dir'
OUTPUT_FILE = 'output_file'
OUTPUT_FORMAT = 'output_format'
CLEAN_OUTPUT_DIR = 'clean_output_dir'
ONLY_PRINTABLE_CHARACTERS = 'only_printable_characters'

MAPPING_PARTITIONS = 'mapping_partitions'
INFER_SQL_DATATYPES = 'infer_sql_datatypes'
REMOVE_SELF_JOINS = 'remove_self_joins'
READ_PARSED_MAPPINGS_PATH = 'read_parsed_mappings_path'
WRITE_PARSED_MAPPINGS_PATH = 'write_parsed_mappings_path'

CHUNKSIZE = 'chunksize'
PUSH_DOWN_SQL_DISTINCTS = 'push_down_sql_distincts'
PUSH_DOWN_SQL_JOINS = 'push_down_sql_joins'
COERCE_FLOAT = 'coerce_float'

NUMBER_OF_PROCESSES = 'number_of_processes'
PROCESS_START_METHOD = 'process_start_method'
ASYNC_MULTIPROCESSING = 'async_multiprocessing'

LOGGING_LEVEL = 'logging_level'
LOGGING_FILE = 'logs_file'

SOURCE_TYPE = 'source_type'
MAPPINGS = 'mappings'
USER = 'user'
PASSWORD = 'password'
HOST = 'host'
PORT = 'port'
DB = 'db'


# input parameters that do not allow empty values
CONFIGURATION_OPTIONS_EMPTY_VALID = {
            OUTPUT_DIR: constants.DEFAULT_OUTPUT_DIR,
            OUTPUT_FILE: constants.DEFAULT_OUTPUT_FILE,
            READ_PARSED_MAPPINGS_PATH: constants.DEFAULT_READ_PARSED_MAPPINGS_PATH,
            WRITE_PARSED_MAPPINGS_PATH: constants.DEFAULT_WRITE_PARSED_MAPPINGS_PATH,
            MAPPING_PARTITIONS: constants.DEFAULT_MAPPING_PARTITIONS,
            LOGGING_FILE: constants.DEFAULT_LOGS_FILE
        }


# input parameters whose value can be empty
CONFIGURATION_OPTIONS_EMPTY_NON_VALID = {
            OUTPUT_FORMAT: constants.DEFAULT_OUTPUT_FORMAT,
            CLEAN_OUTPUT_DIR: constants.DEFAULT_CLEAN_OUTPUT_DIR,
            ONLY_PRINTABLE_CHARACTERS: constants.DEFAULT_ONLY_PRINTABLE_CHARACTERS,
            PUSH_DOWN_SQL_DISTINCTS: constants.DEFAULT_PUSH_DOWN_SQL_DISTINCTS,
            PUSH_DOWN_SQL_JOINS: constants.DEFAULT_PUSH_DOWN_SQL_JOINS,
            INFER_SQL_DATATYPES: constants.DEFAULT_INFER_SQL_DATATYPES,
            REMOVE_SELF_JOINS: constants.DEFAULT_REMOVE_SELF_JOINS,
            NUMBER_OF_PROCESSES: constants.DEFAULT_NUMBER_OF_PROCESSES,
            ASYNC_MULTIPROCESSING: constants.DEFAULT_ASYNC_MULTIPROCESSING,
            PROCESS_START_METHOD: constants.DEFAULT_PROCESS_START_METHOD,
            CHUNKSIZE: constants.DEFAULT_CHUNKSIZE,
            COERCE_FLOAT: constants.DEFAULT_COERCE_FLOAT,
            LOGGING_LEVEL: constants.DEFAULT_LOGGING_LEVEL,
        }


def _is_option_provided(config, option, empty_value_is_valid=False):
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
        # add configuration section if its does not exist
        if not self.has_section(self.configuration_section):
            self.add_section(self.configuration_section)

        for configuration_option, configuration_option_default in CONFIGURATION_OPTIONS_EMPTY_VALID.items():
            if not _is_option_provided(self, configuration_option):
                self.set(self.configuration_section, configuration_option, str(configuration_option_default))
        for configuration_option, configuration_option_default in CONFIGURATION_OPTIONS_EMPTY_NON_VALID.items():
            if not _is_option_provided(self, configuration_option, empty_value_is_valid=True):
                self.set(self.configuration_section, configuration_option, str(configuration_option_default))

    def validate_configuration_section(self):
        # OUTPUT DIR & FILE, WRITE PARSED MAPPINGS FILE, LOGS FILE
        self.set_output_dir(utils.get_valid_dir_path(self.get_output_dir()))
        self.set_output_file(utils.get_valid_file_name(self.get_output_file()))
        self.set_parsed_mappings_write_path(utils.get_valid_file_path(self.get_parsed_mappings_write_path()))
        self.set_logging_file(utils.get_valid_file_path(self.get_logging_file()))

        # OUTPUT FORMAT
        output_format = str(self.get_output_format()).upper()
        self.set_output_format(output_format)
        if output_format not in constants.VALID_OUTPUT_FORMATS:
            raise ValueError(OUTPUT_FORMAT + ' value `' + self.get_output_format() +
                             '` is not valid. Must be in: ' + str(constants.VALID_OUTPUT_FORMATS) + '.')

        # MAPPING PARTITIONS
        mapping_partitions = str(self.get_mapping_partitions()).upper()
        self.set_mapping_partitions(mapping_partitions)
        if mapping_partitions != 'GUESS' and not (
                set(mapping_partitions) <= set(constants.VALID_MAPPING_PARTITIONS)):
            raise ValueError(MAPPING_PARTITIONS + ' value `' + self.get_mapping_partitions() +
                             '` is not valid. Must be `GUESS`, empty, or a subset of ' +
                             str(constants.VALID_MAPPING_PARTITIONS) + '.')

        # LOGGING LEVEL
        logging_level = str(self.get_logging_level()).lower()
        self.set_logging_level(logging_level)
        if logging_level not in constants.VALID_LOGGING_LEVEL:
            raise ValueError(LOGGING_LEVEL + ' value `' + self.get_logging_level() +
                             '` is not valid. Must be in: ' + str(constants.VALID_LOGGING_LEVEL) + '.')

        # PROCESS START METHOD
        process_start_method = str(self.get_process_start_method()).lower()
        self.set_process_start_method(process_start_method)
        if process_start_method not in constants.VALID_PROCESS_START_METHOD:
            raise ValueError(PROCESS_START_METHOD + ' value `' + self.get_process_start_method() +
                             '` is not valid. Must be in: ' + str(constants.VALID_PROCESS_START_METHOD) + '.')

    def validate_data_source_sections(self):
        # SOURCE TYPE
        for section in self.get_data_sources_sections():
            self.set(section, SOURCE_TYPE, self.get_source_type(section).upper())
            if self.get_source_type(section) not in constants.VALID_DATA_SOURCE_TYPES:
                raise ValueError(SOURCE_TYPE + ' value `' + self.get_source_type(section) + ' is not valid. '
                                 'Must be in: ' + str(constants.VALID_DATA_SOURCE_TYPES) + '.')

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

    def is_async_multiprocessing_enabled(self):
        return self.getboolean(self.configuration_section, ASYNC_MULTIPROCESSING)

    def is_process_start_method_default(self):
        return self.get(self.configuration_section, PROCESS_START_METHOD) == 'default'

    def infer_sql_datatypes(self):
        return self.getboolean(self.configuration_section, INFER_SQL_DATATYPES)

    def push_down_sql_joins(self):
        return self.getboolean(self.configuration_section, PUSH_DOWN_SQL_JOINS)

    def push_down_sql_distincts(self):
        return self.getboolean(self.configuration_section, PUSH_DOWN_SQL_DISTINCTS)

    def coerce_float(self):
        return self.getboolean(self.configuration_section, COERCE_FLOAT)

    def remove_self_joins(self):
        return self.getboolean(self.configuration_section, REMOVE_SELF_JOINS)

    def clean_output_dir(self):
        return self.getboolean(self.configuration_section, CLEAN_OUTPUT_DIR)

    def only_write_printable_characters(self):
        return self.getboolean(self.configuration_section, ONLY_PRINTABLE_CHARACTERS)

    def get_configuration_option(self, option):
        return self.get(self.configuration_section, option)

    def get_process_start_method(self):
        return self.get(self.configuration_section, PROCESS_START_METHOD)

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

    def get_mapping_partitions(self):
        return self.get(self.configuration_section, MAPPING_PARTITIONS)

    def get_output_dir(self):
        return self.get(self.configuration_section, OUTPUT_DIR)

    def get_output_file(self):
        return self.get(self.configuration_section, OUTPUT_FILE)

    def get_output_format(self):
        return self.get(self.configuration_section, OUTPUT_FORMAT)

    def get_chunksize(self):
        return self.getint(self.configuration_section, CHUNKSIZE)

    def get_output_file_path(self, mapping_partition=None):
        if self.get_output_file():
            file_path = os.path.join(self.get_output_dir(), self.get_output_file())
            # remove file extension, we will set it based on the output format
            file_path = utils.remove_file_extension(file_path)
        elif mapping_partition:
            file_path = os.path.join(self.getoutput_dir(), mapping_partition)
        else:
            file_path = os.path.join(self.getoutput_dir(), OUTPUT_FILE)

        # add file extension
        file_path += constants.OUTPUT_FORMAT_FILE_EXTENSION[self.get_output_format()]

        return file_path

    def set_mapping_partitions(self, mapping_partitions_criteria):
        self.set(self.configuration_section, MAPPING_PARTITIONS, mapping_partitions_criteria)

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

    def set_process_start_method(self, process_start_method):
        self.set(self.configuration_section, PROCESS_START_METHOD, process_start_method)

    ################################################################################
    #######################   DATA SOURCE SECTIONS METHODS   #######################
    ################################################################################

    def get_data_sources_sections(self):
        return list(set(self.sections()) - {self.configuration_section})

    def get_source_type(self, source_section):
        return self.get(source_section, SOURCE_TYPE)

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

        return mapping_file_paths

    def get_host(self, source_section):
        return self.get(source_section, HOST)

    def get_port(self, source_section):
        return self.get(source_section, PORT)

    def get_user(self, source_section):
        return self.get(source_section, USER)

    def get_password(self, source_section):
        return self.get(source_section, PASSWORD)

    def get_db(self, source_section):
        return self.get(source_section, DB)
