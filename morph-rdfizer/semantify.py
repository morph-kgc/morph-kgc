import logging

import TriplesMap as tp
from args_parsing import parse_config
from configuration import configure_logger, get_configuration_and_sources
from morph_utils import sparql_to_dataframe


if __name__ == "__main__":

    config = parse_config()

    configure_logger(config)
    configuration, data_sources = get_configuration_and_sources(config)

    for source_name, source_options in data_sources.items():
        '''TODO'''
        tp.parse_rml_mapping_file(source_options['mapping_file'])
