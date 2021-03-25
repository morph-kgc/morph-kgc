""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import logging
import time
import sys
import utils
import pandas as pd

from mapping.mapping_parser import MappingParser
from args_parser import parse_config
from materializer import Materializer


def process_mappings(config):
    input_parsed_mappings_path = config.get('CONFIGURATION', 'input_parsed_mappings_path')
    if input_parsed_mappings_path:
        # retrieve parsed mapping from file and finish mapping processing
        mappings = pd.read_csv(input_parsed_mappings_path, keep_default_na=False)
        logging.info(str(len(mappings)) + ' mappings rules with ' + str(len(set(mappings[
                     'mapping_partition']))) + ' mapping partitions loaded from file.')
    else:
        mappings_parser = MappingParser(config)

        start_time = time.time()
        mappings = mappings_parser.parse_mappings()
        logging.info('Mappings processed in ' + utils.get_delta_time(start_time) + ' seconds.')

        output_parsed_mappings_path = config.get('CONFIGURATION', 'output_parsed_mappings_path')
        if output_parsed_mappings_path:
            # the parsed mappings are to be saved to a file, and the execution of the engine terminates
            mappings.sort_values(by=['id'], axis=0).to_csv(output_parsed_mappings_path, index=False)
            logging.info('Parsed mapping rules saved to file.')
            sys.exit()

    return mappings


def process_materialization(mappings, config):
    materializer = Materializer(mappings, config)

    start_time = time.time()
    if int(config.get('CONFIGURATION', 'number_of_processes')) == 1:
        materializer.materialize()
    else:
        materializer.materialize_concurrently()
    logging.info('Materialization finished in ' + utils.get_delta_time(start_time) + ' seconds.')


if __name__ == "__main__":

    config = parse_config()
    mappings = process_mappings(config)
    process_materialization(mappings, config)
