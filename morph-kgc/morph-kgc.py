""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import logging
import time

from mapping.mapping_parser import MappingParser
from mapping.mapping_partitioner import MappingPartitioner
from args_parser import parse_config
from materializer import materialize


if __name__ == "__main__":

    start_time = time.time()

    config = parse_config()

    mappings_parser = MappingParser(config)
    parsed_mappings = mappings_parser.parse_mappings()

    mapping_partitioner = MappingPartitioner(parsed_mappings, config)
    mappings = mapping_partitioner.partition_mappings()

    materialize(mappings, config)

    logging.info('Materialization finished in ' + "{:.3f}".format((time.time() - start_time)) + ' seconds.')
