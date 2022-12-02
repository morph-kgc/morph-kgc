__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import time
import logging

import multiprocessing as mp

from itertools import repeat

from .args_parser import load_config_from_command_line
from .materializer import _materialize_mapping_group_to_file
from .data_source.relational_database import setup_oracle
from .utils import get_delta_time
from .mapping.mapping_parser import retrieve_mappings
from .constants import R2RML_TRIPLES_MAP_CLASS
from .utils import prepare_output_files


if __name__ == "__main__":

    config = load_config_from_command_line()

    setup_oracle(config)

    mappings_df = retrieve_mappings(config)

    # keep only asserted mapping rules
    asserted_mapping_df = mappings_df.loc[mappings_df['triples_map_type'] == R2RML_TRIPLES_MAP_CLASS]
    mapping_groups = [group for _, group in asserted_mapping_df.groupby(by='mapping_partition')]

    prepare_output_files(config, mappings_df)

    start_time = time.time()
    num_triples = 0
    if config.is_multiprocessing_enabled():
        logging.debug(f'Parallelizing with {config.get_number_of_processes()} cores.')

        pool = mp.Pool(config.get_number_of_processes())
        num_triples = sum(
            pool.starmap(_materialize_mapping_group_to_file, zip(mapping_groups, repeat(mappings_df), repeat(config))))
        pool.close()
        pool.join()
    else:
        for mapping_group in mapping_groups:
            num_triples += _materialize_mapping_group_to_file(mapping_group, mappings_df, config)

    logging.info(f'Number of triples generated in total: {num_triples}.')
    logging.info(f'Materialization finished in {get_delta_time(start_time)} seconds.')
