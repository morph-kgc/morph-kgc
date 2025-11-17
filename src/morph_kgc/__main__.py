__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"

import sys
import time
import logging

import multiprocessing as mp

from itertools import repeat

from .args_parser import load_config_from_command_line
from .materializer import _materialize_mapping_group_to_file
from .materializer import _materialize_mapping_group_to_kafka
from .utils import get_delta_time
from .mapping.mapping_parser import retrieve_mappings
from .constants import LOGGING_NAMESPACE, RML_TRIPLES_MAP_CLASS
from .utils import prepare_output_files


LOGGER = logging.getLogger(LOGGING_NAMESPACE)


if __name__ == "__main__":

    config = load_config_from_command_line()

    from .constants import JELLY

    if config.get_output_format() == JELLY:
        try:
            import pyjelly
        except ImportError as e:
            raise RuntimeError(
                "JELLY output requested, but pyjelly[rdflib] is not installed. "
                "Install: pip install 'morph-kgc[jelly]'"
            ) from e

        from . import materialize
        from .utils import create_dirs_in_path

        import sys

        config_path = sys.argv[1] if len(sys.argv) > 1 else None

        if not config_path:
            LOGGER.error("Config path is missing. Usage: python -m morph_kgc <config.ini>")
            sys.exit(2)

        graph = materialize(config_path)
        output_path = config.get_output_file_path(None)
        create_dirs_in_path(output_path)
        graph.serialize(destination=output_path, format="jelly")

        LOGGER.info(f'Jelly file generated: {output_path}')
        LOGGER.info(f'Materialization finished.')
        sys.exit(0)

    rml_df, fnml_df, http_api_df = retrieve_mappings(config)
    config.set('CONFIGURATION', 'http_api_df', http_api_df.to_csv())

    # keep only asserted mapping rules
    asserted_mapping_df = rml_df.loc[rml_df['triples_map_type'] == RML_TRIPLES_MAP_CLASS]
    mapping_groups = [group for _, group in asserted_mapping_df.groupby(by='mapping_partition')]

    prepare_output_files(config, rml_df)

    start_time = time.time()
    num_triples = 0
    if config.is_multiprocessing_enabled():
        LOGGER.debug(f'Parallelizing with {config.get_number_of_processes()} cores.')

        pool = mp.Pool(config.get_number_of_processes())
        if not config.get_output_kafka_server():
            num_triples = sum(pool.starmap(_materialize_mapping_group_to_file,
                                           zip(mapping_groups, repeat(rml_df), repeat(fnml_df), repeat(config))))
        else:
            num_triples = sum(pool.starmap(_materialize_mapping_group_to_kafka,
                                           zip(mapping_groups, repeat(rml_df), repeat(fnml_df), repeat(config))))
        pool.close()
        pool.join()
    else:
        for mapping_group in mapping_groups:
            if not config.get_output_kafka_server():
                num_triples += _materialize_mapping_group_to_file(mapping_group, rml_df, fnml_df, config)
            else:
                num_triples += _materialize_mapping_group_to_kafka(mapping_group, rml_df, fnml_df, config)

    LOGGER.info(f'Number of triples generated in total: {num_triples}.')
    LOGGER.info(f'Materialization finished in {get_delta_time(start_time)} seconds.')
