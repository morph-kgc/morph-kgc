__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]
__copyright__ = "Copyright © 2020 Julián Arenas-Guerrero"

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import sys
import logging
import multiprocessing as mp

from rdflib import Graph
from pyoxigraph import Store
from io import BytesIO
from itertools import repeat

from .args_parser import load_config_from_command_line
from .mapping.mapping_parser import retrieve_mappings
from .data_source.relational_database import setup_oracle
from .materializer import _materialize_mapping_rule, _materialize_mapping_group_to_set
from .args_parser import load_config_from_argument
from .constants import R2RML_TRIPLES_MAP_CLASS


def materialize_set(config):
    config = load_config_from_argument(config)

    # parallelization when running as a library is only enabled for Linux see #94
    if 'linux' not in sys.platform:
        logging.info(
            f'Parallelization is not supported for {sys.platform} when running as a library. '
            f'If you need to speed up your data integration pipeline, please run through the command line.')
        config.set_number_of_processes('1')

    setup_oracle(config)

    mappings_df = retrieve_mappings(config)

    # keep only asserted mapping rules
    asserted_mapping_df = mappings_df.loc[mappings_df['triples_map_type'] == R2RML_TRIPLES_MAP_CLASS]
    mapping_groups = [group for _, group in asserted_mapping_df.groupby(by='mapping_partition')]

    if config.is_multiprocessing_enabled():
        logging.debug(f'Parallelizing with {config.get_number_of_processes()} cores.')

        pool = mp.Pool(config.get_number_of_processes())
        triples = set().union(
            *pool.starmap(_materialize_mapping_group_to_set, zip(mapping_groups, repeat(mappings_df), repeat(config))))
        pool.close()
        pool.join()
    else:
        triples = set()
        for mapping_group in mapping_groups:
            triples.update(_materialize_mapping_group_to_set(mapping_group, mappings_df, config))

    logging.info(f'Number of triples generated in total: {len(triples)}.')

    return triples


def materialize(config):
    triples = materialize_set(config)

    graph = Graph()
    rdf_ntriples = '.\n'.join(triples)
    if rdf_ntriples:
        # only add final dot if at least one triple was generated
        rdf_ntriples += '.'
        graph.parse(data=rdf_ntriples, format='nquads')

    return graph


def materialize_oxigraph(config):
    triples = materialize_set(config)

    graph = Store()
    rdf_ntriples = '.\n'.join(triples)
    if rdf_ntriples:
        # only add final dot if at least one triple was generated
        rdf_ntriples += '.'
        graph.bulk_load(BytesIO(rdf_ntriples.encode()), 'application/n-quads')

    return graph
