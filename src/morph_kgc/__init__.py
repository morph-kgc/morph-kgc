__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import logging

from rdflib import Graph

from .engine import retrieve_mappings
from .args_parser import load_config_from_argument
from .materializer import _materialize_mapping_rule
from .utils import get_subject_maps
from .data_source.relational_database import setup_oracle


def materialize(config):

    config = load_config_from_argument(config)

    setup_oracle(config)

    mappings_df = retrieve_mappings(config)
    subject_maps_df = get_subject_maps(mappings_df)
    mapping_partitions = [group for _, group in mappings_df.groupby(by='mapping_partition')]

    num_triples = 0
    graph = Graph()
    for mapping_partition in mapping_partitions:
        triples = set()
        for i, mapping_rule in mapping_partition.iterrows():
            triples.update(set(_materialize_mapping_rule(mapping_rule, subject_maps_df, config)))

            logging.debug(str(len(triples)) + ' triples generated for mapping rule `' + str(mapping_rule['id']) + '`.')

        rdf_ntriples = '.\n'.join(triples)
        rdf_ntriples += '.'
        graph.parse(data=rdf_ntriples, format=config.get_output_format().replace('-', '').lower())

    logging.info('Number of triples generated in total: ' + str(num_triples) + '.')

    return graph
