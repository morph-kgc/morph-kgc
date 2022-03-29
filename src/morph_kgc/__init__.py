__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]
__copyright__ = "Copyright © 2020 Julián Arenas-Guerrero"

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import logging

from rdflib import Graph
from pyoxigraph import Store
from io import BytesIO

from .engine import retrieve_mappings
from .args_parser import load_config_from_argument
from .materializer import _materialize_mapping_rule
from .data_source.relational_database import setup_oracle
from .constants import R2RML_TRIPLES_MAP_CLASS


def materialize(config):
    config = load_config_from_argument(config)

    setup_oracle(config)

    mappings_df = retrieve_mappings(config)

    # keep only asserted mapping rules
    asserted_mapping_df = mappings_df.loc[mappings_df['triples_map_type'] == R2RML_TRIPLES_MAP_CLASS]
    mapping_partitions = [group for _, group in asserted_mapping_df.groupby(by='mapping_partition')]

    graph = Graph()
    for mapping_partition in mapping_partitions:
        triples = set()
        for i, mapping_rule in mapping_partition.iterrows():
            results_df = _materialize_mapping_rule(mapping_rule, mappings_df, config)
            triples.update(set(results_df['triple']))

            logging.debug(str(len(triples)) + ' triples generated for mapping rule `' + str(mapping_rule['id']) + '`.')

        rdf_ntriples = '.\n'.join(triples)
        if rdf_ntriples:
            # it can happen that a mapping rule generates 0 triples, do not add the final full stop
            rdf_ntriples += '.'
            graph.parse(data=rdf_ntriples, format=config.get_output_format().replace('-', '').lower())

    logging.info('Number of triples generated in total: ' + str(len(graph)) + '.')

    return graph


def materialize_oxigraph(config):
    config = load_config_from_argument(config)

    setup_oracle(config)

    mappings_df = retrieve_mappings(config)

    # keep only asserted mapping rules
    asserted_mapping_df = mappings_df.loc[mappings_df['triples_map_type'] == R2RML_TRIPLES_MAP_CLASS]
    mapping_partitions = [group for _, group in asserted_mapping_df.groupby(by='mapping_partition')]

    graph = Store()
    for mapping_partition in mapping_partitions:
        triples = set()
        for i, mapping_rule in mapping_partition.iterrows():
            results_df = _materialize_mapping_rule(mapping_rule, mappings_df, config)
            triples.update(set(results_df['triple']))

            logging.debug(str(len(triples)) + ' triples generated for mapping rule `' + str(mapping_rule['id']) + '`.')

        rdf_ntriples = '.\n'.join(triples)
        if rdf_ntriples:
            # it can happen that a mapping rule generates 0 triples, do not add the final full stop
            rdf_ntriples += '.'
            graph.bulk_load(BytesIO(rdf_ntriples.encode()), 'application/n-quads')

    logging.info('Number of triples generated in total: ' + str(len(graph)) + '.')

    return graph


def materialize_set(config):
    config = load_config_from_argument(config)

    setup_oracle(config)

    mappings_df = retrieve_mappings(config)

    # keep only asserted mapping rules
    asserted_mapping_df = mappings_df.loc[mappings_df['triples_map_type'] == R2RML_TRIPLES_MAP_CLASS]
    mapping_partitions = [group for _, group in asserted_mapping_df.groupby(by='mapping_partition')]

    triples = set()
    for mapping_partition in mapping_partitions:
        for i, mapping_rule in mapping_partition.iterrows():
            results_df = _materialize_mapping_rule(mapping_rule, mappings_df, config)
            triples.update(set(results_df['triple']))

            logging.debug(str(len(set(results_df['triple']))) + ' triples generated for mapping rule `' + str(mapping_rule['id']) + '`.')

    logging.info('Number of triples generated in total: ' + str(len(triples)) + '.')

    return triples
