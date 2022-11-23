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

from .args_parser import load_config_from_command_line
from .engine import retrieve_mappings, process_materialization
from .data_source.relational_database import setup_oracle
from .args_parser import load_config_from_argument


def materialize_set(config):
    config = load_config_from_argument(config)

    setup_oracle(config)

    mappings = retrieve_mappings(config)
    triples = process_materialization(mappings, config, to_file=False)

    return triples


def materialize(config):
    triples = materialize_set(config)

    graph = Graph()
    rdf_ntriples = '.\n'.join(triples)
    if rdf_ntriples:
        # only add final dot if at leat one triple was generated
        rdf_ntriples += '.'
        graph.parse(data=rdf_ntriples, format='nquads')

    return graph


def materialize_oxigraph(config):
    triples = materialize_set(config)

    graph = Store()
    rdf_ntriples = '.\n'.join(triples)
    if rdf_ntriples:
        # only add final dot if at leat one triple was generated
        rdf_ntriples += '.'
        graph.bulk_load(BytesIO(rdf_ntriples.encode()), 'application/n-quads')

    return graph
