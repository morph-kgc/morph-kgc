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
from .materializer import _materialize_mapping_group_to_set
from .args_parser import load_config_from_argument
from .constants import RML_TRIPLES_MAP_CLASS


def materialize_set(config, python_source=None):
    config = load_config_from_argument(config)

    # parallelization when running as a library is only enabled for Linux see #94
    if 'linux' not in sys.platform:
        logging.info(
            f'Parallelization is not supported for {sys.platform} when running as a library. '
            f'If you need to speed up your data integration pipeline, please run through the command line.')
        config.set_number_of_processes('1')

    rml_df, fnml_df = retrieve_mappings(config)

    # keep only asserted mapping rules
    asserted_mapping_df = rml_df.loc[rml_df['triples_map_type'] == RML_TRIPLES_MAP_CLASS]
    mapping_groups = [group for _, group in asserted_mapping_df.groupby(by='mapping_partition')]

    if config.is_multiprocessing_enabled():
        logging.debug(f'Parallelizing with {config.get_number_of_processes()} cores.')

        pool = mp.Pool(config.get_number_of_processes())
        triples = set().union(*pool.starmap(_materialize_mapping_group_to_set,
                                            zip(mapping_groups, repeat(rml_df), repeat(fnml_df), repeat(config),
                                                repeat(python_source))))
        pool.close()
        pool.join()
    else:
        triples = set()
        for mapping_group in mapping_groups:
            triples.update(_materialize_mapping_group_to_set(mapping_group, rml_df, fnml_df, config, python_source))

    logging.info(f'Number of triples generated in total: {len(triples)}.')

    return triples


def materialize(config, python_source=None):
    triples = materialize_set(config, python_source)

    graph = Graph()
    if triples:
        rdf_ntriples = '.\n'.join(triples) + '.'
        graph.parse(data=rdf_ntriples, format='nquads')

    return graph


def materialize_oxigraph(config, python_source=None):
    triples = materialize_set(config, python_source)

    graph = Store()
    if triples:
        rdf_ntriples = '.\n'.join(triples) + '.'
        graph.bulk_load(BytesIO(rdf_ntriples.encode()), 'application/n-quads')

    return graph


def materialize_kafka(config, python_source=None):
    from kafka import KafkaProducer

    kafka_producer = None

    try:
        triples = materialize_set(config, python_source)
        output_kafka_server = config.get_output_kafka_server()
        output_kafka_topic = config.get_output_kafka_topic()

        if not output_kafka_server or not output_kafka_topic:
            logging.error('Output Kafka server or topic is empty.')
            sys.exit()

        kafka_producer = KafkaProducer(bootstrap_servers=output_kafka_server)

        if triples:
            rdf_ntriples = '.\n'.join(triples) + '.'

            # send the RDF triples to Kafka
            kafka_producer.send(output_kafka_topic, value=rdf_ntriples.encode('utf-8'))

        logging.info(f'RDF triples materialized and sent to Kafka topic: {output_kafka_topic}.')
    except Exception as e:
            logging.error(f'Error during materialization or Kafka publishing: {e}')
    finally:
        # close the Kafka producer
        if kafka_producer:
            kafka_producer.close()
