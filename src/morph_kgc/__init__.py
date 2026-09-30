__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]
__copyright__ = "Copyright © 2020 Julián Arenas-Guerrero"

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import sys
import logging
import multiprocessing as mp

from rdflib import Graph, Dataset
from pyoxigraph import Store, RdfFormat
from io import BytesIO
from itertools import repeat

from .args_parser import load_config_from_command_line
from .mapping.mapping_parser import retrieve_mappings, MappingParser
from .materializer import _materialize_mapping_group_to_set
from .args_parser import load_config_from_argument
from .constants import RML_TRIPLES_MAP_CLASS, LOGGING_NAMESPACE, NQUADS
from .mapping.yarrrml import load_yarrrml
from pathlib import Path


LOGGER = logging.getLogger(LOGGING_NAMESPACE)


def materialize_set(config, python_source=None):
    config = load_config_from_argument(config)

    # parallelization when running as a library is only enabled for Linux see #94
    if 'linux' not in sys.platform:
        LOGGER.info(
            f'Parallelization is not supported for {sys.platform} when running as a library. '
            f'If you need to speed up your data integration pipeline, please run through the command line.')
        config.set_number_of_processes('1')

    rml_df, fnml_df, http_api_df = retrieve_mappings(config)
    config.set('CONFIGURATION', 'http_api_df', http_api_df.to_csv())

    # keep only asserted mapping rules
    asserted_mapping_df = rml_df.loc[rml_df['triples_map_type'] == RML_TRIPLES_MAP_CLASS]
    mapping_groups = [group for _, group in asserted_mapping_df.groupby(by='mapping_partition')]

    if config.is_multiprocessing_enabled():
        LOGGER.debug(f'Parallelizing with {config.get_number_of_processes()} cores.')

        pool = mp.Pool(config.get_number_of_processes())
        rdf_statements = set().union(*pool.starmap(_materialize_mapping_group_to_set,
                                                   zip(mapping_groups, repeat(rml_df), repeat(fnml_df), repeat(config),
                                                       repeat(python_source))))
        pool.close()
        pool.join()
    else:
        rdf_statements = set()
        for mapping_group in mapping_groups:
            rdf_statements.update(_materialize_mapping_group_to_set(mapping_group, rml_df, fnml_df, config, python_source))

    LOGGER.info(f'Number of RDF statements generated in total: {len(rdf_statements)}.')

    return rdf_statements


def materialize(config, python_source=None):
    rdf_statements = materialize_set(config, python_source)
    config_parsed = load_config_from_argument(config)
    rdf_serialization = ".\n".join(rdf_statements) + "."
    if rdf_statements:
        if config_parsed.get_output_format() == NQUADS:
            graph = Dataset()
            graph.parse(data=rdf_serialization, format="nquads")
        else:
            graph = Graph()
            graph.parse(data=rdf_serialization, format="nt")
        return graph
    return Graph()


def materialize_oxigraph(config, python_source=None):
    rdf_statements = materialize_set(config, python_source)

    graph = Store()
    if rdf_statements:
        rdf_serialization = '.\n'.join(rdf_statements) + '.'
        graph.bulk_load(BytesIO(rdf_serialization.encode()), RdfFormat.TRIG)

    return graph


def materialize_kafka(config, python_source=None):
    from kafka import KafkaProducer

    kafka_producer = None

    try:
        rdf_statements = materialize_set(config, python_source)
        output_kafka_server = config.get_output_kafka_server()
        output_kafka_topic = config.get_output_kafka_topic()

        if not output_kafka_server or not output_kafka_topic:
            LOGGER.error('Output Kafka server or topic is empty.')
            sys.exit()

        kafka_producer = KafkaProducer(bootstrap_servers=output_kafka_server)

        if rdf_statements:
            # The serialization format follows output_format (N-TRIPLES or N-QUADS).
            rdf_serialization = '.\n'.join(rdf_statements) + '.'

            # send the RDF statements to Kafka
            kafka_producer.send(output_kafka_topic, value=rdf_serialization.encode('utf-8'))

        LOGGER.info(f'RDF statements materialized and sent to Kafka topic: {output_kafka_topic}.')
    except Exception as e:
            LOGGER.error(f'Error during materialization or Kafka publishing: {e}')
    finally:
        # close the Kafka producer
        if kafka_producer:
            kafka_producer.close()


def translate_to_rml(mapping_path):
    parser = MappingParser(config=None)
    mapping_graph = Graph()
    mapping_path = Path(mapping_path)

    if mapping_path.suffix in ['.ttl', '.rdf', '.nt']:
        mapping_graph.parse(mapping_path, format='ttl')
    elif mapping_path.suffix in ['.yml', '.yaml', '.yarrrml']:
        mapping_graph = load_yarrrml(mapping_path)

    mapping_graph = parser._normalize_mapping_graph(mapping_graph)
    mapping_graph = parser._complete_and_validate_mapping(mapping_graph)

    return mapping_graph
