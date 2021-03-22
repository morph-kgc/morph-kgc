""" Morph-KGC """

__version__ = "0.0.1"

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import multiprocessing as mp


ARGUMENTS_DEFAULT = {
    'output_dir': 'output',
    'output_file': 'result',
    'output_format': 'nquads',
    'remove_duplicates': 'yes',
    'clean_output_dir': 'yes',
    'mapping_partitions': 'guess',
    'input_parsed_mappings_path': '',
    'output_parsed_mappings_path': '',
    'logs_file': '',
    'logging_level': 'info',
    'push_down_sql_distincts': 'no',
    'push_down_sql_joins': 'yes',
    'number_of_processes': mp.cpu_count(),
    'process_start_method': 'default',
    'async': 'no',
    'chunksize': 100000,
    'infer_datatypes': 'yes',
    'coerce_float': 'no',
    'only_printable_characters': 'no'
}

RELATIONAL_SOURCE_TYPES = ['mysql', 'postgresql', 'oracle', 'sqlserver']
TABULAR_SOURCE_TYPES = ['csv']

VALID_ARGUMENTS = {
    'output_format': ['ntriples', 'nquads'],
    'mapping_partitions': 'spog',
    'file_source_type': RELATIONAL_SOURCE_TYPES + TABULAR_SOURCE_TYPES,
    'process_start_method': ['default', 'spawn', 'fork', 'forkserver'],
    'logging_level': ['notset', 'debug', 'info', 'warning', 'error', 'critical']
}

R2RML = {
    'logical_table': 'http://www.w3.org/ns/r2rml#logicalTable',
    'sql_query': 'http://www.w3.org/ns/r2rml#sqlQuery',
    'column': 'http://www.w3.org/ns/r2rml#column',
    'default_graph': 'http://www.w3.org/ns/r2rml#defaultGraph',
    'IRI': 'http://www.w3.org/ns/r2rml#IRI',
    'literal': 'http://www.w3.org/ns/r2rml#Literal',
    'blank_node': 'http://www.w3.org/ns/r2rml#BlankNode'
}

RML = {
    'logical_source': 'http://semweb.mmlab.be/ns/rml#logicalSource',
    'query': 'http://semweb.mmlab.be/ns/rml#query',
    'reference': 'http://semweb.mmlab.be/ns/rml#reference'
}

MAPPINGS_DATAFRAME_COLUMNS = [
    'source_name',
    'triples_map_id', 'data_source', 'object_map', 'ref_form', 'iterator', 'tablename', 'query',
    'subject_template', 'subject_reference', 'subject_constant', 'subject_rdf_class', 'subject_termtype',
    'graph_constant', 'graph_reference', 'graph_template',
    'predicate_constant', 'predicate_template', 'predicate_reference',
    'object_constant', 'object_template', 'object_reference', 'object_termtype', 'object_datatype', 'object_language',
    'object_parent_triples_map', 'join_conditions',
    'predicate_object_graph_constant', 'predicate_object_graph_reference', 'predicate_object_graph_template'
]
