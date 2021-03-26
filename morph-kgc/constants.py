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
    'infer_sql_datatypes': 'yes',
    'number_of_processes': mp.cpu_count(),
    'process_start_method': 'default',
    'async': 'no',
    'chunksize': 100000,
    'remove_self_joins': 'yes',
    'coerce_float': 'no',
    'only_printable_characters': 'no'
}

RELATIONAL_SOURCE_TYPES = ['MYSQL']
TABULAR_SOURCE_TYPES = ['CSV']

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

RDF = {
    'type': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
}

AUXILIAR_UNIQUE_REPLACING_STRING = 'zzyy_xxww\u200B'

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

MAPPING_PARSING_QUERY = """
    # This query has been reused from SDM-RDFizer (https://github.com/SDM-TIB/SDM-RDFizer). SDM-RDFizer has been developed
    # by members of the Scientific Data Management Group at TIB. Its development has been coordinated and supervised by
    # Maria-Esther Vidal. The implementation has been done by Enrique Iglesias and Guillermo Betancourt under the
    # supervision of David Chaves-Fraga, Samaneh Jozashoori, and Kemele Endris.
    # It has been partially modified by Julián Arenas-Guerrero, PhD student at the Ontology Engineering Group (OEG)
    # in Universidad Politécnica de Madrid (UPM).
    
    prefix rr: <http://www.w3.org/ns/r2rml#>
    prefix rml: <http://semweb.mmlab.be/ns/rml#>
    
    SELECT DISTINCT
        ?triples_map_id ?data_source ?ref_form ?iterator ?tablename ?query ?object_map
        ?subject_template ?subject_reference ?subject_constant
        ?subject_rdf_class ?subject_termtype
        ?graph_constant ?graph_reference ?graph_template
        ?predicate_constant ?predicate_template ?predicate_reference
        ?object_constant ?object_template ?object_reference ?object_termtype ?object_datatype ?object_language
        ?object_parent_triples_map
        ?predicate_object_graph_constant ?predicate_object_graph_reference ?predicate_object_graph_template
    
    WHERE {
        ?triples_map_id rml:logicalSource ?_source .
        OPTIONAL { ?_source rml:source ?data_source . }
        OPTIONAL { ?_source rml:referenceFormulation ?ref_form . }
        OPTIONAL { ?_source rml:iterator ?iterator . }
        OPTIONAL { ?_source rr:tableName ?tablename . }
        OPTIONAL { ?_source rml:query ?query . }
    
    # Subject -------------------------------------------------------------------------
        OPTIONAL {
            ?triples_map_id rr:subjectMap ?_subject_map .
            OPTIONAL { ?_subject_map rr:template ?subject_template . }
            OPTIONAL { ?_subject_map rml:reference ?subject_reference . }
            OPTIONAL { ?_subject_map rr:constant ?subject_constant . }
            OPTIONAL { ?_subject_map rr:class ?subject_rdf_class . }
            OPTIONAL { ?_subject_map rr:termType ?subject_termtype . }
            OPTIONAL { ?_subject_map rr:graph ?graph_constant . }
            OPTIONAL {
                ?_subject_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:constant ?graph_constant .
            }
            OPTIONAL {
                ?_subject_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:template ?graph_template .
            }
            OPTIONAL {
                ?_subject_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:reference ?graph_reference .
            }
        }
        OPTIONAL { ?triples_map_id rr:subject ?subject_constant . }
    
    # Predicate -----------------------------------------------------------------------
        OPTIONAL {
            ?triples_map_id rr:predicateObjectMap ?_predicate_object_map .
            OPTIONAL {
                ?_predicate_object_map rr:predicateMap ?_predicate_map .
                ?_predicate_map rr:constant ?predicate_constant .
            }
            OPTIONAL {
                ?_predicate_object_map rr:predicateMap ?_predicate_map .
                ?_predicate_map rr:template ?predicate_template .
            }
            OPTIONAL {
                ?_predicate_object_map rr:predicateMap ?_predicate_map .
                ?_predicate_map rml:reference ?predicate_reference .
            }
            OPTIONAL { ?_predicate_object_map rr:predicate ?predicate_constant . }
    
    # Object --------------------------------------------------------------------------
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?object_map .
                ?object_map rr:constant ?object_constant .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?object_map .
                ?object_map rr:template ?object_template .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?object_map .
                ?object_map rml:reference ?object_reference .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:objectMap ?object_map .
                ?object_map rr:parentTriplesMap ?object_parent_triples_map .
                OPTIONAL { ?object_map rr:termType ?object_termtype . }
            }
            OPTIONAL {
                ?_predicate_object_map rr:object ?object_constant .
                OPTIONAL { ?object_map rr:datatype ?object_datatype . }
                OPTIONAL { ?object_map rr:language ?object_language . }
            }
            OPTIONAL { ?_predicate_object_map rr:graph ?predicate_object_graph_constant . }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:constant ?predicate_object_graph_constant .
            }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:template ?predicate_object_graph_template .
            }
            OPTIONAL {
                ?_predicate_object_map rr:graphMap ?_graph_structure .
                ?_graph_structure rr:reference ?predicate_object_graph_reference .
            }
        }
    }
"""

JOIN_CONDITION_PARSING_QUERY = """
    prefix rr: <http://www.w3.org/ns/r2rml#>

    SELECT DISTINCT ?object_map ?join_condition ?child_value ?parent_value
    WHERE {
        ?object_map rr:joinCondition ?join_condition .
        ?join_condition rr:child ?child_value;
                        rr:parent ?parent_value.
    }
"""