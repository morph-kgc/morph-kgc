""" Morph-KGC """

__version__ = "0.0.1"

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020-2021 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import multiprocessing as mp


##############################################################################
########################   ARGUMENTS DEFAULT VALUES   ########################
##############################################################################

DEFAULT_OUTPUT_DIR = 'output'
DEFAULT_OUTPUT_FILE = 'result'
DEFAULT_OUTPUT_FORMAT = 'NQUADS'
DEFAULT_REMOVE_DUPLICATES = 'yes'
DEFAULT_CLEAN_OUTPUT_DIR = 'yes'
DEFAULT_MAPPING_PARTITIONS = 'GUESS'
DEFAULT_READ_PARSED_MAPPINGS_PATH = ''
DEFAULT_WRITE_PARSED_MAPPINGS_PATH = ''
DEFAULT_LOGS_FILE = ''
DEFAULT_LOGGING_LEVEL = 'INFO'
DEFAULT_PUSH_DOWN_SQL_DISTINCTS = 'no'
DEFAULT_PUSH_DOWN_SQL_JOINS = 'no'
DEFAULT_INFER_SQL_DATATYPES = 'yes'
DEFAULT_NUMBER_OF_PROCESSES = 2 * mp.cpu_count()
DEFAULT_PROCESS_START_METHOD = 'default'
DEFAULT_ASYNC_MULTIPROCESSING = 'no'
DEFAULT_CHUNKSIZE = 100000
DEFAULT_REMOVE_SELF_JOINS = 'yes'
DEFAULT_COERCE_FLOAT = 'no'
DEFAULT_NA_VALUES = ',#N/A,N/A,#N/A N/A,n/a,NA,<NA>,#NA,NULL,null,NaN,nan'
DEFAULT_ONLY_PRINTABLE_CHARACTERS = 'no'


##############################################################################
#########################   VALID ARGUMENTS VALUES   #########################
##############################################################################

VALID_RELATIONAL_SOURCE_TYPES = ['MYSQL']
VALID_TABULAR_SOURCE_TYPES = ['CSV']
VALID_DATA_SOURCE_TYPES = VALID_RELATIONAL_SOURCE_TYPES + VALID_TABULAR_SOURCE_TYPES
VALID_OUTPUT_FORMATS = ['NTRIPLES', 'NQUADS']
VALID_MAPPING_PARTITIONS = 'SPOG'
VALID_PROCESS_START_METHOD = ['default', 'spawn', 'fork', 'forkserver']
VALID_LOGGING_LEVEL = ['notset', 'debug', 'info', 'warning', 'error', 'critical']


##############################################################################
###################   FILE EXTENSIONS FOR OUTPUT FORMATS   ###################
##############################################################################

OUTPUT_FORMAT_FILE_EXTENSION = {
    'NTRIPLES': '.nt',
    'NQUADS': '.nq'
}


##############################################################################
######################   [R2]RML & RDF SPECIFICATIONS   ######################
##############################################################################

R2RML_LOGICAL_TABLE = 'http://www.w3.org/ns/r2rml#logicalTable'
R2RML_SQL_QUERY = 'http://www.w3.org/ns/r2rml#sqlQuery'
R2RML_COLUMN = 'http://www.w3.org/ns/r2rml#column'
R2RML_DEFAULT_GRAPH = 'http://www.w3.org/ns/r2rml#defaultGraph'
R2RML_IRI = 'http://www.w3.org/ns/r2rml#IRI'
R2RML_LITERAL = 'http://www.w3.org/ns/r2rml#Literal'
R2RML_BLANK_NODE = 'http://www.w3.org/ns/r2rml#BlankNode'

RML_LOGICAL_SOURCE = 'http://semweb.mmlab.be/ns/rml#logicalSource'
RML_QUERY = 'http://semweb.mmlab.be/ns/rml#query'
RML_REFERENCE = 'http://semweb.mmlab.be/ns/rml#reference'

RDF_TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'


AUXILIAR_UNIQUE_REPLACING_STRING = 'zzyy_xxww\u200B'


##############################################################################
#######################   MAPPING DATAFRAME COLUMNS   ########################
##############################################################################

MAPPINGS_DATAFRAME_COLUMNS = [
    'source_name', 'triples_map_id', 'data_source', 'object_map', 'ref_form', 'iterator', 'tablename', 'query',
    'subject_template', 'subject_reference', 'subject_constant', 'subject_rdf_class', 'subject_termtype',
    'graph_constant', 'graph_reference', 'graph_template',
    'predicate_constant', 'predicate_template', 'predicate_reference',
    'object_constant', 'object_template', 'object_reference', 'object_termtype', 'object_datatype', 'object_language',
    'object_parent_triples_map', 'join_conditions',
    'predicate_object_graph_constant', 'predicate_object_graph_reference', 'predicate_object_graph_template'
]


##############################################################################
########################   MAPPING PARSING QUERIES   #########################
##############################################################################

MAPPING_PARSING_QUERY = """
    # This query has been reused from SDM-RDFizer (https://github.com/SDM-TIB/SDM-RDFizer). SDM-RDFizer has been
    # developed by members of the Scientific Data Management Group at TIB. Its development has been coordinated and
    # supervised by Maria-Esther Vidal. The implementation has been done by Enrique Iglesias and Guillermo Betancourt
    # under the supervision of David Chaves-Fraga, Samaneh Jozashoori, and Kemele Endris.
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