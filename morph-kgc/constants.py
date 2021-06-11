__version__ = "v1.0.1"

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
DEFAULT_OUTPUT_FORMAT = 'N-QUADS'
DEFAULT_REMOVE_DUPLICATES = 'yes'
DEFAULT_CLEAN_OUTPUT_DIR = 'yes'
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
DEFAULT_NA_FILTER = 'yes'
DEFAULT_NA_VALUES = ',#N/A,N/A,#N/A N/A,n/a,NA,<NA>,#NA,NULL,null,NaN,nan'
DEFAULT_ONLY_PRINTABLE_CHARACTERS = 'no'
DEFAULT_MATERIALIZE_DEFAULT_GRAPH = 'no'


##############################################################################
########################   MAPPING PARTITION OPTIONS   #######################
##############################################################################
PARTIAL_AGGREGATION_PARTITION = 'PARTIAL-AGGREGATIONS'
NO_PARTITIONING = 'NO-PARTITIONING'


##############################################################################
#########################   VALID ARGUMENTS VALUES   #########################
##############################################################################

VALID_RELATIONAL_SOURCE_TYPES = ['RDB']
VALID_TABULAR_SOURCE_TYPES = ['CSV']
VALID_DATA_SOURCE_TYPES = VALID_RELATIONAL_SOURCE_TYPES + VALID_TABULAR_SOURCE_TYPES
VALID_OUTPUT_FORMATS = ['N-TRIPLES', 'N-QUADS']
VALID_PROCESS_START_METHOD = ['DEFAULT', 'SPAWN', 'FORK', 'FORKSERVER']
VALID_LOGGING_LEVEL = ['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']


##############################################################################
###################   FILE EXTENSIONS FOR OUTPUT FORMATS   ###################
##############################################################################

OUTPUT_FORMAT_FILE_EXTENSION = {
    'N-TRIPLES': '.nt',
    'N-QUADS': '.nq'
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
