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
DEFAULT_LOGGING_FILE = ''
DEFAULT_LOGGING_LEVEL = 'INFO'
DEFAULT_INFER_SQL_DATATYPES = 'no'
DEFAULT_NUMBER_OF_PROCESSES = 2 * mp.cpu_count()
DEFAULT_CHUNKSIZE = 100000
DEFAULT_NA_FILTER = 'yes'
DEFAULT_NA_VALUES = ',#N/A,N/A,#N/A N/A,n/a,NA,<NA>,#NA,NULL,null,NaN,nan'
DEFAULT_ONLY_PRINTABLE_CHARACTERS = 'no'
DEFAULT_MATERIALIZE_DEFAULT_GRAPH = 'no'

# ORACLE
DEFAULT_ORACLE_CLIENT_LIB_DIR = ''
DEFAULT_ORACLE_CLIENT_CONFIG_DIR = ''

# DEVELOPMENT OPTIONS
DEFAULT_READ_PARSED_MAPPINGS_PATH = ''
DEFAULT_WRITE_PARSED_MAPPINGS_PATH = ''


##############################################################################
########################   MAPPING PARTITION OPTIONS   #######################
##############################################################################

PARTIAL_AGGREGATIONS_PARTITIONING = 'PARTIAL-AGGREGATIONS'
MAXIMAL_PARTITIONING = 'MAXIMAL'
NO_PARTITIONING = ['NO', 'FALSE', 'OFF', '0']


##############################################################################
#########################   DATA SOURCE TYPES   ##############################
##############################################################################

RDB = 'RDB'
CSV = 'CSV'
TSV = 'TSV'
EXCEL = 'EXCEL'
PARQUET = 'PARQUET'
FEATHER = 'FEATHER'
ORC = 'ORC'
STATA = 'STATA'
SAS = 'SAS'
SPSS = 'SPSS'
JSON = 'JSON'
XML = 'XML'

# DBMSs
MYSQL = 'MYSQL'
MARIADB = 'MARIADB'
MSSQL = 'MSSQL'
ORACLE = 'ORACLE'
POSTGRESQL = 'POSTGRESQL'

FILE_SOURCE_TYPES = [CSV, TSV, EXCEL, PARQUET, FEATHER, ORC, STATA, SAS, SPSS, JSON, XML]
DATA_SOURCE_TYPES = [RDB] + FILE_SOURCE_TYPES


##############################################################################
#########################   VALID ARGUMENTS VALUES   #########################
##############################################################################

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
###########################   R2RML SPECIFICATION   ##########################
##############################################################################

R2RML_NAMESPACE = 'http://www.w3.org/ns/r2rml#'

# classes
R2RML_BASE_TABLE_OR_VIEW_CLASS = 'http://www.w3.org/ns/r2rml#BaseTableOrView'
R2RML_GRAPH_MAP_CLASS = 'http://www.w3.org/ns/r2rml#GraphMap'
R2RML_JOIN_CLASS = 'http://www.w3.org/ns/r2rml#Join'
R2RML_LOGICAL_TABLE_CLASS = 'http://www.w3.org/ns/r2rml#LogicalTable'
R2RML_OBJECT_MAP_CLASS = 'http://www.w3.org/ns/r2rml#ObjectMap'
R2RML_PREDICATE_MAP_CLASS = 'http://www.w3.org/ns/r2rml#PredicateMap'
R2RML_PREDICATE_OBJECT_MAP_CLASS = 'http://www.w3.org/ns/r2rml#PredicateObjectMap'
R2RML_R2RML_VIEW_CLASS = 'http://www.w3.org/ns/r2rml#R2RMLView'
R2RML_REF_OBJECT_MAP_CLASS = 'http://www.w3.org/ns/r2rml#RefObjectMap'
R2RML_SUBJECT_MAP_CLASS = 'http://www.w3.org/ns/r2rml#SubjectMap'
R2RML_TERM_MAP_CLASS = 'http://www.w3.org/ns/r2rml#TermMap'
R2RML_TRIPLES_MAP_CLASS = 'http://www.w3.org/ns/r2rml#TriplesMap'

# properties
R2RML_LOGICAL_TABLE = 'http://www.w3.org/ns/r2rml#logicalTable'
R2RML_TABLE_NAME = 'http://www.w3.org/ns/r2rml#tableName'
R2RML_PARENT_TRIPLES_MAP = 'http://www.w3.org/ns/r2rml#parentTriplesMap'
R2RML_SUBJECT_MAP = 'http://www.w3.org/ns/r2rml#subjectMap'
R2RML_PREDICATE_MAP = 'http://www.w3.org/ns/r2rml#predicateMap'
R2RML_OBJECT_MAP = 'http://www.w3.org/ns/r2rml#objectMap'
R2RML_GRAPH_MAP = 'http://www.w3.org/ns/r2rml#graphMap'
R2RML_SUBJECT_CONSTANT_SHORTCUT = 'http://www.w3.org/ns/r2rml#subject'
R2RML_PREDICATE_CONSTANT_SHORTCUT = 'http://www.w3.org/ns/r2rml#predicate'
R2RML_OBJECT_CONSTANT_SHORTCUT = 'http://www.w3.org/ns/r2rml#object'
R2RML_GRAPH_CONSTANT_SHORTCUT = 'http://www.w3.org/ns/r2rml#graph'
R2RML_PREDICATE_OBJECT_MAP = 'http://www.w3.org/ns/r2rml#predicateObjectMap'
R2RML_CONSTANT = 'http://www.w3.org/ns/r2rml#constant'
R2RML_TEMPLATE = 'http://www.w3.org/ns/r2rml#template'
R2RML_COLUMN = 'http://www.w3.org/ns/r2rml#column'
R2RML_CLASS = 'http://www.w3.org/ns/r2rml#class'
R2RML_CHILD = 'http://www.w3.org/ns/r2rml#child'
R2RML_PARENT = 'http://www.w3.org/ns/r2rml#parent'
R2RML_JOIN_CONDITION = 'http://www.w3.org/ns/r2rml#joinCondition'
R2RML_DATATYPE = 'http://www.w3.org/ns/r2rml#datatype'
R2RML_LANGUAGE = 'http://www.w3.org/ns/r2rml#language'
R2RML_SQL_QUERY = 'http://www.w3.org/ns/r2rml#sqlQuery'
R2RML_SQL_VERSION = 'http://www.w3.org/ns/r2rml#sqlVersion'
R2RML_TERM_TYPE = 'http://www.w3.org/ns/r2rml#termType'

# other
R2RML_DEFAULT_GRAPH = 'http://www.w3.org/ns/r2rml#defaultGraph'
R2RML_IRI = 'http://www.w3.org/ns/r2rml#IRI'
R2RML_LITERAL = 'http://www.w3.org/ns/r2rml#Literal'
R2RML_BLANK_NODE = 'http://www.w3.org/ns/r2rml#BlankNode'
R2RML_SQL2008 = 'http://www.w3.org/ns/r2rml#SQL2008'


##############################################################################
############################   RML SPECIFICATION   ###########################
##############################################################################

QL_NAMESPACE = 'http://semweb.mmlab.be/ns/ql#'
QL_CSV = 'http://semweb.mmlab.be/ns/ql#CSV'
QL_JSON = 'http://semweb.mmlab.be/ns/ql#JSONPath'
QL_XML = 'http://semweb.mmlab.be/ns/ql#XPath'

RML_NAMESPACE = 'http://semweb.mmlab.be/ns/rml#'
RML_LOGICAL_SOURCE = 'http://semweb.mmlab.be/ns/rml#logicalSource'
RML_QUERY = 'http://semweb.mmlab.be/ns/rml#query'
RML_ITERATOR = 'http://semweb.mmlab.be/ns/rml#iterator'
RML_REFERENCE = 'http://semweb.mmlab.be/ns/rml#reference'
RML_REFERENCE_FORMULATION = 'http://semweb.mmlab.be/ns/rml#referenceFormulation'


##############################################################################
##################################   OTHER   #################################
##############################################################################

AUXILIAR_UNIQUE_REPLACING_STRING = 'zzyy_xxww\u200B'
