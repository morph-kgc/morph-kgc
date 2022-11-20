__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


##############################################################################
########################   MAPPING PARTITION OPTIONS   #######################
##############################################################################

PARTIAL_AGGREGATIONS_PARTITIONING = 'PARTIAL-AGGREGATIONS'
MAXIMAL_PARTITIONING = 'MAXIMAL'
NO_PARTITIONING = ['NO', 'FALSE', 'OFF', '0']


##############################################################################
########################   MAPPING PARTITION OPTIONS   #######################
##############################################################################

PARTIAL_AGGREGATIONS_PARTITIONING = 'PARTIAL-AGGREGATIONS'
MAXIMAL_PARTITIONING = 'MAXIMAL'
NO_PARTITIONING = ['NO', 'FALSE', 'OFF', '0']


##############################################################################
#########################   DATA SOURCE TYPES   ##############################
##############################################################################

# data files
TV = 'TV'   # RML Tabular View
CSV = 'CSV'
TSV = 'TSV'
EXCEL = ['XLS', 'XLSX', 'XLSM', 'XLSB']
ODS = ['ODS', 'FODS']
PARQUET = 'PARQUET'
FEATHER = ['FEATHER', 'FEA']
ORC = 'ORC'
STATA = 'DTA'
SAS = ['XPT', 'SAS7BDAT']
SPSS = 'SAV'
JSON = 'JSON'
XML = 'XML'

# DBMSs
RDB = 'RDB'
MYSQL = 'MYSQL'
MARIADB = 'MARIADB'
MSSQL = 'MSSQL'
ORACLE = 'ORACLE'
POSTGRESQL = 'POSTGRESQL'
SQLITE = 'SQLITE'

FILE_SOURCE_TYPES = [TV, CSV, TSV, PARQUET, ORC, STATA, SPSS, JSON, XML] + EXCEL + FEATHER + SAS + ODS
DATA_SOURCE_TYPES = [RDB] + FILE_SOURCE_TYPES

# RDF serializations
NTRIPLES = 'N-TRIPLES'
NQUADS = 'N-QUADS'


##############################################################################
#########################   VALID ARGUMENTS VALUES   #########################
##############################################################################

VALID_OUTPUT_FORMATS = [NTRIPLES, NQUADS]
VALID_LOGGING_LEVEL = ['NOTSET', 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']


##############################################################################
###################   FILE EXTENSIONS FOR OUTPUT FORMATS   ###################
##############################################################################

OUTPUT_FORMAT_FILE_EXTENSION = {
    NTRIPLES: '.nt',
    NQUADS: '.nq'
}


##############################################################################
###########################   R2RML SPECIFICATION   ##########################
##############################################################################

R2RML_NAMESPACE = 'http://www.w3.org/ns/r2rml#'

# classes
R2RML_BASE_TABLE_OR_VIEW_CLASS = f'{R2RML_NAMESPACE}BaseTableOrView'
R2RML_GRAPH_MAP_CLASS = f'{R2RML_NAMESPACE}GraphMap'
R2RML_JOIN_CLASS = f'{R2RML_NAMESPACE}Join'
R2RML_LOGICAL_TABLE_CLASS = f'{R2RML_NAMESPACE}LogicalTable'
R2RML_OBJECT_MAP_CLASS = f'{R2RML_NAMESPACE}ObjectMap'
R2RML_PREDICATE_MAP_CLASS = f'{R2RML_NAMESPACE}PredicateMap'
R2RML_PREDICATE_OBJECT_MAP_CLASS = f'{R2RML_NAMESPACE}PredicateObjectMap'
R2RML_R2RML_VIEW_CLASS = f'{R2RML_NAMESPACE}R2RMLView'
R2RML_REF_OBJECT_MAP_CLASS = f'{R2RML_NAMESPACE}RefObjectMap'
R2RML_SUBJECT_MAP_CLASS = f'{R2RML_NAMESPACE}SubjectMap'
R2RML_TERM_MAP_CLASS = f'{R2RML_NAMESPACE}TermMap'
R2RML_TRIPLES_MAP_CLASS = f'{R2RML_NAMESPACE}TriplesMap'

# properties
R2RML_LOGICAL_TABLE = f'{R2RML_NAMESPACE}logicalTable'
R2RML_TABLE_NAME = f'{R2RML_NAMESPACE}tableName'
R2RML_PARENT_TRIPLES_MAP = f'{R2RML_NAMESPACE}parentTriplesMap'
R2RML_SUBJECT_MAP = f'{R2RML_NAMESPACE}subjectMap'
R2RML_PREDICATE_MAP = f'{R2RML_NAMESPACE}predicateMap'
R2RML_OBJECT_MAP = f'{R2RML_NAMESPACE}objectMap'
R2RML_GRAPH_MAP = f'{R2RML_NAMESPACE}graphMap'
R2RML_SUBJECT_CONSTANT_SHORTCUT = f'{R2RML_NAMESPACE}subject'
R2RML_PREDICATE_CONSTANT_SHORTCUT = f'{R2RML_NAMESPACE}predicate'
R2RML_OBJECT_CONSTANT_SHORTCUT = f'{R2RML_NAMESPACE}object'
R2RML_GRAPH_CONSTANT_SHORTCUT = f'{R2RML_NAMESPACE}graph'
R2RML_PREDICATE_OBJECT_MAP = f'{R2RML_NAMESPACE}predicateObjectMap'
R2RML_CONSTANT = f'{R2RML_NAMESPACE}constant'
R2RML_TEMPLATE = f'{R2RML_NAMESPACE}template'
R2RML_COLUMN = f'{R2RML_NAMESPACE}column'
R2RML_CLASS = f'{R2RML_NAMESPACE}class'
R2RML_CHILD = f'{R2RML_NAMESPACE}child'
R2RML_PARENT = f'{R2RML_NAMESPACE}parent'
R2RML_JOIN_CONDITION = f'{R2RML_NAMESPACE}joinCondition'
R2RML_DATATYPE = f'{R2RML_NAMESPACE}datatype'
R2RML_LANGUAGE = f'{R2RML_NAMESPACE}language'
R2RML_SQL_QUERY = f'{R2RML_NAMESPACE}sqlQuery'
R2RML_SQL_VERSION = f'{R2RML_NAMESPACE}sqlVersion'
R2RML_TERM_TYPE = f'{R2RML_NAMESPACE}termType'

# other
R2RML_DEFAULT_GRAPH = f'{R2RML_NAMESPACE}defaultGraph'
R2RML_IRI = f'{R2RML_NAMESPACE}IRI'
R2RML_LITERAL = f'{R2RML_NAMESPACE}Literal'
R2RML_BLANK_NODE = f'{R2RML_NAMESPACE}BlankNode'
R2RML_SQL2008 = f'{R2RML_NAMESPACE}SQL2008'


##############################################################################
############################   RML SPECIFICATION   ###########################
##############################################################################

RML_NAMESPACE = 'http://semweb.mmlab.be/ns/rml#'

# properties
RML_LOGICAL_SOURCE = f'{RML_NAMESPACE}logicalSource'
RML_SOURCE = f'{RML_NAMESPACE}source'
RML_QUERY = f'{RML_NAMESPACE}query'
RML_ITERATOR = f'{RML_NAMESPACE}iterator'
RML_REFERENCE = f'{RML_NAMESPACE}reference'
RML_REFERENCE_FORMULATION = f'{RML_NAMESPACE}referenceFormulation'

# ql
QL_NAMESPACE = 'http://semweb.mmlab.be/ns/ql#'
QL_CSV = f'{QL_NAMESPACE}CSV'
QL_JSON = f'{QL_NAMESPACE}JSONPath'
QL_XML = f'{QL_NAMESPACE}XPath'


##############################################################################
############################   RML-star SPECIFICATION   ###########################
##############################################################################

# classes
RML_STAR_STAR_MAP_CLASS = f'{RML_NAMESPACE}StarMap'
RML_STAR_NON_ASSERTED_TRIPLES_MAP_CLASS = f'{RML_NAMESPACE}NonAssertedTriplesMap'

# properties
RML_STAR_QUOTED_TRIPLES_MAP = f'{RML_NAMESPACE}quotedTriplesMap'
RML_STAR_SUBJECT_MAP = f'{RML_NAMESPACE}subjectMap'
RML_STAR_OBJECT_MAP = f'{RML_NAMESPACE}objectMap'
RML_STAR_SUBJECT_CONSTANT_SHORTCUT = f'{RML_NAMESPACE}subject'
RML_STAR_OBJECT_CONSTANT_SHORTCUT = f'{RML_NAMESPACE}object'

# other
RML_STAR_RDF_STAR_TRIPLE = f'{RML_NAMESPACE}RDFstarTriple'


##############################################################################
#############################   XSD DATA TYPES   #############################
##############################################################################

XSD_NAMESPACE = 'http://www.w3.org/2001/XMLSchema#'

XSD_HEX_BINARY = f'{XSD_NAMESPACE}hexBinary'
XSD_INTEGER = f'{XSD_NAMESPACE}integer'
XSD_DECIMAL = f'{XSD_NAMESPACE}decimal'
XSD_DOUBLE = f'{XSD_NAMESPACE}double'
XSD_BOOLEAN = f'{XSD_NAMESPACE}boolean'
XSD_DATE = f'{XSD_NAMESPACE}date'
XSD_TIME = f'{XSD_NAMESPACE}time'
XSD_DATETIME = f'{XSD_NAMESPACE}dateTime'
XSD_STRING = f'{XSD_NAMESPACE}string'


##############################################################################
##################################   OTHER   #################################
##############################################################################

RDF_NAMESPACE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#'
RDF_TYPE = f'{RDF_NAMESPACE}type'

AUXILIAR_UNIQUE_REPLACING_STRING = 'zzyy_xxww\u200B'
