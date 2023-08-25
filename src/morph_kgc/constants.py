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
#########################   DATA SOURCE TYPES   ##############################
##############################################################################

# data files
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
JSON = ['JSON', 'GEOJSON']
XML = 'XML'

# DBMSs
RDB = 'RDB'
MYSQL = 'MYSQL'
MARIADB = 'MARIADB'
MSSQL = 'MSSQL'
ORACLE = 'ORACLE'
POSTGRESQL = 'POSTGRESQL'
SQLITE = 'SQLITE'
DATABRICKS = 'DATABRICKS'

# in-memory data
PYTHON_SOURCE = 'PYTHON_SOURCE'
DATAFRAME = 'DATAFRAME'
DICTIONARY = 'DICTIONARY'
JSON_STRING = 'JSON_STRING'

FILE_SOURCE_TYPES = [CSV, TSV, PARQUET, ORC, STATA, SPSS, XML] + JSON + EXCEL + FEATHER + SAS + ODS
DATA_SOURCE_TYPES = [RDB] + FILE_SOURCE_TYPES
IN_MEMORY_TYPES = [PYTHON_SOURCE, DATAFRAME, DICTIONARY, JSON_STRING]

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
R2RML_SUBJECT_SHORTCUT = f'{R2RML_NAMESPACE}subject'
R2RML_PREDICATE_SHORTCUT = f'{R2RML_NAMESPACE}predicate'
R2RML_OBJECT_SHORTCUT = f'{R2RML_NAMESPACE}object'
R2RML_GRAPH_SHORTCUT = f'{R2RML_NAMESPACE}graph'
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
############################   RML-Core   #########################################
##############################################################################

RML_NAMESPACE = 'http://w3id.org/rml/'

RML_TRIPLES_MAP_CLASS = f'{RML_NAMESPACE}TriplesMap'
RML_LOGICAL_SOURCE = f'{RML_NAMESPACE}logicalSource'
RML_SOURCE = f'{RML_NAMESPACE}source'
RML_QUERY = f'{RML_NAMESPACE}query'
RML_ITERATOR = f'{RML_NAMESPACE}iterator'
RML_REFERENCE = f'{RML_NAMESPACE}reference'
RML_REFERENCE_FORMULATION = f'{RML_NAMESPACE}referenceFormulation'
RML_LOGICAL_TABLE = f'{RML_NAMESPACE}logicalTable'
RML_TABLE_NAME = f'{RML_NAMESPACE}tableName'
RML_PARENT_TRIPLES_MAP = f'{RML_NAMESPACE}parentTriplesMap'
RML_SUBJECT_MAP = f'{RML_NAMESPACE}subjectMap'
RML_PREDICATE_MAP = f'{RML_NAMESPACE}predicateMap'
RML_OBJECT_MAP = f'{RML_NAMESPACE}objectMap'
RML_GRAPH_MAP = f'{RML_NAMESPACE}graphMap'
RML_SUBJECT_SHORTCUT = f'{RML_NAMESPACE}subject'
RML_PREDICATE_SHORTCUT = f'{RML_NAMESPACE}predicate'
RML_OBJECT_SHORTCUT = f'{RML_NAMESPACE}object'
RML_GRAPH_SHORTCUT = f'{RML_NAMESPACE}graph'
RML_PREDICATE_OBJECT_MAP = f'{RML_NAMESPACE}predicateObjectMap'
RML_CONSTANT = f'{RML_NAMESPACE}constant'
RML_TEMPLATE = f'{RML_NAMESPACE}template'
RML_COLUMN = f'{RML_NAMESPACE}column'
RML_CLASS = f'{RML_NAMESPACE}class'
RML_CHILD = f'{RML_NAMESPACE}child'
RML_PARENT = f'{RML_NAMESPACE}parent'
RML_JOIN_CONDITION = f'{RML_NAMESPACE}joinCondition'
RML_DATATYPE = f'{RML_NAMESPACE}datatype'
RML_LANGUAGE = f'{RML_NAMESPACE}language'
RML_SQL_QUERY = f'{RML_NAMESPACE}sqlQuery'
RML_SQL_VERSION = f'{RML_NAMESPACE}sqlVersion'
RML_TERM_TYPE = f'{RML_NAMESPACE}termType'
RML_DEFAULT_GRAPH = f'{RML_NAMESPACE}defaultGraph'

RML_IRI = f'{RML_NAMESPACE}IRI'
RML_LITERAL = f'{RML_NAMESPACE}Literal'
RML_BLANK_NODE = f'{RML_NAMESPACE}BlankNode'

RML_SQL2008 = f'{RML_NAMESPACE}SQL2008'
RML_CSV = f'{RML_NAMESPACE}CSV'
RML_JSONPATH = f'{RML_NAMESPACE}JSONPath'
RML_XPATH = f'{RML_NAMESPACE}XPath'


# RML legacy
RML_LEGACY_NAMESPACE = 'http://semweb.mmlab.be/ns/rml#'

RML_LEGACY_LOGICAL_SOURCE = f'{RML_LEGACY_NAMESPACE}logicalSource'
RML_LEGACY_SOURCE = f'{RML_LEGACY_NAMESPACE}source'
RML_LEGACY_QUERY = f'{RML_LEGACY_NAMESPACE}query'
RML_LEGACY_ITERATOR = f'{RML_LEGACY_NAMESPACE}iterator'
RML_LEGACY_REFERENCE = f'{RML_LEGACY_NAMESPACE}reference'
RML_LEGACY_REFERENCE_FORMULATION = f'{RML_LEGACY_NAMESPACE}referenceFormulation'
QL_NAMESPACE = 'http://semweb.mmlab.be/ns/ql#'
QL_CSV = f'{QL_NAMESPACE}CSV'
QL_JSON = f'{QL_NAMESPACE}JSONPath'
QL_XML = f'{QL_NAMESPACE}XPath'


##############################################################################
############################   RML-star   ####################################
##############################################################################

RML_STAR_MAP_CLASS = f'{RML_NAMESPACE}StarMap'
RML_ASSERTED_TRIPLES_MAP_CLASS = f'{RML_NAMESPACE}AssertedTriplesMap'
RML_NON_ASSERTED_TRIPLES_MAP_CLASS = f'{RML_NAMESPACE}NonAssertedTriplesMap'
RML_QUOTED_TRIPLES_MAP = f'{RML_NAMESPACE}quotedTriplesMap'
RML_RDF_STAR_TRIPLE = f'{RML_NAMESPACE}RDFstarTriple'


# RML-star legacy
RML_LEGACY_STAR_MAP_CLASS = f'{RML_LEGACY_NAMESPACE}StarMap'
RML_LEGACY_NON_ASSERTED_TRIPLES_MAP_CLASS = f'{RML_LEGACY_NAMESPACE}NonAssertedTriplesMap'
RML_LEGACY_QUOTED_TRIPLES_MAP = f'{RML_LEGACY_NAMESPACE}quotedTriplesMap'
RML_LEGACY_RDF_STAR_TRIPLE = f'{RML_LEGACY_NAMESPACE}RDFstarTriple'
RML_LEGACY_SUBJECT_MAP = f'{RML_LEGACY_NAMESPACE}subjectMap'
RML_LEGACY_OBJECT_MAP = f'{RML_LEGACY_NAMESPACE}objectMap'

##############################################################################
############################   RML-FNML   ####################################
##############################################################################

FNO_NAMESPACE = 'https://w3id.org/function/ontology#'

# FnO classes
FNO_FUNCTION = f'{FNO_NAMESPACE}Function'
FNO_EXECUTION = f'{FNO_NAMESPACE}Execution'
FNO_PARAMETER = f'{FNO_NAMESPACE}Parameter'
FNO_OUTPUT = f'{FNO_NAMESPACE}Output'

# FnO properties
FNO_EXECUTES = f'{FNO_NAMESPACE}executes'
FNO_PREDICATE = f'{FNO_NAMESPACE}predicate'
FNO_TYPE = f'{FNO_NAMESPACE}type'
FNO_REQUIRED = f'{FNO_NAMESPACE}required'
FNO_NAME = f'{FNO_NAMESPACE}name'
FNO_SOLVES = f'{FNO_NAMESPACE}solves'
FNO_EXPECTS = f'{FNO_NAMESPACE}expects'
FNO_RETURNS = f'{FNO_NAMESPACE}returns'

# FNML
RML_EXECUTION = f'{RML_NAMESPACE}functionExecution'
RML_INPUT = f'{RML_NAMESPACE}input'
RML_FUNCTION_MAP = f'{RML_NAMESPACE}functionMap'
RML_RETURN_MAP = f'{RML_NAMESPACE}returnMap'
RML_PARAMETER_MAP = f'{RML_NAMESPACE}parameterMap'
RML_VALUE_MAP = f'{RML_NAMESPACE}inputValueMap'
RML_FUNCTION_SHORTCUT = f'{RML_NAMESPACE}function'
RML_RETURN_SHORTCUT = f'{RML_NAMESPACE}return'
RML_PARAMETER_SHORTCUT = f'{RML_NAMESPACE}parameter'
RML_VALUE_SHORTCUT = f'{RML_NAMESPACE}inputValue'


# FNML legacy
FNML_NAMESPACE = 'http://semweb.mmlab.be/ns/fnml#'
FNML_EXECUTION = f'{FNML_NAMESPACE}execution'
FNML_INPUT = f'{FNML_NAMESPACE}input'
FNML_FUNCTION_MAP = f'{FNML_NAMESPACE}functionMap'
FNML_RETURN_MAP = f'{FNML_NAMESPACE}returnMap'
FNML_PARAMETER_MAP = f'{FNML_NAMESPACE}parameterMap'
FNML_VALUE_MAP = f'{FNML_NAMESPACE}valueMap'
FNML_FUNCTION_SHORTCUT = f'{FNML_NAMESPACE}function'
FNML_RETURN_SHORTCUT = f'{FNML_NAMESPACE}return'
FNML_PARAMETER_SHORTCUT = f'{FNML_NAMESPACE}parameter'
FNML_VALUE_SHORTCUT = f'{FNML_NAMESPACE}value'


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

RDFS_NAMESPACE = 'http://www.w3.org/2000/01/rdf-schema#'

AUXILIAR_UNIQUE_REPLACING_STRING = 'zzyy_xxww\u200B'
