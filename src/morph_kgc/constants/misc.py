__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""Miscellaneous internal constants."""

# Used by template parser to safely replace escaped braces during regex extraction
AUXILIAR_UNIQUE_REPLACING_STRING = "zzyy_xxww​"

# Root logger name for the whole morph-kgc package
LOGGING_NAMESPACE = "morph_kgc"

# Config section and key names
CONFIGURATION_SECTION   = "CONFIGURATION"
NUMBER_OF_PROCESSES     = "number_of_processes"
INFER_SQL_DATATYPES     = "infer_sql_datatypes"
ENFORCE_SQL_QUERY_FILTER_NULL = "enforce_sql_query_filter_null"
ONLY_PRINTABLE_CHARS    = "only_write_printable_characters"
NA_VALUES               = "na_values"
LITERAL_ESCAPING_CHARS  = "literal_escaping_chars"
SAFE_PERCENT_ENCODING   = "safe_percent_encoding"
UDFS                    = "udfs"
API_TOKEN               = "api_token"
STATE_DIR               = "state_dir"
RESOURCE_SECTION_PREFIX = "RESOURCE:"
