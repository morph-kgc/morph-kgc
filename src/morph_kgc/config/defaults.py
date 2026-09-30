__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]
__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"

import multiprocessing as mp

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
DEFAULT_OUTPUT_FILE: str = "knowledge-graph"
DEFAULT_OUTPUT_DIR: str = ""
DEFAULT_OUTPUT_FORMAT: str = "N-TRIPLES"

# ---------------------------------------------------------------------------
# Data sources and Serialization
# ---------------------------------------------------------------------------
# Set of ASCII characters that should not be percent encoded.
# All characters are encoded by default.
DEFAULT_SAFE_PERCENT_ENCODING: str = ""
# Comma-separated list stored as a string so it survives round-trips through
# INI files. The model converts it to list[str] on construction.
# See issue #321: ",\,\n,\r are always escaped.
DEFAULT_LITERAL_ESCAPING_CHARS: str = '",\n,\r'
# Set of values to be interpreted as NULL when retrieving data from the input
# sources. The set of values must be separated by commas.
# Empty string is valid (disables NA handling).
DEFAULT_NA_VALUES: str = ",nan"

# ---------------------------------------------------------------------------
# Mapping
# ---------------------------------------------------------------------------
DEFAULT_MAPPING_PARTITIONING: str = "PARTIAL-AGGREGATIONS"
DEFAULT_INFER_SQL_DATATYPES: bool = False

# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
DEFAULT_NUMBER_OF_PROCESSES: int = 2 * mp.cpu_count()

# ---------------------------------------------------------------------------
# Functions / UDFs
# ---------------------------------------------------------------------------
DEFAULT_UDFS: str = ""
DEFAULT_API_TOKEN: str = ""
# Directory where the shared context of stateful functions is persisted.
# Empty means a temporary directory created (and removed) for every run.
DEFAULT_STATE_DIR: str = ""

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
DEFAULT_LOGGING_LEVEL: str = "INFO"
DEFAULT_LOGGING_FILE: str = ""
