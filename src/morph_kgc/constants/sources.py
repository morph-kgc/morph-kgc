__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""Data source type constants."""

# ── Tabular / file formats ────────────────────────────────────────────────────
CSV        = "CSV"
TSV        = "TSV"
EXCEL      = ["XLS", "XLSX", "XLSM", "XLSB"]
ODS        = ["ODS", "FODS"]
PARQUET    = "PARQUET"
GEOPARQUET = "GEOPARQUET"
FEATHER    = ["FEATHER", "FEA"]
ORC        = "ORC"
STATA      = "DTA"
SAS        = ["XPT", "SAS7BDAT"]
SPSS       = "SAV"
JSON       = ["JSON", "GEOJSON", "JSONPATH"]
XML        = ["XML", "XPATH"]
SHP        = "SHP"

# ── Relational databases ──────────────────────────────────────────────────────
RDB         = "RDB"
MYSQL       = "MYSQL"
MARIADB     = "MARIADB"
MSSQL       = "MSSQL"
ORACLE      = "ORACLE"
POSTGRESQL  = "POSTGRESQL"
SQLITE      = "SQLITE"
DATABRICKS  = "DATABRICKS"
SNOWFLAKE   = "SNOWFLAKE"

# ── Property-graph databases ──────────────────────────────────────────────────
PGDB = "PGDB"

# ── HTTP API ──────────────────────────────────────────────────────────────────
HTTPAPI = "HTTPAPI"

# ── In-memory data structures ─────────────────────────────────────────────────
PYTHON_SOURCE = "PYTHON_SOURCE"
DATAFRAME     = "DATAFRAME"
DICTIONARY    = "DICTIONARY"
JSON_STRING   = "JSON_STRING"

# ── Aggregate sets ────────────────────────────────────────────────────────────
FILE_SOURCE_TYPES = (
    [CSV, TSV, PARQUET, GEOPARQUET, ORC, STATA, SPSS, SHP]
    + XML + JSON + EXCEL + FEATHER + SAS + ODS
)
DATA_SOURCE_TYPES = [RDB] + [PGDB] + FILE_SOURCE_TYPES
IN_MEMORY_TYPES   = [PYTHON_SOURCE, DATAFRAME, DICTIONARY, JSON_STRING]
