__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]
__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"

import errno
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from .defaults import (
    DEFAULT_OUTPUT_FILE,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_OUTPUT_FORMAT,
    DEFAULT_SAFE_PERCENT_ENCODING,
    DEFAULT_LITERAL_ESCAPING_CHARS,
    DEFAULT_NA_VALUES,
    DEFAULT_MAPPING_PARTITIONING,
    DEFAULT_INFER_SQL_DATATYPES,
    DEFAULT_NUMBER_OF_PROCESSES,
    DEFAULT_UDFS,
    DEFAULT_API_TOKEN,
    DEFAULT_STATE_DIR,
    DEFAULT_LOGGING_LEVEL,
    DEFAULT_LOGGING_FILE,
)

LOGGER = logging.getLogger("morph_kgc")

# ---------------------------------------------------------------------------
# Valid option sets.
# ---------------------------------------------------------------------------
VALID_OUTPUT_FORMATS = {"N-TRIPLES", "N-QUADS", "JELLY"}
VALID_LOGGING_LEVELS = {"NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
VALID_PARTITIONING = {"PARTIAL-AGGREGATIONS", "MAXIMAL", "NO", "FALSE", "OFF", "0"}
OUTPUT_FORMAT_FILE_EXTENSION = {
    "N-TRIPLES": ".nt",
    "N-QUADS": ".nq",
    "JELLY": ".jelly",
}


# ---------------------------------------------------------------------------
# Per-data-source configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class DataSourceConfig:
    """Holds all options for a single ``[DATA_SOURCE_NAME]`` INI section."""

    name: str
    mappings: list[str]
    # Databases
    db_url: str = ""
    connect_args: str = ""
    # File-based sources
    file_path: str = ""

    def has_db_url(self) -> bool:
        return bool(self.db_url)

    def has_file_path(self) -> bool:
        return bool(self.file_path)

    def has_connect_args(self) -> bool:
        return bool(self.connect_args)

    def get_db_url_with_env(self) -> str:
        """Expand ``{ENV_VAR}`` placeholders in the DB URL."""
        return self.db_url.format(**os.environ)


# ---------------------------------------------------------------------------
# Per-resource configuration
# ---------------------------------------------------------------------------

# Resource types understood by the built-in reconciliation functions. Any other
# value is accepted as well, so that user-defined stateful functions can declare
# resource types of their own.
SKOS_VOCABULARY_RESOURCE = "SKOS_VOCABULARY"
SPARQL_ENDPOINT_RESOURCE = "SPARQL_ENDPOINT"


@dataclass(frozen=True)
class ResourceConfig:
    """
    Holds all options for a single ``[RESOURCE:<name>]`` INI section.

    A resource describes something the engine accesses while materializing but
    that is not a data source: a SKOS vocabulary to reconcile against, a SPARQL
    endpoint to query, ... Keeping them in the configuration file (instead of
    in the mapping) decouples the mapping from deployment-specific details such
    as URLs and credentials.

    ``resource_type``, ``url``, ``username`` and ``password`` are common to all
    resources; every other option of the section is kept verbatim in
    ``options`` so that user-defined resource types need no engine changes.
    """

    name: str
    resource_type: str = ""
    url: str = ""
    username: str = ""
    password: str = ""
    options: dict = field(default_factory=dict)

    # -- Option accessors ---------------------------------------------------

    def get(self, key: str, default: str = "") -> str:
        """Return the (extra) option *key*, or *default* when not declared."""
        value = self.options.get(key.lower())
        return default if value is None or value == "" else value

    def get_int(self, key: str, default: int) -> int:
        value = self.get(key)
        if not value:
            return default
        try:
            return int(value)
        except ValueError:
            raise ValueError(
                f"Option '{key}' of resource '{self.name}' must be an integer, "
                f"got '{value}'."
            )

    def get_list(self, key: str) -> list[str]:
        """Split a comma-separated option into a list of trimmed values."""
        return [item.strip() for item in self.get(key).split(",") if item.strip()]

    def has_credentials(self) -> bool:
        return bool(self.username) or bool(self.password)

    def identifiers(self) -> tuple[str, ...]:
        """
        Values, besides the section name, a mapping may use to name this
        resource: the IRI that identifies it and the URL it is accessed at.
        They differ when a vocabulary is published under a canonical IRI but
        downloaded from somewhere else.
        """
        return tuple(
            identifier
            for identifier in (self.get("iri"), self.url)
            if identifier
        )

    # -- Environment-variable expansion -------------------------------------

    def get_url(self) -> str:
        return self._expand_env("url", self.url)

    def get_username(self) -> str:
        return self._expand_env("username", self.username)

    def get_password(self) -> str:
        return self._expand_env("password", self.password)

    def _expand_env(self, key: str, value: str) -> str:
        """Expand ``{ENV_VAR}`` placeholders, as done for ``db_url``."""
        if "{" not in value and "}" not in value:
            return value
        try:
            return value.format(**os.environ)
        except KeyError as exc:
            raise ValueError(
                f"Option '{key}' of resource '{self.name}' references the "
                f"environment variable {exc.args[0]!r}, which is not set."
            ) from exc


# ---------------------------------------------------------------------------
# Main configuration model
# ---------------------------------------------------------------------------

@dataclass
class MorphConfig:
    """
    Validated, typed representation of a Morph-KGC configuration.

    Construct via one of the loader functions in ``config.loaders``:

        config = load_from_file("config.ini")
        config = load_from_string("[CONFIGURATION]\\noutput_format=N-QUADS")
        config = load_from_dict({"output_format": "N-QUADS", ...})
        config = load_from_cli()          # reads sys.argv

    Attribute names intentionally mirror the INI option names so that
    ``load_from_dict`` can be implemented with a simple ``**kwargs``.
    """

    # -- Data sources (required) ---------------------------------------------
    data_sources: dict[str, DataSourceConfig] = field(default_factory=dict)

    # -- Accessed resources ([RESOURCE:<name>] sections) ----------------------
    resources: dict[str, ResourceConfig] = field(default_factory=dict)

    # -- Output --------------------------------------------------------------
    output_file: str = DEFAULT_OUTPUT_FILE
    output_dir: str = DEFAULT_OUTPUT_DIR
    output_format: str = DEFAULT_OUTPUT_FORMAT

    # -- Data sources and serialization --------------------------------------
    safe_percent_encoding: str = DEFAULT_SAFE_PERCENT_ENCODING
    # Stored internally as list[str]; the loaders pass a comma-separated string
    # which ``__post_init__`` splits.
    literal_escaping_chars: list[str] = field(default_factory=list)
    na_values: list[str] = field(default_factory=list)

    # -- Mapping -------------------------------------------------------------
    mapping_partitioning: str = DEFAULT_MAPPING_PARTITIONING
    infer_sql_datatypes: bool = DEFAULT_INFER_SQL_DATATYPES

    # -- Execution -----------------------------------------------------------
    number_of_processes: int = DEFAULT_NUMBER_OF_PROCESSES

    # -- Functions / UDFs ----------------------------------------------------
    udfs: str = DEFAULT_UDFS
    api_token: str = DEFAULT_API_TOKEN
    state_dir: str = DEFAULT_STATE_DIR

    # -- Logging -------------------------------------------------------------
    logging_level: str = DEFAULT_LOGGING_LEVEL
    logging_file: str = DEFAULT_LOGGING_FILE

    # -----------------------------------------------------------------------

    def __post_init__(self) -> None:
        self._normalize_and_validate()

    # -----------------------------------------------------------------------
    # Normalization + validation
    # -----------------------------------------------------------------------

    def _normalize_and_validate(self) -> None:
        self._normalize_strings()
        self._validate_output_format()
        self._validate_logging_level()
        self._validate_mapping_partitioning()
        self._validate_number_of_processes()
        self._validate_paths()
        self._coerce_list_fields()
        self._setup_logging()
        self._log_config()

    def _normalize_strings(self) -> None:
        self.output_format = self.output_format.strip().upper()
        self.logging_level = self.logging_level.strip().upper()
        self.mapping_partitioning = self.mapping_partitioning.strip().upper()

    def _validate_output_format(self) -> None:
        if self.output_format not in VALID_OUTPUT_FORMATS:
            raise ValueError(
                f"'output_format' value '{self.output_format}' is not valid. "
                f"Must be one of: {sorted(VALID_OUTPUT_FORMATS)}."
            )

    def _validate_logging_level(self) -> None:
        if self.logging_level not in VALID_LOGGING_LEVELS:
            raise ValueError(
                f"'logging_level' value '{self.logging_level}' is not valid. "
                f"Must be one of: {sorted(VALID_LOGGING_LEVELS)}."
            )

    def _validate_mapping_partitioning(self) -> None:
        if self.mapping_partitioning not in VALID_PARTITIONING:
            raise ValueError(
                f"'mapping_partitioning' value '{self.mapping_partitioning}' is not valid. "
                f"Must be one of: {sorted(VALID_PARTITIONING)}."
            )

    def _validate_number_of_processes(self) -> None:
        if self.number_of_processes < 1:
            raise ValueError(
                f"'number_of_processes' must be >= 1, got {self.number_of_processes}."
            )

    def _validate_paths(self) -> None:
        _create_dirs_in_path(self.logging_file)

    def _coerce_list_fields(self) -> None:
        # na_values: split on comma; preserve empty string as a valid NA value
        if isinstance(self.na_values, str):
            self.na_values = list(set(self.na_values.split(",")))
        elif not self.na_values:
            self.na_values = list(set(DEFAULT_NA_VALUES.split(",")))

        # literal_escaping_chars: same splitting convention
        if isinstance(self.literal_escaping_chars, str):
            raw = self.literal_escaping_chars
            self.literal_escaping_chars = (
                raw.split(",") if raw else DEFAULT_LITERAL_ESCAPING_CHARS.split(",")
            )
        elif not self.literal_escaping_chars:
            self.literal_escaping_chars = DEFAULT_LITERAL_ESCAPING_CHARS.split(",")

    def _setup_logging(self) -> None:
        _configure_logger(self.logging_level, self.logging_file)

    def _log_config(self) -> None:
        LOGGER.debug(
            "CONFIGURATION: output_format=%s, partitioning=%s, processes=%d, "
            "output_file=%s, output_dir=%s",
            self.output_format,
            self.mapping_partitioning,
            self.number_of_processes,
            self.output_file,
            self.output_dir,
        )
        for name, ds in self.data_sources.items():
            LOGGER.debug("DATA SOURCE '%s': mappings=%s", name, ds.mappings)
        for name, resource in self.resources.items():
            LOGGER.debug(
                "RESOURCE '%s': resource_type=%s, url=%s",
                name, resource.resource_type, resource.url,
            )

    # -----------------------------------------------------------------------
    # Convenience predicates
    # -----------------------------------------------------------------------

    def is_multiprocessing_enabled(self) -> bool:
        return self.number_of_processes > 1

    def is_no_partitioning(self) -> bool:
        return self.mapping_partitioning in {"NO", "FALSE", "OFF", "0"}

    def is_partial_aggregations_partitioning(self) -> bool:
        return self.mapping_partitioning == "PARTIAL-AGGREGATIONS"

    def is_maximal_partitioning(self) -> bool:
        return self.mapping_partitioning == "MAXIMAL"

    # -----------------------------------------------------------------------
    # Output path helper
    # -----------------------------------------------------------------------

    def get_output_file_path(self, mapping_group: Optional[str] = None) -> str:
        """
        Returns the resolved output file path for a given mapping partition
        group name.
        """
        extension = OUTPUT_FORMAT_FILE_EXTENSION[self.output_format]

        if self.output_dir:
            file_name = mapping_group or self.output_file or DEFAULT_OUTPUT_FILE
            return Path(self.output_dir, file_name).with_suffix(extension).as_posix()

        file_name = self.output_file or DEFAULT_OUTPUT_FILE
        return Path(file_name).with_suffix(extension).as_posix()

    # -----------------------------------------------------------------------
    # Data source accessors
    # -----------------------------------------------------------------------

    def get_data_sources_sections(self) -> list[str]:
        return list(self.data_sources.keys())

    def get_mappings_files(self, source_name: str) -> list[str]:
        return self.data_sources[source_name].mappings

    def get_db_url(self, source_name: str) -> str:
        return self.data_sources[source_name].get_db_url_with_env()

    def has_db_url(self, source_name: str) -> bool:
        return self.data_sources[source_name].has_db_url()

    def get_file_path(self, source_name: str) -> str:
        return self.data_sources[source_name].file_path

    def has_file_path(self, source_name: str) -> bool:
        return self.data_sources[source_name].has_file_path()

    def get_connect_args(self, source_name: str) -> str:
        return self.data_sources[source_name].connect_args

    def has_connect_args(self, source_name: str) -> bool:
        return self.data_sources[source_name].has_connect_args()

    # -----------------------------------------------------------------------
    # Resource accessors
    # -----------------------------------------------------------------------

    def get_resources_sections(self) -> list[str]:
        return list(self.resources.keys())

    def has_resource(self, resource_name: str) -> bool:
        return resource_name in self.resources

    def get_resource(self, resource_name: str) -> ResourceConfig:
        """Return the resource declared as ``[RESOURCE:<resource_name>]``."""
        return self.resources[resource_name]

    def get_resources_of_type(self, resource_type: str) -> dict[str, ResourceConfig]:
        """Return every declared resource whose ``resource_type`` matches."""
        resource_type = resource_type.strip().upper()
        return {
            name: resource
            for name, resource in self.resources.items()
            if resource.resource_type == resource_type
        }

    def find_resource(self, reference: str) -> Optional[ResourceConfig]:
        """
        Resolve a resource by section name or, failing that, by the IRI that
        identifies it (its ``iri`` option, falling back to its ``url``).

        Resolving by IRI lets a mapping name the accessed vocabulary or
        endpoint directly, while where it is fetched from and the credentials
        it needs stay in the configuration file.
        """
        resource = self.resources.get(reference)
        if resource is not None:
            return resource

        for resource in self.resources.values():
            if reference in resource.identifiers():
                return resource

        return None


# ---------------------------------------------------------------------------
# Module-private helpers
# ---------------------------------------------------------------------------

def _create_dirs_in_path(path: str) -> None:
    """Creates intermediate directories for a file path if they do not exist."""
    if not path:
        return
    parent = Path(path).parent
    if str(parent) == ".":
        return
    try:
        parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise


def _configure_logger(level: str, log_file: str) -> None:
    """Configures the morph_kgc root logger."""
    logger = logging.getLogger("morph_kgc")
    logger.setLevel(getattr(logging, level, logging.INFO))

    if not logger.handlers:
        handler: logging.Handler
        if log_file:
            _create_dirs_in_path(log_file)
            handler = logging.FileHandler(log_file)
        else:
            handler = logging.StreamHandler()

        formatter = logging.Formatter(
            fmt="%(asctime)s - %(name)s - %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
