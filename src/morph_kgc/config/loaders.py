"""
Pure functions that construct a ``MorphConfig`` from different input sources:

    load_from_file(path)       ← INI config file path
    load_from_string(text)     ← INI config as a string (library / notebook use)
    load_from_dict(mapping)    ← plain Python dict (programmatic use)
    load_from_cli()            ← reads sys.argv (CLI entrypoint)

Each function is responsible for:
  1. Parsing the raw input into sections + key/value pairs.
  2. Resolving mapping file paths (files, directories, URLs).
  3. Constructing ``DataSourceConfig`` objects for every data-source section and
     ``ResourceConfig`` objects for every ``[RESOURCE:<name>]`` section.
  4. Building and returning a validated ``MorphConfig``.

Having the loaders as standalone functions (rather than classmethods) makes
them independently testable and avoids the ``ConfigParser`` subclass pattern.
"""

__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]
__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"

import argparse
import errno
import os
from configparser import ConfigParser, ExtendedInterpolation
from pathlib import Path
from typing import Any

from .._version import __version__
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
from ..constants.misc import RESOURCE_SECTION_PREFIX
from .model import DataSourceConfig, MorphConfig, ResourceConfig

# Name of the mandatory INI section that holds engine-level options.
_CONFIGURATION_SECTION = "CONFIGURATION"

# Keys that belong to the CONFIGURATION section (not data sources).
_CONFIGURATION_KEYS = {
    "output_file",
    "output_dir",
    "output_format",
    "safe_percent_encoding",
    "literal_escaping_chars",
    "na_values",
    "mapping_partitioning",
    "infer_sql_datatypes",
    "number_of_processes",
    "udfs",
    "api_token",
    "state_dir",
    "logging_level",
    "logging_file",
}

# Keys that belong to data-source sections.
_DATA_SOURCE_KEYS = {"mappings", "db_url", "connect_args", "file_path"}

# Section-name prefix that marks an accessed resource instead of a data source,
# e.g. ``[RESOURCE:disease_vocabulary]``.
_RESOURCE_SECTION_PREFIX = RESOURCE_SECTION_PREFIX

# Resource keys that are modelled as named ``ResourceConfig`` fields. Every
# other key of the section is kept in ``ResourceConfig.options``.
_RESOURCE_KEYS = {"resource_type", "url", "username", "password"}


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def load_config(source: MorphConfig | str | os.PathLike | dict[str, Any]) -> MorphConfig:
    """
    Accept a MorphConfig, a file path, a raw INI string, or a dict and
    return a validated MorphConfig.
    """
    if isinstance(source, MorphConfig):
        return source

    if isinstance(source, dict):
        return load_from_dict(source)

    if isinstance(source, os.PathLike):
        return load_from_file(source)

    if isinstance(source, str):
        path = Path(source).expanduser()

        if path.is_file():
            return load_from_file(path)

        return load_from_string(source)

    raise TypeError(
        "Unsupported config source type. "
        "Expected file path, raw INI string, or dict; "
        f"got {type(source)!r}."
    )

def load_from_file(path: str | os.PathLike) -> MorphConfig:
    """Load configuration from an INI file on disk."""
    parser = _make_parser()
    parser.read(str(path))
    return _build_config(parser)


def load_from_string(text: str) -> MorphConfig:
    """Load configuration from a raw INI-format string."""
    parser = _make_parser()
    parser.read_string(text)
    return _build_config(parser)


def load_from_dict(mapping: dict[str, Any]) -> MorphConfig:
    """
    Load configuration from a plain Python dict.

    The dict may follow two shapes:

    Flat (single data source, all keys at top level)::

        {
            "output_format": "N-QUADS",
            "mappings": "mapping.ttl",
            "db_url": "sqlite:///data.db",
        }

    Sectioned (multiple data sources, mirrors INI structure)::

        {
            "CONFIGURATION": {"output_format": "N-QUADS"},
            "MY_SOURCE":     {"mappings": "mapping.ttl", "db_url": "sqlite:///data.db"},
        }

    The loader auto-detects which shape is used by checking whether any value
    is itself a ``dict``.
    """
    if any(isinstance(v, dict) for v in mapping.values()):
        sectioned = mapping
    else:
        # Flat dict: separate CONFIGURATION keys from data-source keys.
        cfg_keys = {k: v for k, v in mapping.items() if k.lower() in _CONFIGURATION_KEYS}
        ds_keys = {k: v for k, v in mapping.items() if k.lower() in _DATA_SOURCE_KEYS}
        sectioned = {_CONFIGURATION_SECTION: cfg_keys}
        if ds_keys:
            sectioned["DEFAULT_SOURCE"] = ds_keys

    parser = _make_parser()
    for section, options in sectioned.items():
        if not parser.has_section(section):
            parser.add_section(section)
        for key, value in options.items():
            parser.set(section, str(key), str(value))

    return _build_config(parser)


def load_from_cli() -> MorphConfig:
    """
    Parse sys.argv and load configuration from the provided INI file.
    Intended for the ``__main__.py`` CLI entry point.
    """
    args = _parse_cli_arguments()
    return load_from_file(args.config)


# ---------------------------------------------------------------------------
# Internal: ConfigParser construction
# ---------------------------------------------------------------------------

def _make_parser() -> ConfigParser:
    """Returns a ConfigParser with ExtendedInterpolation enabled."""
    return ConfigParser(interpolation=ExtendedInterpolation())


# ---------------------------------------------------------------------------
# Internal: raw parser → MorphConfig
# ---------------------------------------------------------------------------

def _build_config(parser: ConfigParser) -> MorphConfig:
    """
    Converts a populated ConfigParser into a ``MorphConfig``.

    Steps:
      1. Extract CONFIGURATION section options (with defaults).
      2. Iterate over all other sections → build ``DataSourceConfig`` and
         ``ResourceConfig`` objects.
      3. Instantiate ``MorphConfig`` (triggers validation in ``__post_init__``).
    """
    cfg = _extract_configuration_options(parser)
    cfg["data_sources"] = _extract_data_sources(parser)
    cfg["resources"] = _extract_resources(parser)
    return MorphConfig(**cfg)


def _extract_configuration_options(parser: ConfigParser) -> dict[str, Any]:
    """Returns a dict of CONFIGURATION-section options, filled with defaults."""
    section = _CONFIGURATION_SECTION

    def get(key: str, default: Any) -> str:
        if parser.has_option(section, key):
            val = parser.get(section, key)
            return val if val != "" else str(default)
        return str(default)

    def getbool(key: str, default: bool) -> bool:
        if parser.has_option(section, key):
            val = parser.get(section, key).strip()
            if val == "":
                return default
            return parser.getboolean(section, key)
        return default

    def getint(key: str, default: int) -> int:
        if parser.has_option(section, key):
            val = parser.get(section, key).strip()
            if val == "":
                return default
            return parser.getint(section, key)
        return default

    # Options where an empty string is intentionally valid (not replaced by default).
    def get_nullable(key: str, default: str) -> str:
        if parser.has_option(section, key):
            return parser.get(section, key)
        return default

    return {
        "output_file": get_nullable("output_file", DEFAULT_OUTPUT_FILE),
        "output_dir": get("output_dir", DEFAULT_OUTPUT_DIR),
        "output_format": get("output_format", DEFAULT_OUTPUT_FORMAT),
        "safe_percent_encoding": get_nullable("safe_percent_encoding", DEFAULT_SAFE_PERCENT_ENCODING),
        "literal_escaping_chars": get_nullable("literal_escaping_chars", DEFAULT_LITERAL_ESCAPING_CHARS),
        "na_values": get_nullable("na_values", DEFAULT_NA_VALUES),
        "mapping_partitioning": get("mapping_partitioning", DEFAULT_MAPPING_PARTITIONING),
        "infer_sql_datatypes": getbool("infer_sql_datatypes", DEFAULT_INFER_SQL_DATATYPES),
        "number_of_processes": getint("number_of_processes", DEFAULT_NUMBER_OF_PROCESSES),
        "udfs": get_nullable("udfs", DEFAULT_UDFS),
        "api_token": get_nullable("api_token", DEFAULT_API_TOKEN),
        "state_dir": get_nullable("state_dir", DEFAULT_STATE_DIR),
        "logging_level": get("logging_level", DEFAULT_LOGGING_LEVEL),
        "logging_file": get_nullable("logging_file", DEFAULT_LOGGING_FILE),
    }


def _extract_data_sources(parser: ConfigParser) -> dict[str, DataSourceConfig]:
    """
    Builds a ``DataSourceConfig`` for every section that is not CONFIGURATION.
    """
    sources: dict[str, DataSourceConfig] = {}

    for section in parser.sections():
        if section.upper() == _CONFIGURATION_SECTION:
            continue
        if _is_resource_section(section):
            continue

        mappings_raw = parser.get(section, "mappings", fallback="")
        mappings = _resolve_mapping_paths(mappings_raw)

        sources[section] = DataSourceConfig(
            name=section,
            mappings=mappings,
            db_url=parser.get(section, "db_url", fallback=""),
            connect_args=parser.get(section, "connect_args", fallback=""),
            file_path=parser.get(section, "file_path", fallback=""),
        )

    return sources


def _is_resource_section(section: str) -> bool:
    """True when *section* declares an accessed resource, not a data source."""
    return section.upper().startswith(_RESOURCE_SECTION_PREFIX)


def _resource_section_name(section: str) -> str:
    """``[RESOURCE:disease_vocabulary]`` → ``disease_vocabulary``."""
    return section[len(_RESOURCE_SECTION_PREFIX):].strip()


def _extract_resources(parser: ConfigParser) -> dict[str, ResourceConfig]:
    """
    Builds a ``ResourceConfig`` for every ``[RESOURCE:<name>]`` section.

    Keys other than ``resource_type``, ``url``, ``username`` and ``password``
    are kept verbatim in ``ResourceConfig.options``, so resource types defined
    by user stateful functions need no engine changes.
    """
    resources: dict[str, ResourceConfig] = {}

    for section in parser.sections():
        if not _is_resource_section(section):
            continue

        name = _resource_section_name(section)
        if not name:
            raise ValueError(
                f"Section '[{section}]' declares a resource without a name. "
                f"Use '[{_RESOURCE_SECTION_PREFIX}<name>]'."
            )
        if name in resources:
            raise ValueError(f"Resource '{name}' is declared more than once.")

        options = {
            key: value
            for key, value in parser.items(section)
            if key not in _RESOURCE_KEYS
        }

        resources[name] = ResourceConfig(
            name=name,
            resource_type=parser.get(section, "resource_type", fallback="").strip().upper(),
            url=parser.get(section, "url", fallback="").strip(),
            username=parser.get(section, "username", fallback=""),
            password=parser.get(section, "password", fallback=""),
            options=options,
        )

    return resources


def _resolve_mapping_paths(raw: str) -> list[str]:
    """
    Expands a comma-separated mapping specification into concrete file paths.

    Each entry may be:
      - a file path  → included as-is
      - a directory  → all files directly under the directory root are included
      - a URL        → included as-is
    """
    paths: list[str] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue

        if entry.startswith("http"):
            paths.append(entry)
        elif os.path.isfile(entry):
            paths.append(entry)
        elif os.path.isdir(entry):
            for filename in os.listdir(entry):
                full = os.path.join(entry, filename)
                if os.path.isfile(full):
                    paths.append(full)
        else:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), entry)

    return paths


# ---------------------------------------------------------------------------
# Internal: CLI argument parsing
# ---------------------------------------------------------------------------

def _existing_file_path(path: str) -> str:
    if not os.path.isfile(path):
        raise argparse.ArgumentTypeError(f"'{path}' is not a valid file path.")
    return path


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate Knowledge Graphs from Heterogeneous Data Sources.",
        epilog="Copyright © 2020 Julián Arenas-Guerrero",
        allow_abbrev=False,
        prog="morph_kgc",
        argument_default=argparse.SUPPRESS,
    )
    parser.add_argument(
        "config",
        type=_existing_file_path,
        help="path to the configuration file",
    )
    parser.add_argument(
        "-v",
        "--version",
        action="version",
        version=f"Morph-KGC {__version__} | Copyright © 2020 Julián Arenas-Guerrero",
    )
    return parser.parse_args()
