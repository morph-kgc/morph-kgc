__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Stage 1 — Data Loading
======================
Fetches raw data for a mapping rule from the appropriate source and
preprocesses it into a clean, string-typed, deduplicated DataFrame.

Public API
----------
load_data(config, rule, references, python_source=None) -> pd.DataFrame
"""

import pandas as pd

from ...constants import RDB, PGDB, ORACLE, FILE_SOURCE_TYPES, IN_MEMORY_TYPES
from ...mapping.model import RMLRule
from ...utils import normalize_oracle_identifier_casing, remove_null_values_from_dataframe
from ...source import get_adapter


def _fetch(config, rule: RMLRule, references: set[str], python_source, rml_mapping) -> pd.DataFrame:
    adapter = get_adapter(rule.logical_source.format_)
    return adapter.get_data(config, rule, references, python_source, rml_mapping)


def _preprocess(data: pd.DataFrame, rule: RMLRule, references: set[str], config) -> pd.DataFrame:
    if rule.logical_source.format_ == RDB:
        db_url = config.get_db_url(rule.logical_source.config_section_name)
        if db_url.lower().startswith(ORACLE.lower()):
            data = normalize_oracle_identifier_casing(data, references)
    data = data.map(str)
    data = remove_null_values_from_dataframe(data, config, references)
    data = data.convert_dtypes(convert_boolean=False)
    data = data.astype(str)
    return data.drop_duplicates()


def load_data(
    config,
    rule: RMLRule,
    references: set[str],
    python_source=None,
    rml_mapping=None,
) -> pd.DataFrame:
    """Fetch and preprocess source data for *rule*."""
    data = _fetch(config, rule, references, python_source, rml_mapping)
    return _preprocess(data, rule, references, config)
