from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
In-memory / Python data adapter
==================================
Implements DataSourceAdapter for in-memory data structures passed directly
to morph-kgc (PYTHON_SOURCE, DATAFRAME, DICTIONARY, JSON_STRING).

The four IN_MEMORY_TYPES strings are registered in source/__init__.py.
"""

import json
from typing import Any

import pandas as pd
from jsonpath import JSONPath

from ..utils import normalize_hierarchical_data
from .file import strip_json_root, alias_json_root_references


def _check_if_json(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        json.loads(value)
        return True
    except ValueError:
        return False


def _read_inmemory_json(
    source_value: str,
    rml_rule,
    references: list[str],
) -> pd.DataFrame:
    # TODO: this method repeats code from _read_json() in source/file.py
    json_data = json.loads(source_value)

    stripped_references = [strip_json_root(reference) for reference in references]

    jsonpath_expression = rml_rule.logical_source.iterator + '.('
    # add top level object of the references to reduce intermediate results (THIS IS NOT STRICTLY NECESSARY)
    for reference in stripped_references:
        jsonpath_expression += reference + ','
    jsonpath_expression = jsonpath_expression[:-1] + ')'

    jsonpath_result = JSONPath(jsonpath_expression).parse(json_data)
    # normalize and remove nulls
    json_df = pd.json_normalize([
        json_object
        for json_object in normalize_hierarchical_data(jsonpath_result)
        if None not in json_object.values()
           and all(reference.split('.')[0] in json_object for reference in stripped_references)
    ])
    json_df = alias_json_root_references(json_df, references)

    # add columns with null values for those references in the mapping rule that are not present in the data file
    missing_references_in_df = list(set(references).difference(set(json_df.columns)))
    json_df[missing_references_in_df] = None
    json_df.dropna(axis=0, how='any', inplace=True)

    return json_df


class PythonDataAdapter:
    """DataSourceAdapter for in-memory Python data structures."""

    def get_data(
        self,
        config: Any,
        rml_rule: Any,
        references: set[str],
        python_source: dict | None = None,
        rml_mapping: Any | None = None,
    ) -> pd.DataFrame:
        refs        = list(references)
        source_key  = rml_rule.logical_source.value[1:-1]   # strip enclosing braces/quotes
        source_val  = (python_source or {})[source_key]

        # format could also be done checking reference formulations
        if isinstance(source_val, pd.DataFrame):
            # Sanitize string columns: strip stray quotes from string values
            df = source_val.copy()
            for col in df.select_dtypes(include=["object", "string"]).columns:
                df[col] = df[col].apply(
                    lambda x: x.replace('"', "") if isinstance(x, str) else x
                )
            return df[refs]
        elif isinstance(source_val, list):
            return pd.DataFrame(source_val, columns=refs)
        elif isinstance(source_val, tuple):
            return pd.DataFrame(list(source_val), columns=refs)
        elif isinstance(source_val, dict):
            return _read_inmemory_json(json.dumps(source_val), rml_rule, refs)
        elif _check_if_json(source_val):
            return _read_inmemory_json(source_val, rml_rule, refs)
        else:
            raise ValueError(
                f"PythonDataAdapter: unsupported in-memory data structure "
                f"{type(source_val).__name__!r} for source key {source_key!r}."
            )
