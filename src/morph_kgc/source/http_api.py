from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
HTTP API adapter
=================
Implements DataSourceAdapter for REST / HTTP API sources.

The source type string "HTTPAPI" is registered in source/__init__.py.
"""

import importlib.util
import json
import os
import sys
from string import Formatter
from typing import Any

import pandas as pd
from jsonpath import JSONPath
from ..http import fetch
from ..utils import normalize_hierarchical_data


def _load_module_from_path(module_name: str, file_path: str):
    """Dynamically load a Python module from *file_path*."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ValueError(
            f"'api_token' does not point at a Python module: '{file_path}'."
        )
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _resolve_header_value(config, raw_value: str) -> str:
    """
    Resolve the value a header is sent with.

    A value written as a ``{ENV_VAR}`` template is replaced with the
    environment, and so is one that is the bare name of an environment
    variable, so that secrets need not be written to the mapping. Otherwise the
    module given in the ``api_token`` configuration option, when there is one,
    is asked for it: the value is then the name of a token it hands out. A
    value that is neither, and one its ``get_api_token`` hands out no token
    for, is sent as it is written, which is what a constant header such as
    ``Accept`` needs.
    """
    if raw_value in os.environ:
        return os.environ[raw_value]

    try:
        placeholders = {name for _, name, _, _ in Formatter().parse(raw_value) if name}
    except ValueError:
        # Braces that are not a placeholder are part of the value.
        placeholders = set()

    if placeholders:
        missing = placeholders - os.environ.keys()
        if missing:
            raise ValueError(
                f"The HTTP API header value '{raw_value}' references the "
                f"environment variable(s) {', '.join(sorted(missing))}, which "
                f"are not set."
            )
        return raw_value.format(**os.environ)

    if config.api_token:
        module = _load_module_from_path("dynamic_api_token", config.api_token)
        token = module.get_api_token(raw_value)
        # A module hands out no token for a value that names none, so that
        # constant headers can be written alongside the tokens.
        if token:
            return token

    return raw_value


def _fetch_http_api(config, rml_rule, references: set[str], rml_mapping) -> pd.DataFrame:
    source_name = rml_rule.logical_source.value

    # Find the matching HTTPAPIEntry
    entry = next(
        (e for e in rml_mapping.http_api_entries if e.source == source_name),
        None,
    )
    if entry is None:
        raise KeyError(f"HTTP API entry {source_name!r} not found in mapping.")

    absolute_path = entry.absolute_path

    payload: dict = {}
    headers: dict = {}

    for header in entry.headers:
        field_name = header.field_name
        field_value_raw = header.field_value

        # Skip malformed entries if any
        if not field_name or not field_value_raw:
            continue

        field_value = _resolve_header_value(config, field_value_raw)

        if field_name.lower() in ["authorization", "accept", "keyid", "user-agent"]:
            headers[field_name] = field_value
        else:
            payload[field_name] = field_value

    response = fetch(
        absolute_path,
        params      = payload,
        headers     = headers,
        description = "HTTP API",
    )

    try:
        json_data = json.loads(response.body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            f"The HTTP API '{absolute_path}' did not answer with JSON: {exc}."
        ) from exc

    jsonpath_expression = rml_rule.logical_source.iterator + '.('
    # add top level object of the references to reduce intermediate results (THIS IS NOT STRICTLY NECESSARY)
    for reference in references:
        jsonpath_expression += reference.split('.')[0] + ','
    jsonpath_expression = jsonpath_expression[:-1] + ')'

    jsonpath_result = JSONPath(jsonpath_expression).parse(json_data)
    # normalize and remove nulls
    json_df = pd.json_normalize([
        json_object
        for json_object in normalize_hierarchical_data(jsonpath_result)
        if None not in json_object.values()
           and all(reference.split('.')[0] in json_object for reference in references)
    ])

    # add columns with null values for those references in the mapping rule that are not present in the data file
    missing_references_in_df = list(set(references).difference(set(json_df.columns)))
    json_df[missing_references_in_df] = None
    json_df.dropna(axis=0, how='any', subset=[r for r in references if r in json_df.columns], inplace=True)

    return json_df

class HttpApiAdapter:
    """DataSourceAdapter for HTTP API sources."""

    def get_data(
        self,
        config: Any,
        rml_rule: Any,
        references: set[str],
        python_source: dict | None = None,
        rml_mapping: Any | None = None,
    ) -> pd.DataFrame:
        return _fetch_http_api(config, rml_rule, references, rml_mapping)
