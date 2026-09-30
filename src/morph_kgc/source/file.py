from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
File-based data source adapter
================================
Implements DataSourceAdapter for all file formats:
CSV, TSV, Excel, ODS, Parquet, GeoParquet, Shapefile, Feather, ORC,
Stata, SAS, SPSS, JSON/GeoJSON/JSONPath, XML/XPath.

The adapter is registered for every string in FILE_SOURCE_TYPES by
source/__init__.py.
"""

from typing import Any

import duckdb
import json
import pandas as pd
import urllib.request
import xml.etree.ElementTree as et
from io import BytesIO

from jsonpath import JSONPath
from elementpath.xpath3 import XPath3Parser
from pathlib import Path
import elementpath

from ..constants.sources import (
    CSV, TSV, EXCEL, ODS, PARQUET, GEOPARQUET, SHP, FEATHER,
    ORC, STATA, SAS, SPSS, JSON, XML,
)
from ..constants.rml    import RML_QUERY
from ..utils            import normalize_hierarchical_data


# ── Internal helpers ──────────────────────────────────────────────────────────

def _source_uri(rml_rule) -> str:
    """Return the resolved file path or URL for the logical source."""
    return rml_rule.logical_source.value


def _read_tabular_view(rml_rule) -> pd.DataFrame:
    """Execute an arbitrary SQL-over-file query via DuckDB."""
    return duckdb.query(rml_rule.logical_source.value).df()


def _read_csv(rml_rule, references: list[str], file_source_type: str) -> pd.DataFrame:
    sep = "\t" if file_source_type == TSV else ","
    source = _source_uri(rml_rule)

    try:
        return pd.read_csv(
            source, usecols=references, sep=sep, dtype=str,
            encoding="utf-8", encoding_errors="strict",
            index_col=False, engine="c",
            keep_default_na=False, na_filter=False,
        )
    except Exception:
        # Declared separator didn't match the actual file (issue #81) —
        # fall back to delimiter auto-detection via the Python engine.
        return pd.read_csv(
            source, usecols=references, sep=None, dtype=str,
            encoding="utf-8", encoding_errors="strict",
            index_col=False, engine="python",
            keep_default_na=False, na_filter=False,
        )


def _read_parquet(rml_rule, references: list[str]) -> pd.DataFrame:
    return pd.read_parquet(_source_uri(rml_rule), columns=references)


def _read_geoparquet(rml_rule, references: list[str]) -> pd.DataFrame:
    import geopandas as gpd
    gdf = gpd.read_parquet(_source_uri(rml_rule))
    # geometry column → WKT string so it can be handled as a plain reference
    if "geometry" in gdf.columns:
        gdf["geometry"] = gdf["geometry"].apply(lambda g: g.wkt if g else None)
    return pd.DataFrame(gdf)[references]


def _read_shapefile(rml_rule, references: list[str]) -> pd.DataFrame:
    import geopandas as gpd
    gdf = gpd.read_file(_source_uri(rml_rule))
    if "geometry" in gdf.columns:
        gdf["geometry"] = gdf["geometry"].apply(lambda g: g.wkt if g else None)
    return pd.DataFrame(gdf)[references]


def _read_feather(rml_rule, references: list[str]) -> pd.DataFrame:
    return pd.read_feather(_source_uri(rml_rule), columns=references)


def _read_orc(rml_rule, references: list[str]) -> pd.DataFrame:
    return pd.read_orc(_source_uri(rml_rule), columns=references)


def _read_stata(rml_rule, references: list[str]) -> pd.DataFrame:
    return pd.read_stata(_source_uri(rml_rule), columns=references, convert_categoricals=False)


def _read_sas(rml_rule) -> pd.DataFrame:
    return pd.read_sas(_source_uri(rml_rule))


def _read_spss(rml_rule, references: list[str]) -> pd.DataFrame:
    return pd.read_spss(_source_uri(rml_rule), usecols=references)


def _read_excel(rml_rule, references: list[str]) -> pd.DataFrame:
    return pd.read_excel(_source_uri(rml_rule), usecols=references, dtype=str)


def _read_ods(rml_rule, references: list[str]) -> pd.DataFrame:
    return pd.read_excel(
        _source_uri(rml_rule), usecols=references, dtype=str, engine="odf"
    )


def _read_json(rml_rule, references: list[str]) -> pd.DataFrame:
    source = _source_uri(rml_rule)
    iterator = rml_rule.logical_source.iterator

    if source.startswith("http://") or source.startswith("https://"):
        with urllib.request.urlopen(source) as r:
            raw = json.loads(r.read().decode())
    else:
        with open(source, encoding="utf-8") as f:
            raw = json.load(f)

    jsonpath_expression = iterator + '.('
    # add top level object of the references to reduce intermediate results (THIS IS NOT STRICTLY NECESSARY)
    for reference in references:
        jsonpath_expression += reference.split('.')[0] + ','
    jsonpath_expression = jsonpath_expression[:-1] + ')'

    jsonpath_result = JSONPath(jsonpath_expression).parse(raw)
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


def _read_xml(rml_rule, references):
    source = _source_uri(rml_rule)
    iterator = rml_rule.logical_source.iterator

    if source.startswith("http://") or source.startswith("https://"):
        with urllib.request.urlopen(source) as xml_url:
            xml_string = xml_url.read()
        # Turn into file object for compatibility with iterparse
        with BytesIO(xml_string) as xml_file:
            return _parse_xml_file(xml_file, iterator, references)
    else:
        with Path(source).open(encoding='utf-8') as xml_file:
            return _parse_xml_file(xml_file, iterator, references)


def _parse_xml_file(xml_file, iterator, references):

    # Collect namespaces from XML document
    namespaces = {}
    for event, element in et.iterparse(xml_file, events=['end', 'start-ns']):
        if event == "start-ns":
            namespaces[element[0]] = element[1]
        elif event == "end":
            el = element
    parsed = et.ElementTree(el)
    xml_root = parsed.getroot()
    xpath_result = elementpath.iter_select(xml_root, iterator, namespaces=namespaces, parser=XPath3Parser)

    # we need to retrieve both ELEMENTS and ATTRIBUTES in the XML
    data_records = []
    for e in xpath_result:
        data_record = []
        for reference in references:
            data_value = []
            reference = reference.replace('/@', '@')  # deals with `route/stop/@id`

            if reference.startswith('@'):
                element = None
                attribute = reference
            elif '@' in reference:
                element = reference.split('@')[0]
                attribute = reference.split('@')[1]
            else:
                element = reference
                attribute = None

            if element:
                for r in e.findall(element, namespaces=namespaces):
                    if attribute:
                        data_value.append(r.get(attribute))
                    else:
                        data_value.append(r.text)
            else:
                attribute = attribute[1:]  # do not use the starting @ from the attribute
                data_value.append(e.attrib[attribute])
            data_record.append(data_value)
        data_records.append(data_record)

    # IMPORTANT NOTES
    # XPath 3.0 is used (XPath 3.1 is in the roadmap of the elementpath library)
    # with XPath 3.1 the above could be achieved using just an XPath expression by including the references in it
    # for instance, the XPath expression: /root/[id,creator/name] obtaining for example ["2479", ["Julián", "Jhon"]]

    xml_df = pd.DataFrame.from_records(data_records, columns=references)

    # add columns with null values for those references in the mapping rule that are not present in the data file
    missing_references_in_df = list(set(references).difference(set(xml_df.columns)))
    xml_df[missing_references_in_df] = None
    xml_df.dropna(axis=0, how='any', inplace=True)

    for reference in references:
        xml_df = xml_df.explode(reference)

    return xml_df


# ── Adapter ───────────────────────────────────────────────────────────────────

class FileAdapter:
    """DataSourceAdapter for file-based sources."""

    def get_data(
        self,
        config: Any,
        rml_rule: Any,
        references: set[str],
        python_source: dict | None = None,
        rml_mapping: Any | None = None,
    ) -> pd.DataFrame:
        refs = list(references)
        source_format = rml_rule.logical_source.format_

        if rml_rule.logical_source.value_type == RML_QUERY:
            return _read_tabular_view(rml_rule)
        elif source_format == GEOPARQUET:
            return _read_geoparquet(rml_rule, refs)
        elif source_format == SHP:
            return _read_shapefile(rml_rule, refs)
        elif source_format in [CSV, TSV]:
            return _read_csv(rml_rule, refs, source_format)
        elif source_format in EXCEL:
            return _read_excel(rml_rule, refs)
        elif source_format in ODS:
            return _read_ods(rml_rule, refs)
        elif source_format == PARQUET:
            return _read_parquet(rml_rule, refs)
        elif source_format in FEATHER:
            return _read_feather(rml_rule, refs)
        elif source_format == ORC:
            return _read_orc(rml_rule, refs)
        elif source_format == STATA:
            return _read_stata(rml_rule, refs)
        elif source_format in SAS:
            return _read_sas(rml_rule)
        elif source_format == SPSS:
            return _read_spss(rml_rule, refs)
        elif source_format in JSON:
            return _read_json(rml_rule, refs)
        elif source_format in XML:
            return _read_xml(rml_rule, refs)
        else:
            raise ValueError(
                f"FileAdapter: unrecognised file source type {source_format!r}."
            )
