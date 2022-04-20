__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import json
import pandas as pd
import numpy as np
import elementpath
import xml.etree.ElementTree as et

from jsonpath import JSONPath
from elementpath.xpath3 import XPath3Parser

from ..constants import *
from ..utils import normalize_hierarchical_data


def get_file_data(mapping_rule, references):
    references = list(references)
    file_source_type = mapping_rule['source_type']

    if file_source_type in [CSV, TSV]:
        return _read_csv(mapping_rule, references, file_source_type)
    elif file_source_type in EXCEL:
        return _read_excel(mapping_rule, references)
    elif file_source_type in ODS:
        return _read_ods(mapping_rule, references)
    elif file_source_type == PARQUET:
        return _read_parquet(mapping_rule, references)
    elif file_source_type in FEATHER:
        return _read_feather(mapping_rule, references)
    elif file_source_type == ORC:
        return _read_orc(mapping_rule, references)
    elif file_source_type == STATA:
        return _read_stata(mapping_rule, references)
    elif file_source_type in SAS:
        return _read_sas(mapping_rule)
    elif file_source_type == SPSS:
        return _read_spss(mapping_rule, references)
    elif file_source_type == JSON:
        return _read_json(mapping_rule, references)
    elif file_source_type == XML:
        return _read_xml(mapping_rule, references)
    else:
        raise ValueError(f'Found an invalid source type. Found value `{file_source_type}`.')


def _read_csv(mapping_rule, references, file_source_type):
    delimiter = ',' if file_source_type == 'CSV' else '\t'

    return pd.read_table(str(mapping_rule['data_source']),
                         delimiter=delimiter,
                         index_col=False,
                         encoding='utf-8',
                         encoding_errors='strict',
                         usecols=references,
                         engine='c',
                         dtype=str,
                         keep_default_na=False,
                         na_filter=False)


def _read_parquet(mapping_rule, references):
    return pd.read_parquet(str(mapping_rule['data_source']), engine='pyarrow', columns=references)


def _read_feather(mapping_rule, references):
    return pd.read_feather(str(mapping_rule['data_source']), use_threads=False, columns=references)


def _read_orc(mapping_rule, references):
    return pd.read_orc(str(mapping_rule['data_source']), encoding='utf-8', columns=references)


def _read_stata(mapping_rule, references):
    return pd.read_stata(str(mapping_rule['data_source']),
                         columns=references,
                         convert_dates=False,
                         convert_categoricals=False,
                         convert_missing=False,
                         preserve_dtypes=False,
                         order_categoricals=False)


def _read_sas(mapping_rule):
    return pd.read_sas(str(mapping_rule['data_source']), encoding='utf-8')


def _read_spss(mapping_rule, references):
    return pd.read_spss(str(mapping_rule['data_source']), usecols=references, convert_categoricals=False)


def _read_excel(mapping_rule, references):
    return pd.read_excel(str(mapping_rule['data_source']),
                         sheet_name=0,
                         engine='openpyxl',
                         usecols=references,
                         dtype=str,
                         keep_default_na=False,
                         na_filter=False)


def _read_ods(mapping_rule, references):
    return pd.read_excel(str(mapping_rule['data_source']),
                         sheet_name=0,
                         engine='odf',
                         usecols=references,
                         dtype=str,
                         keep_default_na=False,
                         na_filter=False)


def _read_json(mapping_rule, references):
    with open(str(mapping_rule['data_source']), encoding='utf-8') as json_file:
        json_data = json.load(json_file)

    jsonpath_expression = mapping_rule['iterator'] + '.('
    # add top level object of the references to reduce intermediate results (THIS IS NOT STRICTLY NECESSARY)
    for reference in references:
        jsonpath_expression += reference.split('.')[0] + ','
    jsonpath_expression = jsonpath_expression[:-1] + ')'

    jsonpath_result = JSONPath(jsonpath_expression).parse(json_data)
    # normalize and remove nulls
    json_df = pd.json_normalize([json_object for json_object in normalize_hierarchical_data(jsonpath_result) if None not in json_object.values()])

    # add columns with null values for those references in the mapping rule that are not present in the data file
    missing_references_in_df = list(set(references).difference(set(json_df.columns)))
    json_df[missing_references_in_df] = np.nan

    return json_df


def _read_xml(mapping_rule, references):
    with open(str(mapping_rule['data_source']), encoding='utf-8') as xml_file:
        xml_root = et.parse(xml_file).getroot()

    xpath_result = elementpath.iter_select(xml_root, mapping_rule['iterator'], parser=XPath3Parser)
    xpath_result = [[[r.text for r in e.findall(reference)] for reference in references] for e in xpath_result]

    # IMPORTANT NOTES
    # XPath 3.0 is used (XPath 3.1 is in the roadmap of the elementpath library)
    # with XPath 3.1 the above could be achieved using just an XPath expression by including the references in it
    # for instance, the XPath expression: /root/[id,creator/name] obtaining for example ["2479", ["Julián", "Jhon"]]

    xml_df = pd.DataFrame.from_records(xpath_result, columns=references)

    # add columns with null values for those references in the mapping rule that are not present in the data file
    missing_references_in_df = list(set(references).difference(set(xml_df.columns)))
    xml_df[missing_references_in_df] = np.nan
    xml_df.dropna(axis=0, how='any', inplace=True)

    for reference in references:
        xml_df = xml_df.explode(reference)

    return xml_df
