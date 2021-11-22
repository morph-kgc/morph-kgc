__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import json
import pandas as pd

from lxml import etree
from jsonpath import JSONPath

from ..constants import *


def get_file_data(config, mapping_rule, references):
    file_source_type = mapping_rule['source_type']

    if file_source_type in [CSV, TSV]:
        return _read_csv(config, mapping_rule, references, file_source_type)
    elif file_source_type in EXCEL:
        return _read_excel(config, mapping_rule, references)
    elif file_source_type == PARQUET:
        return _read_parquet(mapping_rule, references)
    elif file_source_type in FEATHER:
        return _read_feather(mapping_rule, references)
    elif file_source_type == ORC:
        return _read_orc(mapping_rule, references)
    elif file_source_type == STATA:
        return _read_stata(config, mapping_rule, references)
    elif file_source_type in SAS:
        return _read_sas(config, mapping_rule, references)
    elif file_source_type == SPSS:
        return _read_spss(mapping_rule, references)
    elif file_source_type == JSON:
        return _read_json(mapping_rule, references)
    elif file_source_type == XML:
        return _read_xml(mapping_rule, references)
    else:
        raise ValueError('Found an invalid source type. Found value `' + file_source_type + '`.')


def _read_csv(config, mapping_rule, references, file_source_type):
    delimiter = ',' if file_source_type == 'CSV' else '\t'

    return pd.read_table(str(mapping_rule['data_source']),
                         delimiter=delimiter,
                         index_col=False,
                         encoding='utf-8',
                         encoding_errors='strict',
                         usecols=references,
                         chunksize=config.get_chunksize(),
                         engine='c',
                         dtype=str,
                         keep_default_na=False,
                         na_values=config.get_na_values(),
                         na_filter=False)


def _read_parquet(mapping_rule, references):
    parquet_df = pd.read_parquet(str(mapping_rule['data_source']),
                                 engine='pyarrow',
                                 columns=references)

    return [parquet_df]     # return as list because it does not support chunksize


def _read_feather(mapping_rule, references):
    feather_df = pd.read_feather(str(mapping_rule['data_source']),
                                 use_threads=True,
                                 columns=references)

    return [feather_df]


def _read_orc(mapping_rule, references):
    orc_df = pd.read_orc(str(mapping_rule['data_source']),
                         encoding='utf-8',
                         columns=references)

    return [orc_df]


def _read_stata(config, mapping_rule, references):
    return pd.read_stata(str(mapping_rule['data_source']),
                         columns=references,
                         chunksize=config.get_chunksize(),
                         convert_dates=False,
                         convert_categoricals=False,
                         convert_missing=False,
                         preserve_dtypes=False,
                         order_categoricals=False)


def _read_sas(config, mapping_rule, references):
    sas_df = pd.read_sas(str(mapping_rule['data_source']),
                         encoding='utf-8',
                         chunksize=config.get_chunksize())
    sas_df = sas_df[references]

    return sas_df


def _read_spss(mapping_rule, references):
    spss_df = pd.read_spss(str(mapping_rule['data_source']),
                           usecols=references,
                           convert_categoricals=False)

    return [spss_df]


def _read_excel(config, mapping_rule, references):
    excel_df = pd.read_excel(str(mapping_rule['data_source']),
                             sheet_name=0,
                             engine='openpyxl',
                             usecols=references,
                             dtype=str,
                             keep_default_na=False,
                             na_values=config.get_na_values(),
                             na_filter=False)

    return [excel_df]


def _read_json(mapping_rule, references):
    with open(str(mapping_rule['data_source']), encoding='utf-8') as json_file:
        json_data = json.load(json_file)

    jsonpath_expression = mapping_rule['iterator'] + '.('
    for reference in references:
        jsonpath_expression += reference + ','
    jsonpath_expression = jsonpath_expression[:-1] + ')'

    jsonpath_result = JSONPath(jsonpath_expression).parse(json_data)
    json_df = pd.DataFrame.from_records(jsonpath_result)
    json_df.dropna(axis=0, how='any', inplace=True)

    # in case lists where retrieved, explode them e.g.: ['a', 'b'] -> 'a', 'b'
    for reference in references:
        json_df = json_df.explode(reference)

    return [json_df]


def _read_xml(mapping_rule, references):
    with open(str(mapping_rule['data_source']), encoding='utf-8') as xml_file:
        xml_root = etree.parse(xml_file).getroot()

    xpath_expression = mapping_rule['iterator']
    for reference in references:
        xpath_expression += '[' + reference + ']'

    xpath_result = xml_root.xpath(xpath_expression)
    xpath_result = [[e.find(reference).text for reference in references] for e in xpath_result]

    xml_df = pd.DataFrame.from_records(xpath_result, columns=references)

    return [xml_df]
