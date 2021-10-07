__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import pandas as pd

from ..constants import *


def get_table_data(config, mapping_rule, references):
    tabular_source_type = mapping_rule['source_type']

    if tabular_source_type in [CSV_SOURCE_TYPE, TSV_SOURCE_TYPE]:
        return _read_csv(config, mapping_rule, references, tabular_source_type)
    elif tabular_source_type == EXCEL_SOURCE_TYPE:
        return _read_excel(config, mapping_rule, references)
    elif tabular_source_type == PARQUET_SOURCE_TYPE:
        return _read_parquet(mapping_rule, references)
    elif tabular_source_type == FEATHER_SOURCE_TYPE:
        return _read_feather(mapping_rule, references)
    elif tabular_source_type == ORC_SOURCE_TYPE:
        return _read_orc(mapping_rule, references)
    elif tabular_source_type == STATA_SOURCE_TYPE:
        return _read_stata(config, mapping_rule, references)
    elif tabular_source_type == SAS_SOURCE_TYPE:
        return _read_sas(config, mapping_rule)
    elif tabular_source_type == SPSS_SOURCE_TYPE:
        return _read_spss(mapping_rule, references)
    else:
        raise ValueError('Found an invalid source type. Found value `' + tabular_source_type + '`.')


def _read_csv(config, mapping_rule, references, tabular_source_type):
    delimiter = ',' if tabular_source_type == 'CSV' else '\t'

    return pd.read_table(mapping_rule['data_source'],
                         delimiter=delimiter,
                         index_col=False,   # TODO: use None?
                         usecols=references,
                         chunksize=config.get_chunksize(),
                         engine='c',
                         dtype=str,
                         memory_map=False,  # TODO: use True?
                         keep_default_na=False,
                         na_values=config.get_na_values(),
                         na_filter=config.apply_na_filter())


def _read_parquet(mapping_rule, references):
    parquet_df = pd.read_parquet(mapping_rule['data_source'],
                                 engine='pyarrow',
                                 columns=references)

    return [parquet_df]     # return as list because it does not support chunksize


def _read_feather(mapping_rule, references):
    feather_df = pd.read_feather(mapping_rule['data_source'],
                                 use_threads=True,
                                 columns=references)

    return [feather_df]


def _read_orc(mapping_rule, references):
    orc_df = pd.read_orc(mapping_rule['data_source'],
                         encoding='iso-8859-1',
                         columns=references)

    return [orc_df]


def _read_stata(config, mapping_rule, references):
    return pd.read_stata(mapping_rule['data_source'],
                         columns=references,
                         chunksize=config.get_chunksize(),
                         convert_dates=False,
                         convert_categoricals=False,
                         convert_missing=False,
                         preserve_dtypes=False,
                         order_categoricals=False)


def _read_sas(config, mapping_rule):
    return pd.read_sas(mapping_rule['data_source'],
                       encoding='iso-8859-1',
                       chunksize=config.get_chunksize())


def _read_spss(mapping_rule, references):
    spss_df = pd.read_spss(mapping_rule['data_source'],
                           usecols=references,
                           convert_categoricals=False)

    return [spss_df]


def _read_excel(config, mapping_rule, references):
    excel_df = pd.read_excel(mapping_rule['data_source'],
                            sheet_name=0,
                            engine='openpyxl',
                            usecols=references,
                            dtype=str,
                            keep_default_na=False,
                            na_values=config.get_na_values(),
                            na_filter=config.apply_na_filter())

    return [excel_df]
