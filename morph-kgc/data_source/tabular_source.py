__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import pandas as pd


def get_table_data(config, mapping_rule, references):
    tabular_source_type = config.get_source_type(mapping_rule['data_source'])

    if tabular_source_type in ['CSV', 'TSV']:
        get_csv_data(config, mapping_rule, references, tabular_source_type)
    elif tabular_source_type == 'PARQUET':
        pass
    elif tabular_source_type == 'FEATHER':
        pass
    elif tabular_source_type == 'ORC':
        pass
    elif tabular_source_type == 'STATA':
        pass
    elif tabular_source_type == 'SAS':
        pass
    elif tabular_source_type == 'SPSS':
        pass


def get_csv_data(config, mapping_rule, references, tabular_source_type):
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
