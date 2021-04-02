""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import pandas as pd


def get_table_data(config, mapping_rule, references):
    return pd.read_table(mapping_rule['data_source'],
                         delimiter=',',
                         usecols=references,
                         chunksize=config.get_chunksize(),
                         engine='c',
                         keep_default_na=False,
                         na_values=config.get_na_values(),
                         na_filter=config.has_na_values())
