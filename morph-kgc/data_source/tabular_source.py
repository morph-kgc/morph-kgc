""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import pandas as pd
import constants


def get_table_data(config, mapping_rule, references):
    return pd.read_table(mapping_rule['data_source'],
                         delimiter=',',
                         usecols=references,
                         engine='c',
                         chunksize=config.getint(constants.CONFIG_SECTION, 'chunksize'))
