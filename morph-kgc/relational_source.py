""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__version__ = "0.1"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"
__status__ = 'Prototype'

import logging
import mysql.connector
import pandas as pd


def _relational_db_connection(config, source_name):
    source_type = config.get(source_name, 'source_type').lower()

    if source_type == 'mysql':
        return mysql.connector.connect(
            host=config.get(source_name, 'host'),
            port=config.get(source_name, 'port'),
            user=config.get(source_name, 'user'),
            passwd=config.get(source_name, 'password'),
            database=config.get(source_name, 'db'),
        )
    else:
        raise ValueError('source_type ' + str(source_type) + ' in configuration file is not valid.')


def execute_relational_query(query, config, source_name):
    logging.info(query)

    db_connection = _relational_db_connection(config, source_name)
    try:
        query_results_df = pd.read_sql(query, con=db_connection,
                                       coerce_float=config.getboolean('CONFIGURATION', 'coerce_float'))
    except:
        raise Exception('Query ' + query + ' has failed to execute.')
    db_connection.close()

    for col_name in list(query_results_df.columns):
        query_results_df[col_name] = query_results_df[col_name].astype(str)

    return query_results_df


def get_column_datatype(config, source_name, table_name, column_name):
    db_connection = _relational_db_connection(config, source_name)
    query = "select data_type from information_schema.columns where table_name='" + table_name + "' and column_name='" + column_name + "' ;"

    try:
        query_results_df = pd.read_sql(query, con=db_connection)
    except:
        raise Exception('Query ' + query + ' has failed to execute.')

    if 'data_type' in query_results_df.columns:
        return query_results_df['data_type'][0]
    elif 'DATA_TYPE' in query_results_df.columns:
        return query_results_df['DATA_TYPE'][0]