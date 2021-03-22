""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright (C) 2020 Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import logging
import mysql.connector
import pandas as pd


SQL_RDF_DATATYPE = {
    'INTEGER': 'http://www.w3.org/2001/XMLSchema#integer',
    'INT': 'http://www.w3.org/2001/XMLSchema#integer',
    'SMALLINT': 'http://www.w3.org/2001/XMLSchema#integer',
    'DECIMAL': 'http://www.w3.org/2001/XMLSchema#decimal',
    'NUMERIC': 'http://www.w3.org/2001/XMLSchema#decimal',
    'FLOAT': 'http://www.w3.org/2001/XMLSchema#double',
    'REAL': 'http://www.w3.org/2001/XMLSchema#double',
    'DOUBLE': 'http://www.w3.org/2001/XMLSchema#double',
    'BOOL': 'http://www.w3.org/2001/XMLSchema#boolean',
    'TINYINT': 'http://www.w3.org/2001/XMLSchema#boolean',
    'BOOLEAN': 'http://www.w3.org/2001/XMLSchema#boolean',
    'DATE': 'http://www.w3.org/2001/XMLSchema#date',
    'TIME': 'http://www.w3.org/2001/XMLSchema#time',
    'DATETIME': 'http://www.w3.org/2001/XMLSchema#',
    'TIMESTAMP': 'http://www.w3.org/2001/XMLSchema#dateTime',
    'BINARY': 'http://www.w3.org/2001/XMLSchema#hexBinary',
    'VARBINARY': 'http://www.w3.org/2001/XMLSchema#hexBinary',
    'BIT': 'http://www.w3.org/2001/XMLSchema#hexBinary',
    'YEAR': 'http://www.w3.org/2001/XMLSchema#integer'
}


def relational_db_connection(config, source_name):
    source_type = config.get(source_name, 'source_type').lower()

    if source_type == 'mysql':
        try:
            db_connection = mysql.connector.connect(
                host=config.get(source_name, 'host'),
                port=config.get(source_name, 'port'),
                user=config.get(source_name, 'user'),
                passwd=config.get(source_name, 'password'),
                database=config.get(source_name, 'db'),
            )
        except mysql.connector.Error as err:
            raise Exception("Error while connecting to DB of data source '" + source_name + "': {}".format(err))
    else:
        raise ValueError("source_type '" + str(source_type) + "' in configuration file is not valid.")

    return db_connection


def execute_relational_query(query, config, source_name):
    logging.debug("SQL query for data source '" + source_name + "': " + query)

    db_connection = relational_db_connection(config, source_name)
    try:
        query_results_df = pd.read_sql(query, con=db_connection,
                                       coerce_float=config.getboolean('CONFIGURATION', 'coerce_float'))
    except:
        raise Exception("Query '" + query + "' has failed to execute.")
    db_connection.close()

    for col_name in list(query_results_df.columns):
        query_results_df[col_name] = query_results_df[col_name].astype(str)

    return query_results_df


def get_column_datatype(config, source_name, table_name, column_name):
    db_connection = relational_db_connection(config, source_name)
    query = "select data_type from information_schema.columns where table_name='" + table_name + "' and column_name='" + column_name + "' ;"

    try:
        query_results_df = pd.read_sql(query, con=db_connection)
    except:
        raise Exception("Query '" + query + "' has failed to execute.")

    data_type = ''
    if 'data_type' in query_results_df.columns:
        data_type = query_results_df['data_type'][0]
    elif 'DATA_TYPE' in query_results_df.columns:
        data_type = query_results_df['DATA_TYPE'][0]

    if data_type.upper() in SQL_RDF_DATATYPE:
        return SQL_RDF_DATATYPE[data_type.upper()]
    else:
        return ''


def build_sql_join_query(config, mapping_rule, parent_triples_map_rule, references, parent_references):
    query = 'SELECT '
    if config.getboolean('CONFIGURATION', 'push_down_sql_distincts'):
        query = query + 'DISTINCT '

    child_query = 'SELECT '
    if len(references) > 0:
        for reference in references:
            child_query = child_query + reference + ' AS child_' + reference + ', '
        child_query = child_query[:-2] + ' FROM ' + mapping_rule['tablename'] + ' WHERE '
        for reference in references:
            child_query = child_query + reference + ' IS NOT NULL AND '
        child_query = child_query[:-5]
    else:
        child_query = None

    parent_query = 'SELECT '
    if len(parent_references) > 0:
        for reference in parent_references:
            parent_query = parent_query + reference + ' AS parent_' + reference + ', '
        parent_query = parent_query[:-2] + ' FROM ' + parent_triples_map_rule['tablename'] + ' WHERE '
        for reference in parent_references:
            parent_query = parent_query + reference + ' IS NOT NULL AND '
        parent_query = parent_query[:-5]
    else:
        parent_query = None

    query = query + '* FROM (' + child_query + ') AS child, (' + parent_query + ') AS parent WHERE '
    for key, join_condition in eval(mapping_rule['join_conditions']).items():
        query = query + 'child.child_' + join_condition['child_value'] + \
                '=parent.parent_' + join_condition['parent_value'] + ' AND '
    query = query[:-4] + ';'

    logging.debug("SQL query for mapping rule '" + str(mapping_rule['id']) + "': " + query)

    return query


def build_sql_query(config, mapping_rule, references):
    query = 'SELECT '
    if config.getboolean('CONFIGURATION', 'push_down_sql_distincts'):
        query = query + 'DISTINCT '

    if len(references) > 0:
        for reference in references:
            query = query + reference + ', '
        query = query[:-2] + ' FROM ' + mapping_rule['tablename'] + ' WHERE '
        for reference in references:
            query = query + reference + ' IS NOT NULL AND '
        query = query[:-4] + ';'
    else:
        query = None

    logging.debug("SQL query for mapping rule '" + str(mapping_rule['id']) + "': " + query)

    return query
