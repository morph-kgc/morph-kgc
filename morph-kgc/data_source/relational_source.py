""" Morph-KGC """

__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import logging
import mysql.connector
import pandas as pd
import constants


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
    source_type = config.get(source_name, 'source_type')

    if source_type == 'MYSQL':
        try:
            db_connection = mysql.connector.connect(
                host=config.get(source_name, 'host'),
                port=config.get(source_name, 'port'),
                user=config.get(source_name, 'user'),
                passwd=config.get(source_name, 'password'),
                database=config.get(source_name, 'db'),
            )
        except mysql.connector.Error as err:
            raise Exception("Error while connecting to DB of data source `" + source_name + "`: {}".format(err))
    else:
        raise ValueError("source_type `" + str(source_type) + "` in configuration file is not valid.")

    return db_connection


def execute_relational_query(query, config, source_name):
    logging.debug("SQL query for data source `" + source_name + "`: [" + query + ']')

    db_connection = relational_db_connection(config, source_name)
    try:
        query_results_df = pd.read_sql(query, con=db_connection,
                                       coerce_float=config.getboolean(constants.CONFIG_SECTION, 'coerce_float'))
    except:
        raise Exception("Query `" + query + "` has failed to execute.")
    db_connection.close()

    for col_name in list(query_results_df.columns):
        query_results_df[col_name] = query_results_df[col_name].astype(str)

    return query_results_df


def get_column_datatype(config, source_name, table_name, column_name):
    db_connection = relational_db_connection(config, source_name)
    query = "SELECT data_type FROM information_schema.columns WHERE table_name='" + table_name + \
            "' AND column_name='" + column_name + "' ;"

    try:
        query_results_df = pd.read_sql(query, con=db_connection)
    except:
        raise Exception('Query [' + query + '] has failed to execute.')

    data_type = ''
    if 'data_type' in query_results_df.columns and len(query_results_df) == 1:
        data_type = query_results_df['data_type'][0]
    elif 'DATA_TYPE' in query_results_df.columns and len(query_results_df) == 1:
        data_type = query_results_df['DATA_TYPE'][0]

    if data_type.upper() in SQL_RDF_DATATYPE:
        return SQL_RDF_DATATYPE[data_type.upper()]
    else:
        return ''


def build_sql_join_query(config, mapping_rule, parent_triples_map_rule, references, parent_references):
    if pd.notna(mapping_rule['query']) or pd.notna(parent_triples_map_rule['query']):
        # This is because additional work is needed to use proper aliases within the rml:query provided
        raise Exception(
            'Pushing down SQL joins is not supported for mapping rules using rml:query instead of rr:tablename')

    child_query = build_sql_subquery(mapping_rule, references, 'child_')
    parent_query = build_sql_subquery(parent_triples_map_rule, parent_references, 'parent_')

    query = 'SELECT * FROM (' + child_query + ') AS child, (' + parent_query + ') AS parent WHERE '
    for key, join_condition in eval(mapping_rule['join_conditions']).items():
        query = query + 'child.child_' + join_condition['child_value'] + \
                '=parent.parent_' + join_condition['parent_value'] + ' AND '
    query = query[:-4] + ';'

    logging.debug('SQL query for mapping rule `' + str(mapping_rule['id']) + '`: [' + query + ']')

    return query


def build_sql_subquery(mapping_rule, references, alias):
    if pd.notna(mapping_rule['query']):
        query = mapping_rule['query']
    elif len(references) > 0:
        query = 'SELECT '
        if len(references) > 0:
            for reference in references:
                query = query + reference + ' AS ' + alias + reference + ', '
            query = query[:-2] + ' FROM ' + mapping_rule['tablename'] + ' WHERE '
            for reference in references:
                query = query + reference + ' IS NOT NULL AND '
            query = query[:-5]
    else:
        query = None

    return query


def build_sql_query(config, mapping_rule, references):
    if pd.notna(mapping_rule['query']):
        query = mapping_rule['query']
    elif len(references) > 0:
        query = 'SELECT '
        if config.getboolean(constants.CONFIG_SECTION, 'push_down_sql_distincts'):
            query = query + 'DISTINCT '
        for reference in references:
            query = query + reference + ', '
        query = query[:-2] + ' FROM ' + mapping_rule['tablename'] + ' WHERE '
        for reference in references:
            query = query + reference + ' IS NOT NULL AND '
        query = query[:-4] + ';'
    else:
        query = None

    if query is not None:
        logging.debug('SQL query for mapping rule `' + str(mapping_rule['id']) + '`: [' + query + ']')

    return query


def get_sql_data(config, mapping_rule, references, parent_triples_map_rule=None, parent_references=None):
    if parent_triples_map_rule is not None:
        sql_query = build_sql_join_query(config, mapping_rule, parent_triples_map_rule, references, parent_references)
    else:
        sql_query = build_sql_query(config, mapping_rule, references)
    db_connection = relational_db_connection(config, mapping_rule['source_name'])
    result_chunks = pd.read_sql(sql_query,
                                con=db_connection,
                                chunksize=config.getint(constants.CONFIG_SECTION, 'chunksize'),
                                coerce_float=config.getboolean(constants.CONFIG_SECTION, 'coerce_float'))

    return result_chunks, db_connection

