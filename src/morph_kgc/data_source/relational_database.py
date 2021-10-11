__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import logging
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


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


def _replace_query_enclosing_characters(sql_query, db_dialect):
    db_dialect = db_dialect.upper()
    dialect_sql_query = ''

    if db_dialect in ['MYSQL', 'MARIADB']:
        dialect_sql_query = sql_query   # the query already uses backticks as enclosed characters
    elif db_dialect == 'MSSQL':
        # replace backticks with square brackets
        square_brackets = ['[', ']']
        num_enclosing_char = 0
        for char in sql_query:
            if char == '`':
                dialect_sql_query = dialect_sql_query + square_brackets[num_enclosing_char % 2]
                num_enclosing_char += 1
            else:
                dialect_sql_query = dialect_sql_query + char
    else:
        # replace backticks with double quotes
        dialect_sql_query = sql_query.replace('`', '"')

    return dialect_sql_query


def relational_db_connection(config, source_name):
    db_connection = create_engine(config.get_database_url(source_name), poolclass=NullPool)
    db_dialect = db_connection.dialect.name.upper()

    return db_connection, db_dialect


def get_column_datatype(config, source_name, table_name, column_name):
    db_connection, db_dialect = relational_db_connection(config, source_name)

    sql_query = "SELECT `data_type` FROM `information_schema`.`columns` WHERE `table_name`='" + table_name + \
                "' AND `column_name`='" + column_name + "'"

    sql_query = _replace_query_enclosing_characters(sql_query, db_dialect)

    try:
        query_results_df = pd.read_sql(sql_query, con=db_connection)
    except:
        raise Exception('Query [' + sql_query + '] has failed to execute.')

    data_type = ''
    if 'data_type' in query_results_df.columns and len(query_results_df) == 1:
        data_type = query_results_df['data_type'][0]
    elif 'DATA_TYPE' in query_results_df.columns and len(query_results_df) == 1:
        data_type = query_results_df['DATA_TYPE'][0]

    if data_type.upper() in SQL_RDF_DATATYPE:
        return SQL_RDF_DATATYPE[data_type.upper()]
    else:
        return 'http://www.w3.org/2001/XMLSchema#string'


def build_sql_join_query(mapping_rule, parent_triples_map_rule, references, parent_references):
    if pd.notna(mapping_rule['query']) or pd.notna(parent_triples_map_rule['query']):
        # This is because additional work is needed to use proper aliases within the rml:query provided
        raise Exception(
            'Pushing down SQL joins is not supported for mapping rules using rml:query instead of rr:tablename')

    child_query = build_sql_subquery(mapping_rule, references, 'child_')
    parent_query = build_sql_subquery(parent_triples_map_rule, parent_references, 'parent_')

    query = 'SELECT * FROM (' + child_query + ') AS `child`, (' + parent_query + ') AS `parent` WHERE '
    for key, join_condition in eval(mapping_rule['join_conditions']).items():
        query = query + '`child`.`child_' + join_condition['child_value'] + \
                '`=`parent`.`parent_' + join_condition['parent_value'] + '` AND '
    query = query[:-5]

    logging.debug('SQL query for mapping rule `' + str(mapping_rule['id']) + '`: [' + query + ']')

    return query


def build_sql_subquery(mapping_rule, references, alias):
    if pd.notna(mapping_rule['query']):
        query = mapping_rule['query']
    elif len(references) > 0:
        query = 'SELECT '
        if len(references) > 0:
            for reference in references:
                query = query + '`' + reference + '` AS `' + alias + reference + '`, '
            query = query[:-2] + ' FROM `' + mapping_rule['tablename'] + '` WHERE '
            for reference in references:
                query = query + '`' + reference + '` IS NOT NULL AND '
            query = query[:-5]
    else:
        query = None

    return query


def build_sql_query(config, mapping_rule, references):
    """
    Build a query for MYSQL using backticks '`' as enclosing character. This character will later be replaced with the
    one corresponding one to the dialect that applies.
    """

    if pd.notna(mapping_rule['query']):
        query = mapping_rule['query']
    elif len(references) > 0:
        query = 'SELECT '
        if config.push_down_sql_distincts():
            query = query + 'DISTINCT '
        for reference in references:
            query = query + '`' + reference + '`, '
        query = query[:-2] + ' FROM `' + mapping_rule['tablename'] + '` WHERE '
        for reference in references:
            query = query + '`' + reference + '` IS NOT NULL AND '
        query = query[:-5]
    else:
        query = None

    if query is not None:
        logging.debug('SQL query for mapping rule `' + str(mapping_rule['id']) + '`: [' + query + ']')

    return query


def get_sql_data(config, mapping_rule, references, parent_triples_map_rule=None, parent_references=None):
    db_connection, db_dialect = relational_db_connection(config, mapping_rule['source_name'])

    if parent_triples_map_rule is not None:
        sql_query = build_sql_join_query(mapping_rule, parent_triples_map_rule, references, parent_references)
    else:
        sql_query = build_sql_query(config, mapping_rule, references)

    sql_query = _replace_query_enclosing_characters(sql_query, db_dialect)

    result_chunks = pd.read_sql(sql_query,
                                con=db_connection,
                                chunksize=config.get_chunksize(),
                                coerce_float=False)

    return result_chunks


def setup_oracle(config):
    if config.is_oracle_client_config_dir_provided() or config.is_oracle_client_lib_dir_provided():
        import cx_Oracle

    if config.is_oracle_client_config_dir_provided() and config. is_oracle_client_lib_dir_provided():
        cx_Oracle.init_oracle_client(lib_dir=config.get_oracle_client_lib_dir(),
                                     config_dir=config.get_oracle_client_config_dir())
    elif config.is_oracle_client_config_dir_provided():
        cx_Oracle.init_oracle_client(config_dir=config.get_oracle_client_config_dir())
    elif config.is_oracle_client_lib_dir_provided():
        cx_Oracle.init_oracle_client(lib_dir=config.get_oracle_client_lib_dir())
