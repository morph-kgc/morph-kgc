__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import logging
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from ..constants import MYSQL, MARIADB, MSSQL

# PostgresSQL datatypes: https://www.postgresql.org/docs/14/datatype.html
SQL_RDF_DATATYPE = {
    'INTEGER': 'http://www.w3.org/2001/XMLSchema#integer',
    'INT': 'http://www.w3.org/2001/XMLSchema#integer',
    'SMALLINT': 'http://www.w3.org/2001/XMLSchema#integer',
    'INT8': 'http://www.w3.org/2001/XMLSchema#integer',
    'INT4': 'http://www.w3.org/2001/XMLSchema#integer',
    'BIGINT': 'http://www.w3.org/2001/XMLSchema#integer',
    'BIGSERIAL': 'http://www.w3.org/2001/XMLSchema#integer',
    'SMALLSERIAL': 'http://www.w3.org/2001/XMLSchema#integer',
    'INT2': 'http://www.w3.org/2001/XMLSchema#integer',
    'SERIAL2': 'http://www.w3.org/2001/XMLSchema#integer',
    'SERIAL4': 'http://www.w3.org/2001/XMLSchema#integer',
    'SERIAL8': 'http://www.w3.org/2001/XMLSchema#integer',
    'DECIMAL': 'http://www.w3.org/2001/XMLSchema#decimal',
    'NUMERIC': 'http://www.w3.org/2001/XMLSchema#decimal',
    'FLOAT': 'http://www.w3.org/2001/XMLSchema#double',
    'FLOAT8': 'http://www.w3.org/2001/XMLSchema#double',
    'REAL': 'http://www.w3.org/2001/XMLSchema#double',
    'DOUBLE': 'http://www.w3.org/2001/XMLSchema#double',
    'DOUBLE PRECISION': 'http://www.w3.org/2001/XMLSchema#double',
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
    'BYTEA': 'http://www.w3.org/2001/XMLSchema#hexBinary',
    'BIT VARYING': 'http://www.w3.org/2001/XMLSchema#hexBinary',
    'VARBIT': 'http://www.w3.org/2001/XMLSchema#hexBinary',
    'YEAR': 'http://www.w3.org/2001/XMLSchema#integer'
}


def _replace_query_enclosing_characters(sql_query, db_dialect):
    dialect_sql_query = ''

    if db_dialect in [MYSQL, MARIADB]:
        dialect_sql_query = sql_query   # the query already uses backticks as enclosed characters
    elif db_dialect == MSSQL:
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


def _relational_db_connection(config, source_name):
    db_connection = create_engine(config.get_database_url(source_name), poolclass=NullPool)
    db_dialect = db_connection.dialect.name.upper()

    return db_connection, db_dialect


def get_column_datatype(config, source_name, table_name, column_name):
    db_connection, db_dialect = _relational_db_connection(config, source_name)

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


def _build_sql_query(config, mapping_rule, references):
    """
    Build a query for MYSQL using backticks '`' as enclosing character. This character will later be replaced with the
    one corresponding one to the dialect that applies.
    """

    if pd.notna(mapping_rule['query']):
        query = mapping_rule['query']
    elif len(references) > 0:
        query = 'SELECT '
        # query = query + 'DISTINCT ' # TODO: is this more efficient?
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


def get_sql_data(config, mapping_rule, references):
    db_connection, db_dialect = _relational_db_connection(config, mapping_rule['source_name'])

    sql_query = _build_sql_query(config, mapping_rule, references)

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
