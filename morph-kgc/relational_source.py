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

ENGINE_DELIMITERS = {
    'mysql': '`',
    'postgresql': '"',
    'oracle': '"',
    'sqlserver': '"'
}


def relational_db_connection(config, source_name):
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


def _is_delimited(value):
    if len(value) > 2:
        if value[0] == '"' and value[len(value) - 1] == '"':
            return True
    return False


def get_valid_query_delimited_name(name, source_type):
    if _is_delimited(name):
        return ENGINE_DELIMITERS[source_type] + name[1:-1] + ENGINE_DELIMITERS[source_type]
    else:
        return name


def get_valid_delimited_name(name):
    if _is_delimited(name):
        return name[1:-1]
    else:
        return name
