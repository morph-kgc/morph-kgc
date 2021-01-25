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