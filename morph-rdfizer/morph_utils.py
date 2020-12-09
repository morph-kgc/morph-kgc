import logging, re, mysql.connector


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


def get_references_in_template(template):
    template = template.replace('\{', 'zwy\u200B').replace('\}', 'ywz\u200A')

    references = re.findall('\{([^}]+)', template)
    references = [reference.replace('zwy\u200B', '\{').replace('ywz\u200A', '\}') for reference in references]

    return references