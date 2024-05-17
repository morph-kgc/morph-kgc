__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


def get_pg_data(config, rml_rule, references):
    db_url = config.get_db_url(rml_rule['source_name'])
    if '://' in db_url:
        import neo4j

        # neo4j://host:port@username:password/db_name
        db = db_url.split('/')[-1]
        db_url = '/'.join(db_url.split('/')[:-1])
        db_url, user_password = db_url.split('@')
        user, password = user_password.split(':')

        driver = neo4j.GraphDatabase.driver(db_url, auth=(user, password))
        return driver.execute_query(rml_rule['logical_source_value'], database=db, result_transformer=neo4j.Result.to_df)
    else:
        import kuzu

        db = kuzu.Database(config.get_db_url(rml_rule['source_name']))
        conn = kuzu.Connection(db)

        return conn.execute(rml_rule['logical_source_value']).get_as_df()
