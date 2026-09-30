from sqlalchemy import create_engine, inspect

def extract_schema(engine_uri):
    engine = create_engine(engine_uri)
    try:
        return _inspect_tables(engine)
    finally:
        # the inspector checks connections out of the engine's pool; without
        # disposing of it they stay open for the lifetime of the process
        engine.dispose()


def _inspect_tables(engine):
    inspector = inspect(engine)
    tables = []

    for table_name in inspector.get_table_names():
        # primary keys
        pk_constraint = inspector.get_pk_constraint(table_name)
        pks = pk_constraint.get("constrained_columns", [])

        # columns
        columns = []
        for col in inspector.get_columns(table_name):
            columns.append({
                "name": col["name"],
                "type": str(col["type"]),
            })

        # foreign keys
        foreign_keys = []
        for fk in inspector.get_foreign_keys(table_name):
            foreign_keys.append({
                "constrained_columns": fk["constrained_columns"],
                "referred_table": fk["referred_table"],
                "referred_columns": fk["referred_columns"],
            })
        # tables
        tables.append({
            "name": table_name,
            "columns": columns,
            "primary_keys": pks,
            "foreign_keys": foreign_keys,
        })

    return tables