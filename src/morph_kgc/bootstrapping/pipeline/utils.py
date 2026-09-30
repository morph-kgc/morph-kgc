
IRI_SAFE = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")

SQL_TO_RDF = {
    "BINARY":   "xsd:hexBinary",
    "VARBINARY": "xsd:hexBinary",
    "BLOB":     "xsd:hexBinary",
    "HEXADECIMAL": "xsd:hexBinary",
    "HEX":      "xsd:hexBinary",
    
    "INTEGER":  "xsd:integer",
    "INT":      "xsd:integer",
    "SMALLINT": "xsd:integer",
    "BIGINT":   "xsd:integer",
    "DECIMAL":  "xsd:decimal",
    "NUMERIC":  "xsd:decimal",

    "FLOAT":    "xsd:double",
    "REAL":     "xsd:double",
    "DOUBLE":   "xsd:double",
    "DOUBLE PRECISION": "xsd:double",

    "BOOLEAN":  "xsd:boolean",
    "DATE":     "xsd:date",
    "TIME":     "xsd:time",
    "TIMESTAMP": "xsd:dateTime", 
}
def quote_string(value):
    if isinstance(value, str):
        return f'"{value}"' 
    return value
def quote_string2(value):
    return f'`{value}`'
def detect_db_type(db_uri):
    dbtype = db_uri.split(":")[0].split("+")[0].lower()

    if dbtype in ("postgresql", "postgres"):
        return "postgresql"
    if dbtype in ("mysql", "mariadb"):
        return "mysql"
    if dbtype == "sqlite":
        return "sqlite"
    return "other"

def rowid_select(table_name, db_type):
    if db_type == "sqlite":
        return f"SELECT rowid AS rowid, * FROM {table_name}"
    return f"SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS rowid, * FROM {table_name}"

def percent_encode(name):
    encoded = []
    for ch in name:
        if ch in IRI_SAFE:
            encoded.append(ch)
        else:
            for byte in ch.encode("utf-8"):
                encoded.append(f"%{byte:02X}")
    return "".join(encoded)
    
def build_subject_template(table_name, primary_keys, rowid_col=None):
    enc_table = percent_encode(table_name)

    if primary_keys:
        pk_parts = ";".join(
            f"{percent_encode(pk)}=$({pk})" for pk in primary_keys
        )
        return f"base:{enc_table}/{pk_parts}"
    if rowid_col:
        return {"value": f"{enc_table}_$({rowid_col})", "type": "blanknode"}

    return f"_:{enc_table}"


def build_table_iri(table_name):
    return f"base:{percent_encode(table_name)}~iri"

def build_literal_property_iri(table_name, column_name):
    return f"base:{percent_encode(table_name)}#{percent_encode(column_name)}"

def build_reference_property_iri(table_name, fk_columns):
    cols_part = ";".join(percent_encode(c) for c in fk_columns)
    return f"base:{percent_encode(table_name)}#ref-{cols_part}"

def sql_type_to_rdf(sql_type):
    base_type = sql_type.strip().upper().split("(")[0].strip()
    rdf_res = SQL_TO_RDF.get(base_type)
    return rdf_res