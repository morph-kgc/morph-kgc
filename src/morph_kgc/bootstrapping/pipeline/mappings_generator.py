
from morph_kgc.bootstrapping.pipeline.utils import quote_string2
import yaml
from .utils import (
    build_literal_property_iri,
    build_reference_property_iri,
    build_subject_template,
    build_table_iri,
    sql_type_to_rdf,
    detect_db_type,
    rowid_select,
    quote_string,
)

def generate_yarrrml(tables, base_iri,db_uri):
    db_type = detect_db_type(db_uri) 
    yarrrml = {
        "prefixes": {
            "base": base_iri,
            "xsd": "http://www.w3.org/2001/XMLSchema#"
        },
        "mappings": {},
    }

    for table in tables:
        yarrrml["mappings"].update(build_table_mappings(table, db_type))
    return yarrrml
    
def build_table_mappings(table, db_type):
    table_name = table["name"]
    columns = table["columns"]
    pks = table["primary_keys"]
    fks = table["foreign_keys"]

    if pks:
        subject = build_subject_template(table_name, pks)
        if db_type == "mysql":
            base_query = f"SELECT * FROM {quote_string2(table_name)}"
        else:
            base_query = f"SELECT * FROM {quote_string(table_name)}"
    else:
        subject = build_subject_template(table_name, pks, rowid_col="rowid")
        if db_type == "mysql":
            base_query = rowid_select(quote_string2(table_name), db_type)
        else:
            base_query = rowid_select(quote_string(table_name), db_type)

    po = [["a", build_table_iri(table_name)]]

    for col in columns:
        col_name = col["name"]
        predicate = build_literal_property_iri(table_name, col_name)
        rdf_type = sql_type_to_rdf(col["type"])
        
        if rdf_type:
            po.append({"p": predicate, "o": [{"value": f"$({col_name})", "datatype": rdf_type}]})
        else:
            po.append([predicate, f"$({col_name})"])

    for fk in fks:
        ref_table = fk["referred_table"]
        fk_cols = fk["constrained_columns"]
        ref_cols = fk["referred_columns"]
        predicate = build_reference_property_iri(table_name, fk_cols)
        
        conditions = [
            {"function": "equal", "parameters": [["str1", f"$({lc})"], ["str2", f"$({rc})"]]}
            for lc, rc in zip(fk_cols, ref_cols)
        ]
        condition = conditions[0] if len(conditions) == 1 else conditions
        po.append({"p": predicate, "o": [{"mapping": f"{ref_table}_map", "condition": condition}]})

    return {
        f"{table_name}_map": {
            "sources": [{"query": base_query}], 
            "s": subject, 
            "po": po
        }
    }


def save_yarrrml(yarrrml_dict, output_path):

    class AuxDumper(yaml.SafeDumper):
        pass

    AuxDumper.add_representer(
        dict,
        lambda dumper, data: dumper.represent_mapping(
            "tag:yaml.org,2002:map", data, flow_style=False
        ),
    )

    yaml_str = yaml.dump(
        yarrrml_dict, Dumper=AuxDumper, sort_keys=False, default_flow_style=None
    )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(yaml_str)

    return yaml_str