"""
import os
import tempfile
import morph_kgc
from rdflib.graph import Graph
from rdflib import compare
from morph_kgc.bootstrapping.pipeline.schema_extractor import extract_schema
from morph_kgc.bootstrapping.pipeline.mappings_generator import generate_yarrrml, save_yarrrml


def test_DirectGraphTC0004():

    base_dir = os.path.dirname(os.path.realpath(__file__))
    db_path = os.path.join(base_dir, "DirectGraphTC0004.db")
    db_url = f"sqlite:///{db_path}"
    ttl_path = os.path.join(base_dir, 'directGraphTC04.ttl')

    g = Graph()
    g.parse(ttl_path)

    with tempfile.TemporaryDirectory() as tmp:
        tables = extract_schema(db_url)

        yarrrml_mapping = generate_yarrrml(tables, base_iri="http://example.com/base/", db_uri=db_url)
        mapping_path = os.path.join(tmp,'direct_mapping.yaml')
        save_yarrrml(yarrrml_mapping, mapping_path)

        config = f'[CONFIGURATION]\ninfer_sql_datatypes=yes\noutput_format=n-triples\nna_values=,nan,None\n[DataSource]\nmappings={mapping_path}\ndb_url={db_url}'
        g_morph = morph_kgc.materialize(config)

    assert compare.isomorphic(g, g_morph)
"""
