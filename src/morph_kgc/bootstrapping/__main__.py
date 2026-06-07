import os
import sys
from configparser import ConfigParser

from .pipeline.schema_extractor import extract_schema
from .pipeline.mappings_generator import generate_yarrrml, save_yarrrml
from .pipeline.graph_builder import materialize_graph, serialize_graph


if __name__ == "__main__":

    config_path = sys.argv[1] if len(sys.argv) > 1 else None

    if not config_path:
        print("Config path is missing.Usage: python -m morph_kgc.bootstrapping <config.ini>")
        sys.exit(2)

    config = ConfigParser()
    config.read(config_path, encoding="utf-8")

    db_url         = config.get("DataSource1",   "db_url")
    base_iri       = config.get("BOOTSTRAPPING", "base_iri",       fallback="http://example.org/")
    build_mappings = config.getboolean("BOOTSTRAPPING", "build_mappings", fallback=False)
    output_dir     = config.get("CONFIGURATION", "output_dir",     fallback="")

    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    print(f"[config] DB       : {db_url}")
    print(f"[config] Base IRI : {base_iri}")
    print(f"[config] Output   : {output_dir or '.'}\n")

    # Module 1: Extract database schema
    print(f"Connecting to: {db_url}")
    tables = extract_schema(db_url)
    print(f"Found {len(tables)} tables: {[t['name'] for t in tables]}\n")

    # Module 2: Generate YARRRML mappings
    yarrrml = generate_yarrrml(tables, base_iri=base_iri, db_uri=db_url)
    mapping_path = os.path.join(output_dir, "direct_mapping.yaml") if output_dir else "direct_mapping.yaml"
    yaml_str = save_yarrrml(yarrrml, mapping_path)

    print("--- Generated YARRRML ---")
    print(yaml_str)
    print(f"Saved mappings to: {mapping_path}")

    # Module 3 (optional): Materialize + serialize if build_mappings is enabled
    if build_mappings:
        print("\n--- Materializing RDF graph with morph-kgc ---")

        output_file   = config.get("CONFIGURATION", "output_file",   fallback="output_graph")
        output_format = config.get("CONFIGURATION", "output_format", fallback="N-TRIPLES").strip().upper()
        na_values     = config.get("CONFIGURATION", "na_values",     fallback=",nan,None")

        graph = materialize_graph(mapping_path, db_url, na_values)
        output_path = serialize_graph(graph, output_dir, output_file, output_format)

        print(f"Successfully generated {len(graph)} triples.")
        print(f"  -> {output_path}")
