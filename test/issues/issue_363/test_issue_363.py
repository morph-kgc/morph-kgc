from pathlib import Path
from morph_kgc import convert_to_rml

CURRENT_DIR = Path(__file__).parent

def compare_graph_with_file(input_file: Path, compared_file: Path):
    """
    Genera un grafo RDF a partir de input_file y lo compara con el TTL de compared_file
    """
    # Generamos el grafo
    generated_graph = convert_to_rml(input_file)
    
    # Serializamos a TTL
    generated_ttl = generated_graph.serialize(format="ttl")
    
    # Leemos el TTL de referencia tal cual
    with open(compared_file, "r", encoding="utf-8") as f:
        expected_ttl = f.read()
    
    # Comparamos como texto
    assert generated_ttl.strip() == expected_ttl.strip(), f"{input_file} no coincide con {compared_file}"

def test_prueba_yml():
    compare_graph_with_file(CURRENT_DIR / "prueba.yml", CURRENT_DIR / "prueba_yml_expected.ttl")

def test_prueba_yarrrml():
    compare_graph_with_file(CURRENT_DIR / "prueba_yarrrml.yaml", CURRENT_DIR / "prueba_yarrrml_expected.ttl")

