import os
import morph_kgc

from rdflib.graph import Graph
from rdflib import compare


def test_RMLTC0001a_GEOPARQUET():
    g = Graph()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'expected.nt'))

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-TRIPLES\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config)

    print("Expected:")
    print(g.serialize(format='nt'))
    print("Generated:")
    print(g_morph.serialize(format='nt'))
    
    assert compare.isomorphic(g, g_morph)
