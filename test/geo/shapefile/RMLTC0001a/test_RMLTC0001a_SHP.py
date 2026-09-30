import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


def test_RMLTC0001a_SHP():
    g = Dataset()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq'))

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.yaml')
    config = f'[CONFIGURATION]\noutput_format=N-TRIPLES\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config)

    print("Expected:")
    print(g.serialize(format='nt'))
    print("Generated:")
    print(g_morph.serialize(format='nt'))

    assert_isomorphic(g, g_morph)
