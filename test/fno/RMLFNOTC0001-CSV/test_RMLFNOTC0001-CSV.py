__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os
import morph_kgc

from rdflib.graph import Graph
from rdflib import compare


def test_RMLFNOTC0001_CSV():
    g = Graph()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.ttl'))

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    # mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping-shortcut.ttl')
    # mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping-verbose.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-TRIPLES\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config)

    assert compare.isomorphic(g, g_morph)

