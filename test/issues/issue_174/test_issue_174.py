__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os
import morph_kgc

from pyoxigraph import Store


def test_issue_174_a():
    g = Store()
    g.bulk_load(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output-a.nq'), 'application/n-quads')

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping-a.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'

    g_morph = morph_kgc.materialize_oxigraph(config)

    assert set(g) == set(g_morph)

def test_issue_174_b():
    g = Store()
    g.bulk_load(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output-b.nq'), 'application/n-quads')

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping-b.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'

    g_morph = morph_kgc.materialize_oxigraph(config)

    assert set(g) == set(g_morph)

def test_issue_174_c():
    g = Store()
    g.bulk_load(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output-c.nq'), 'application/n-quads')

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping-c.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'

    g_morph = morph_kgc.materialize_oxigraph(config)

    assert set(g) == set(g_morph)
