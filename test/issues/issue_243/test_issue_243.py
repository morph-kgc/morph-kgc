__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os
import morph_kgc

from pyoxigraph import Store


def test_issue_243():
    g = Store()
    g.bulk_load(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq'), 'application/n-quads')

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.yml')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'

    g_morph = morph_kgc.materialize_oxigraph(config)

    assert set(g) == set(g_morph)
