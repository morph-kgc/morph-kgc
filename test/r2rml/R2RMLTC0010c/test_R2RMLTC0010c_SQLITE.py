__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


def test_R2RMLTC0010c():
    g = Dataset()
    g.parse(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "output.nq"),
        format="nquads",
    )

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    db_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'resource.db')
    config = f'[CONFIGURATION]\ninfer_sql_datatypes=yes\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}\ndb_url=sqlite:///{db_path}'
    g_morph = morph_kgc.materialize(config)

    assert_isomorphic(g, g_morph)
