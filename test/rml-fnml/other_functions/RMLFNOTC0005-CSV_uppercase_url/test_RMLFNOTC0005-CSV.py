__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


def test_RMLFNOTC0005_CSV():
    g = Dataset()
    g.parse(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "output.nq"),
        format="nquads",
    )

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    csv_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'student.csv')
    
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\nnumber_of_processes=1\n[DataSource]\nmappings={mapping_path}\nfile_path:{csv_path}'
    g_morph = morph_kgc.materialize(config)

    assert_isomorphic(g, g_morph)
