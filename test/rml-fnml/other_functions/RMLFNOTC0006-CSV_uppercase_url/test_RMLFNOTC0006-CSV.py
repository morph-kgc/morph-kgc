__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os
import morph_kgc

from rdflib.graph import Graph
from rdflib import compare


def test_RMLFNOTC0006_CSV():
    g = Graph()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq'))

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    csv_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'student.csv')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\nnumber_of_processes=1\n[DataSource]\nmappings={mapping_path}\nfile_path:{csv_path}'
    g_morph = morph_kgc.materialize(config)

    assert compare.isomorphic(g, g_morph)
