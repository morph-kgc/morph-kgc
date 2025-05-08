__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os
import morph_kgc

from rdflib.graph import Graph
from rdflib import compare


def test_date_to_date():
    g = Graph()
    output_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq')
    g.parse(output_path)
    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'calendar.csv')
    config = f'[DataSource]\nmappings={mapping_path}\nfile_path:{file_path}'
    g_morph = morph_kgc.materialize(config)

    assert compare.isomorphic(g, g_morph)