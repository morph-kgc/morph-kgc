__author__ = "Ioannis Dasoulas"
__credits__ = ["Julián Arenas-Guerrero","Ioannis Dasoulas"]

__license__ = "Apache-2.0"
__maintainer__ = "Ioannis Dasoulas"
__email__ = "ioannis.dasoulas@kuleuven.be"


import os
import morph_kgc

from rdflib.graph import Graph
from rdflib import compare


def test_RMLTC0015a():
    g = Graph()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq'))

    dict1 = {
    "countries": [
        {"Code": "BO", "Name": "Bolivia, Plurinational State of"},
        {"Code": "IE", "Name": "Ireland"}
    ]
    }
    dict2 = {
    "countries": [
        {"Code": "BO", "Name": "Estado Plurinacional de Bolivia"},
        {"Code": "IE", "Name": "Irlanda"}
    ]
    }
    data_dict = {"variable1":dict1, "variable2":dict2}

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config,data_dict)

    assert compare.isomorphic(g, g_morph)
