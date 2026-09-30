__author__ = "Ioannis Dasoulas"
__credits__ = ["Julián Arenas-Guerrero","Ioannis Dasoulas"]

__license__ = "Apache-2.0"
__maintainer__ = "Ioannis Dasoulas"
__email__ = "ioannis.dasoulas@kuleuven.be"


import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


def test_RMLTC0009b():
    g = Dataset()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq'))

    dict1 = {
    "sports": [
        {
        "ID": 100,
        "Name": "Tennis"
        }
    ]
    }
    dict2 = {
    "students" : [
        { 
        "ID": 10,
        "Sport": 100,
        "Name": "Venus Williams"
        },
        { 
        "ID": 20,
        "Name": "Demi Moore"
        }
    ]
    }
    data_dict = {"variable1":dict1, "variable2":dict2}

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config,data_dict)

    assert_isomorphic(g, g_morph)
