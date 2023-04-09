__author__ = "Ioannis Dasoulas"
__credits__ = ["Julián Arenas-Guerrero","Ioannis Dasoulas"]

__license__ = "Apache-2.0"
__maintainer__ = "Ioannis Dasoulas"
__email__ = "ioannis.dasoulas@kuleuven.be"


import os
import morph_kgc

from rdflib.graph import Graph
from rdflib import compare


def test_RMLTC0011b():
    g = Graph()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq'))

    dict1 = {
    "sports": [
        {"ID":110, "Description":"Tennis"},
        {"ID":111, "Description":"Football"},
        {"ID":112, "Description":"Formula1"}
    ]
    }
    dict2 = {
    "links": [
        {"ID_Student":10, "ID_Sport":110},
        {"ID_Student":11, "ID_Sport":111},
        {"ID_Student":11, "ID_Sport":112},
        {"ID_Student":12, "ID_Sport":111}
    ]
    }
    dict3 = {
    "students": [
        {"ID":10, "FirstName":"Venus", "LastName":"Williams"},
        {"ID":11, "FirstName":"Fernando", "LastName":"Alonso"},
        {"ID":12, "FirstName":"David", "LastName":"Villa"}
    ]
    }
    data_dict = {"variable1":dict1, "variable2":dict2, "variable3":dict3}

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config,data_dict)

    assert compare.isomorphic(g, g_morph)
