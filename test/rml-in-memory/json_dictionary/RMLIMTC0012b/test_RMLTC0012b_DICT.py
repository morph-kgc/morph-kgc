__author__ = "Ioannis Dasoulas"
__credits__ = ["Julián Arenas-Guerrero","Ioannis Dasoulas"]

__license__ = "Apache-2.0"
__maintainer__ = "Ioannis Dasoulas"
__email__ = "ioannis.dasoulas@kuleuven.be"


import os
import morph_kgc

from rdflib.graph import Graph


def test_RMLTC0012b():
    g = Graph()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq'))

    dict1 = {
    "lives": [
        {"fname":"Bob","lname":"Smith","city":"London"},
        {"fname":"Sue","lname":"Jones","city":"Madrid"},
        {"fname":"Bob","lname":"Smith","city":"London"}
    ]
    }
    dict2 = {
    "persons": [
        {"fname":"Bob","lname":"Smith","amount":30},
        {"fname":"Sue","lname":"Jones","amount":20},
        {"fname":"Bob","lname":"Smith","amount":30}
    ]
    }
    data_dict = {"variable1":dict1, "variable2":dict2}

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config,data_dict)

    assert g.isomorphic(g_morph)
