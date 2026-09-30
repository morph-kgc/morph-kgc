__author__ = "Ioannis Dasoulas"
__credits__ = ["Julián Arenas-Guerrero","Ioannis Dasoulas"]

__license__ = "Apache-2.0"
__maintainer__ = "Ioannis Dasoulas"
__email__ = "ioannis.dasoulas@kuleuven.be"


import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


def test_RMLTC0013a():
    g = Dataset()
    g.parse(
        os.path.join(os.path.dirname(os.path.realpath(__file__)), "output.nq"),
        format="nquads",
    )
    
    dict1 = {
    "persons": [
        {"ID":"1","Name":"Alice","DateOfBirth":None},
        {"ID":"2","Name":"Bob","DateOfBirth":"September, 2010"}
    ]
    }
    data_dict = {"variable1":dict1}

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config,data_dict)

    assert_isomorphic(g, g_morph)
