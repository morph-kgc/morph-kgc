__author__ = "Ioannis Dasoulas"
__credits__ = ["Julián Arenas-Guerrero","Ioannis Dasoulas"]

__license__ = "Apache-2.0"
__maintainer__ = "Ioannis Dasoulas"
__email__ = "ioannis.dasoulas@kuleuven.be"


import os
import morph_kgc

from rdflib.graph import Graph
from rdflib import compare
import pandas as pd


def test_RMLTC0010c():
    g = Graph()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq'))

    df = pd.DataFrame({"Country Code":[1,2,3],
        "Name":["Bolivia, Plurinational State of", "Ireland","Saint Martin (French part)"],
        "ISO 3166":["BO","IE","MF"]})
    data_dict = {"variable1": df}

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config, data_dict)

    assert compare.isomorphic(g, g_morph)
