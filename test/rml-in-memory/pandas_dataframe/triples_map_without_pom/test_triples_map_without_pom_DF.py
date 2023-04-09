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

def test_triples_map_without_pom():
    g = Graph()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq'))

    df1 = pd.DataFrame({"ID":[10,20], "Sport":["100",""], "Name":["Venus Williams","Demi Moore"]})
    df2 = pd.DataFrame({"ID":[100], "Name":["Tennis"]})
    data_dict = {"variable1":df1, "variable2":df2}

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config, data_dict)

    assert compare.isomorphic(g, g_morph)
