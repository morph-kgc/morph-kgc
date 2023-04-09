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


def test_RMLTC0011b():
    g = Graph()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq'))

    df1 = pd.DataFrame({"ID":[10,11,12], "FirstName":["Venus","Fernando","David"],
         "LastName":["Williams","Alonso","Villa"]})
    df2 = pd.DataFrame({"ID":[110,111,112], "Description":["Tennis","Football","Formula1"]})
    df3 = pd.DataFrame({"ID_Student":[10,11,11,12], "ID_Sport":[110,111,112,111]})
    data_dict = {"variable1":df1, "variable2":df2, "variable3":df3}

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config, data_dict)

    assert compare.isomorphic(g, g_morph)
