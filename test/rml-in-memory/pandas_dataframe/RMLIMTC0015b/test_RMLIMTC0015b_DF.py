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


def test_RMLTC0015b():
    df1 = pd.DataFrame({"Code":["BO","IE"],
        "Name":['"Bolivia, Plurinational State of"', "Ireland"]})
    df2 = pd.DataFrame({"Code":["BO","IE"],
        "Name":['"Estado Plurinacional de Bolivia"', "Irlanda"]})
    data_dict = {"variable1": df1, "variable2": df2}

    try:
        mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
        config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
        g_morph = morph_kgc.materialize(config, data_dict)
        assert False
    except:
        assert True
