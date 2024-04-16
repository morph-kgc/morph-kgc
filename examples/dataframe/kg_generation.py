__author__ = "Ioannis Dasoulas"
__credits__ = ["Julián Arenas-Guerrero","Ioannis Dasoulas"]

__license__ = "Apache-2.0"
__maintainer__ = "Ioannis Dasoulas"
__email__ = "ioannis.dasoulas@kuleuven.be"


import morph_kgc
import pandas as pd


users_df = pd.DataFrame({'Id': [1,2,3,4],\
                      'Username': ["@jude","@emily","@wayne","@jordan1"], \
                      'Name': ["Jude", "Emily", "Wayne", "Jordan"],\
                      'Surname': ["White", "Van de Beeck", "Peterson", "Stones"]})

followers_df = pd.DataFrame({'Id': [1,2,3,4],\
                      'Followers': [344, 456, 1221, 23]})

data_dict = {"variable1": users_df,
            "variable2": followers_df}


config = """
    [DataSource]
    mappings=./mapping.rml.ttl
"""

g_rdflib = morph_kgc.materialize(config, data_dict)

print("Knowledge graphs triples:")
for s,p,o in g_rdflib.triples((None, None, None)):
    print(s,p,o)
