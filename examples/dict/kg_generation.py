__author__ = "Ioannis Dasoulas"
__credits__ = ["Julián Arenas-Guerrero","Ioannis Dasoulas"]

__license__ = "Apache-2.0"
__maintainer__ = "Ioannis Dasoulas"
__email__ = "ioannis.dasoulas@kuleuven.be"


import morph_kgc


users_dict = {"users": [
        {
            "id": 1,
            "username": "@jude",
            "name": "Jude",
            "surname": "White"
        },
        {
            "id": 2,
            "username": "@emily",
            "name": "Emily",
            "surname": "Van de Beeck"
        },
        {
            "id": 3,
            "username": "@wayne",
            "name": "Wayne",
            "surname": "Peterson"
        },
        {
            "id": 4,
            "username": "@jordan1",
            "name": "Jordan",
            "surname": "Stones"
        }
    ]}


followers_dict = {"followers": [
        {
            "id": 1,
            "follows": [2,3],
            "followed_by": 2
        },
        {
            "id": 2,
            "follows": [3,5],
            "followed_by": [1,3,4,5]
        },
        {
            "id": 3,
            "follows": [1,2],
            "followed_by": 1
        },
        {
            "id": 4,
            "follows": [1,2,3],
            "followed_by": [2,3]
        }
    ]}

data_dict = {"variable1": users_dict,
            "variable2": followers_dict}

config = """
    [DataSource]
    mappings=./mapping.rml.ttl
"""

g_rdflib = morph_kgc.materialize('./config.ini', data_dict)

print("Knowledge graphs triples:")
for s,p,o in g_rdflib.triples((None, None, None)):
    print(s,p,o)
