__author__ = "Achim Reiz"
__credits__ = ["Achim Reiz"]

__license__ = "Apache-2.0"
__maintainer__ = "Achim Reiz"
__email__ = "achim.reiz@neonto.de"


import os
import rdflib
import rdflib.compare
from morph_kgc import materialize

def test_fnml_translation():    
    mapping_path_yarrrml = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.yarrrml')
    mapping_path_fnml = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'fnml.ttl')
    csv_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cars.csv')
    config = f'[DataSource]\nmappings:{mapping_path_yarrrml}\nfile_path:{csv_path}'
    config_fnml = f'[DataSource]\nmappings:{mapping_path_fnml}\nfile_path:{csv_path}'
    rml_morph_fnml = materialize(config_fnml)
    rml_morph = materialize(config)
    assert rdflib.compare.isomorphic(rml_morph, rml_morph_fnml), "RML and FNML mappings are not isomorphic"
