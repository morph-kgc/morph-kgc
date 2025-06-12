__author__ = "Achim Reiz"
__credits__ = ["Achim Reiz"]

__license__ = "Apache-2.0"
__maintainer__ = "Achim Reiz"
__email__ = "achim.reiz@neonto.de"


import os
import rdflib
import rdflib.compare
from morph_kgc import materialize

def test_controls_true():    
    yarrrml_mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.yarrrml')
    rml_mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'cars.csv')
    config = f'[DataSource]\nmappings:{yarrrml_mapping_path}\nfile_path:{csv_path}'
    # config = f'[DataSource]\nmappings:{rml_mapping_path}\nfile_path:{csv_path}'
    rml_morph = materialize(config)
    
    assert len(rml_morph) == 1 