__author__ = "Achim Reiz"
__credits__ = ["Achim Reiz"]

__license__ = "Apache-2.0"
__maintainer__ = "Achim Reiz"
__email__ = "achim.reiz@neonto.de"


import os
import rdflib
import rdflib.compare
import morph_kgc

def test_other_type():
    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.yarrrml')
    csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    config = f'[DataSource]\nmappings:{mapping_path}\n'
    rml_morph = morph_kgc.materialize(config)
    assert all([o.value == "str" for s,v,o in rml_morph])
