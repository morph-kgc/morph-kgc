__author__ = "Achim Reiz"
__credits__ = ["Achim Reiz"]

__license__ = "Apache-2.0"
__maintainer__ = "Achim Reiz"
__email__ = "achim.reiz@neonto.de"


import os
import rdflib
import rdflib.compare
import morph_kgc

def test_string_split_by_chartype():    
    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.yarrrml')
    csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))), 'cars.csv')
    config = f'[DataSource]\nmappings:{mapping_path}\nfile_path:{csv_path}'
    rml_morph = morph_kgc.materialize(config)
    rmlmapper_goldstandard = rdflib.Graph()
    rmlmapper_goldstandard.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'rmlmapperoutput.ttl'))
    assert rdflib.compare.isomorphic(rml_morph, rmlmapper_goldstandard)