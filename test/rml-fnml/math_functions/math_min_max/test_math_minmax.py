__author__ = "Achim Reiz"
__credits__ = ["Achim Reiz"]

__license__ = "Apache-2.0"
__maintainer__ = "Achim Reiz"
__email__ = "achim.reiz@neonto.de"


import os, rdflib
import morph_kgc

def test_math_min_max():    
    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.yarrrml')
    csv_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'cars.csv')
    config = f'[DataSource]\nmappings:{mapping_path}\nfile_path:{csv_path}'
    rml_morph = morph_kgc.materialize(config)
    
    
    check1 = [float(o.value) < 10 for s,v,o in rml_morph.triples((None, rdflib.URIRef("http://example.com#number_check"), None))]
    
    assert all(check1)
    assert len(check1)> 1
    check2 = [float(o.value)> 1000 for s,v,o in rml_morph.triples((None, rdflib.URIRef("http://example.com#number_check_2"), None))]
    assert (len(check2) > 1)
    assert all(check2)