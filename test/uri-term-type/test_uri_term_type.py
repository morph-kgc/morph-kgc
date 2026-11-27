__author__ = "Maxime Lefrançois"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"


import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


def test_uri_term_type():
    """RML-Core RMLTC0027a-JSON: rml:URI is a synonym of rml:IRI."""
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, 'output.nq'))
    mapping_path = os.path.join(TEST_DIR, 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    assert_isomorphic(g, morph_kgc.materialize(config))
