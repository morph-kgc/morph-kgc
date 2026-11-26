__author__ = "Maxime Lefrançois"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"


import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


def test_json_bracket_reference():
    """RML-Core RMLTC0010c-JSON: a field name with a space, written in the bracket notation of
    JSONPath ({$['Country Code']})."""
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, 'output.nq'))
    mapping_path = os.path.join(TEST_DIR, 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    assert_isomorphic(g, morph_kgc.materialize(config))
