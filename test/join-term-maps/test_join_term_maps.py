__author__ = "Maxime Lefrançois"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"


import os
import pytest
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.mark.parametrize('case', ['RMLTC0030a', 'RMLTC0030b', 'RMLTC0030c'])
def test_join_term_maps(case):
    """RML-Core RMLTC0030a (reference), 0030b (template) and 0030c (constant): the sides of a
    join condition given by rml:childMap and rml:parentMap."""
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, case, 'output.nq'))
    mapping_path = os.path.join(TEST_DIR, case, 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    assert_isomorphic(g, morph_kgc.materialize(config))
