__author__ = "Maxime Lefrançois"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"


import os
import pytest
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.mark.parametrize('case', ['RMLTC0028a', 'RMLTC0028c'])
def test_constant_literal(case):
    """RML-Core RMLTC0028a (rml:object true, an xsd:boolean) and RMLTC0028c (rml:object
    "train"@en): a constant literal keeps its datatype or language tag."""
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, case, 'output.nq'))
    mapping_path = os.path.join(TEST_DIR, case, 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    assert_isomorphic(g, morph_kgc.materialize(config))
