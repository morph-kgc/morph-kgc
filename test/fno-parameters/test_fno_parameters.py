__author__ = "Maxime Lefrançois"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"


import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


def test_grel_parameter_iri():
    """RML-FNML RMLFNMLTC0002-CSV: the input of grel:toUpperCase is named by its
    fno:Parameter (grel:valueParam), not by its fno:predicate (grel:valueParameter)."""
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, 'output.nq'))
    mapping_path = os.path.join(TEST_DIR, 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    assert_isomorphic(g, morph_kgc.materialize(config))
