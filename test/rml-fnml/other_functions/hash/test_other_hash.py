__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"


import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


def test_other_hash():
    """The hash built-ins: the SHA-256 of a value, as a literal and as an IRI."""
    g = Dataset()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq'))

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\nnumber_of_processes=1\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config)

    assert_isomorphic(g, g_morph)
