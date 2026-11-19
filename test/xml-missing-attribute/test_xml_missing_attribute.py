__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"


import os

import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


def test_xml_missing_attribute():
    """
    A record without an attribute a rule references, of its own (@lang, @id)
    or of one of its elements (director/@name), gets no triple from that rule,
    as one without a referenced element; the run is not aborted.
    """
    expected = Dataset()
    expected.parse(os.path.join(TEST_DIR, 'output.nq'))

    g_morph = morph_kgc.materialize(
        f'[CONFIGURATION]\noutput_format=N-QUADS\nnumber_of_processes=1\n'
        f'[DataSource]\nmappings={os.path.join(TEST_DIR, "mapping.ttl")}'
    )

    assert_isomorphic(expected, g_morph)
