__author__ = "Maxime Lefrançois"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"


import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


def _expected():
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, 'output.nq'))
    return g


def test_issue_407_mapping_directory():
    """An rml:source described as an rml:RelativePathSource (RML-IO), as in the RML-Core
    test cases of the W3C KG-Construct CG, is the file at rml:path in the directory of
    the mapping (rml:root rml:MappingDirectory)."""
    mapping_path = os.path.join(TEST_DIR, 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config)

    assert_isomorphic(_expected(), g_morph)


def test_issue_407_current_working_directory():
    """With rml:root rml:CurrentWorkingDirectory (the default), rml:path is relative to
    the current working directory (the tests run from the repository root)."""
    mapping_path = os.path.join(TEST_DIR, 'mapping_cwd.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config)

    assert_isomorphic(_expected(), g_morph)
