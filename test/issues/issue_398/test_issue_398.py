__author__ = "Ho Geun Choi"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"


import json
import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


def _expected(output_file):
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, output_file))
    return g


def test_issue_398():
    """JSON references and templates written with a leading `$.` (as in RML-Core) must
    select the same fields as the bare form, instead of silently producing no triples."""
    mapping_path = os.path.join(TEST_DIR, 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config)

    assert_isomorphic(_expected('output.nq'), g_morph)


def test_issue_398_in_memory():
    """Same as above for an in-memory dictionary source (top-level fields only)."""
    with open(os.path.join(TEST_DIR, 'data.json'), encoding='utf-8') as f:
        data_dict = {'variable1': json.load(f)}

    mapping_path = os.path.join(TEST_DIR, 'mapping_dictionary.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config, data_dict)

    assert_isomorphic(_expected('output_dictionary.nq'), g_morph)
