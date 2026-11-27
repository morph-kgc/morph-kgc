__author__ = "Maxime Lefrançois"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"


import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


def _run(mapping, output):
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, output))
    mapping_path = os.path.join(TEST_DIR, mapping)
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    assert_isomorphic(g, morph_kgc.materialize(config))


def test_json_integer_natural_datatype():
    """RML-Core: a JSON integer gives its natural RDF literal, "30"^^xsd:integer (RMLTC0012a-JSON
    of kg-construct/rml-core)."""
    _run('mapping.ttl', 'output.nq')


def test_json_integer_legacy_rml():
    """Legacy RML: the same values give simple literals (RMLTC0012a-JSON of
    RMLio/rml-test-cases)."""
    _run('mapping_legacy.ttl', 'output_legacy.nq')
