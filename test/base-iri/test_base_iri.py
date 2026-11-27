__author__ = "Maxime Lefrançois"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"


import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


def _materialize(case, base_iri):
    mapping_path = os.path.join(TEST_DIR, case, 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\nbase_iri={base_iri}\n[DataSource]\nmappings={mapping_path}'
    return morph_kgc.materialize(config)


def _expected(case):
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, case, 'output.nq'))
    return g


def test_base_iri_option():
    """RML-Core RMLTC0019a: a relative IRI ("Carlos") is resolved against the base IRI given in
    the configuration (base_iri), the base of the test case."""
    assert_isomorphic(_expected('RMLTC0019a'), _materialize('RMLTC0019a', 'http://example.com/'))


def test_triples_map_base_iri():
    """RML-Core RMLTC0026c: the rml:baseIRI of a triples map takes the place of base_iri (the
    subjects; the objects are JSON integers)."""
    got = _materialize('RMLTC0026c', 'http://example.org/other/')
    subjects = {str(s) for s, _, _, _ in got.quads()}
    expected = {str(s) for s, _, _, _ in _expected('RMLTC0026c').quads()}
    assert subjects == expected
