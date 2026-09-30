__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC015a: Nested triple-term map without a join under one that has one
"""


def test_RML12TC015a(rml12):
    rml12.assert_matches(__file__, 'output.nt', 'N-TRIPLES')
