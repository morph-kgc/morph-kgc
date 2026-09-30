__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC014a: Nested triple-term maps, each with its own join condition
"""


def test_RML12TC014a(rml12):
    rml12.assert_matches(__file__, 'output.nt', 'N-TRIPLES')
