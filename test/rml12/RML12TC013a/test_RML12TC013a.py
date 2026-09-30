__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC013a: Graph map on the base triples map does not reach the triple term
"""


def test_RML12TC013a(rml12):
    rml12.assert_matches(__file__, 'output.nq', 'N-QUADS')
