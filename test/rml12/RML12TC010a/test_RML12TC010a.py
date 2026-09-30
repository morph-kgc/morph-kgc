__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC010a: Nested triple terms
"""


def test_RML12TC010a(rml12):
    rml12.assert_matches(__file__, 'output.nt', 'N-TRIPLES')
