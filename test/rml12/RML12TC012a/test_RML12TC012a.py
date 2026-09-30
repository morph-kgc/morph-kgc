__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC012a: Triple-term map with a join condition
"""


def test_RML12TC012a(rml12):
    rml12.assert_matches(__file__, 'output.nt', 'N-TRIPLES')
