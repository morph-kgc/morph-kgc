__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC001a: Asserted reifying triple with one CSV source
"""


def test_RML12TC001a(rml12):
    rml12.assert_matches(__file__, 'output.nt', 'N-TRIPLES')
