__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC002a: Non-asserted base triple used as a triple term
"""


def test_RML12TC002a(rml12):
    rml12.assert_matches(__file__, 'output.nt', 'N-TRIPLES')
