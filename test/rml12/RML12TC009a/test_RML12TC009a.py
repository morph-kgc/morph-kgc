__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC009a: Triple-term map whose base triples map has no predicate-object maps
"""


def test_RML12TC009a(rml12):
    rml12.assert_matches(__file__, 'output.nt', 'N-TRIPLES')
