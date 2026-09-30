__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC011a: Base triples map with several predicate-object maps
"""


def test_RML12TC011a(rml12):
    produced = rml12.materialize_within(__file__)
    assert produced == rml12.expected_lines(__file__, 'output.nt')
