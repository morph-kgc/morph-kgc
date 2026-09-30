__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC008a: Cycle in the rml:tripleTermMap reference graph
"""


def test_RML12TC008a(rml12):
    rml12.assert_rejected(__file__, 'cycle')
