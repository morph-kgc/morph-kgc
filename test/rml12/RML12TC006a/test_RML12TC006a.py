__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC006a: Invalid base direction token
"""


def test_RML12TC006a(rml12):
    rml12.assert_rejected(__file__, 'direction')
