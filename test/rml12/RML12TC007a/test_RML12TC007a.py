__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC007a: Base direction without a language
"""


def test_RML12TC007a(rml12):
    rml12.assert_rejected(__file__, 'language')
