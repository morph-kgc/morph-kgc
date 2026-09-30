__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC004a: Directional language-tagged string with the rml:direction shortcut
"""


def test_RML12TC004a(rml12):
    rml12.assert_matches(__file__, 'output.nt', 'N-TRIPLES')
