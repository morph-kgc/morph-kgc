__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
RML12TC005a: Directional language-tagged string from rml:languageMap and rml:directionMap
"""


def test_RML12TC005a(rml12):
    rml12.assert_matches(__file__, 'output.nt', 'N-TRIPLES')
