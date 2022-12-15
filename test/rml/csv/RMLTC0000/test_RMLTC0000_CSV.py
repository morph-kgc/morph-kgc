__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os
import morph_kgc

from rdflib.graph import Graph
from rdflib import compare


def test_RMLTC0000():

#     # JUST for DEBUGGING
#     g = Graph()
#     mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
#     g.parse(mapping_path)
#     q_pref = """
# prefix rr: <http://www.w3.org/ns/r2rml#>
# prefix rml: <http://semweb.mmlab.be/ns/rml#>
# prefix fno: <http://w3id.org/function/ontology#>
# prefix fnml: <http://semweb.mmlab.be/ns/fnml#>
#     """
#
#     q1 = """
#     SELECT *
#     WHERE{
#     ?exec fnml:function ?func.
#     ?func a fno:Function.
#     }
#     """
#     q1 = q_pref + q1
#     print("q1: %s" % q1)
#
#     results = g.query(q1)
#     print()

    g = Graph()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq'))

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.ttl')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config)

    assert compare.isomorphic(g, g_morph)
