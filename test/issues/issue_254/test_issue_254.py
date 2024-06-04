__author__ = "Mahmoud Nassif"
__credits__ = ["Mahmoud Nassif"]

__license__ = "Apache-2.0"
__maintainer__ = "Mahmoud Nassif"
__email__ = "mahmoud.abounassif@gmail.com"


import os
import morph_kgc

from rdflib.graph import Graph


def test_issue_254_a():
    g = Graph()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), "output.nq"))

    mapping_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "mapping.yml"
    )
    config = f"[CONFIGURATION]\noutput_format=N-QUADS\nnumber_of_processes=1\n[DataSource]\nmappings={mapping_path}"
    g_morph = morph_kgc.materialize(config)

    assert set(g) == set(g_morph)
