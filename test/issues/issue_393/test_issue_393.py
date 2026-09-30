__author__ = "robsyc"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"


import os
import morph_kgc

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


def test_issue_393():
    """Records with heterogeneous key sets must not drop each other's rows.

    pd.json_normalize unions columns across sibling records, so a record
    missing an optional key a sibling carries gets NaN there; dropna(how='any')
    then deletes the whole record even though none of THIS rule's references
    is null (issue #393).
    """
    g = Dataset()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'output.nq'))

    mapping_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'mapping.rml')
    config = f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'
    g_morph = morph_kgc.materialize(config)

    assert_isomorphic(g, g_morph)
