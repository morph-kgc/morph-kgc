__author__ = "Stéphane Branly"
__credits__ = ["Stéphane Branly"]

__license__ = "Apache-2.0"
__maintainer__ = "Stéphane Branly"
__email__ = "stephanebranly.pro@gmail.com"


import os
import morph_kgc
import json
from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


def test_issue_316_a():
    g = Dataset()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), "output.nq"))

    udfs_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "udfs.py"
    )
    mapping_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "mapping.ttl"
    )

    with open(os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "a.json"
    ), mode='r') as f:
        json_data = json.loads(f.read())
    
    config = f"[CONFIGURATION]\nudfs={udfs_path}\n[DataSource]\nmappings={mapping_path}"
    g_morph = morph_kgc.materialize(config, {'data': json_data})

    assert_isomorphic(g, g_morph)

def test_issue_316_b():
    g = Dataset()
    g.parse(os.path.join(os.path.dirname(os.path.realpath(__file__)), "output.nq"))

    udfs_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "udfs.py"
    )
    mapping_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "mapping.ttl"
    )

    with open(os.path.join(
        os.path.dirname(os.path.realpath(__file__)), "b.json"
    ), mode='r') as f:
        json_data = json.loads(f.read())
    
    config = f"[CONFIGURATION]\nudfs={udfs_path}\n[DataSource]\nmappings={mapping_path}"
    g_morph = morph_kgc.materialize(config, {'data': json_data})

    assert_isomorphic(g, g_morph)
