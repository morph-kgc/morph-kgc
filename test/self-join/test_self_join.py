__author__ = "Maxime Lefrançois"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"


import os
import pytest
import morph_kgc

from rdflib import Dataset
from morph_kgc.config.loaders import load_config
from morph_kgc.constants import RML_PARENT_TRIPLES_MAP
from morph_kgc.mapping.parser import retrieve_mappings
from morph_kgc.testing import assert_isomorphic


TEST_DIR = os.path.dirname(os.path.realpath(__file__))

# case -> whether the self join can be replaced by joining each row with itself
CASES = {
    # RMLTC0033a (proposed to kg-construct/rml-core): neither subject depends on the join
    # column only, each stop is joined with every stop of its trip
    'RMLTC0033a-CSV': False,
    'RMLTC0033a-JSON': False,
    # the child subject, predicate and graph depend on the join column only
    'child-subject-on-join-column': True,
    # the parent subject depends on the join column only
    'parent-subject-on-join-column': True,
    # the child subject depends on the join column only, but not its graph
    'graph-map-not-on-join-column': False,
}


def _config(case):
    mapping_path = os.path.join(TEST_DIR, case, 'mapping.ttl')
    return f'[CONFIGURATION]\noutput_format=N-QUADS\n[DataSource]\nmappings={mapping_path}'


@pytest.mark.parametrize('case', CASES)
def test_self_join(case):
    """A join of a logical source with itself on the same column gives the triples of the
    join, removed or not."""
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, case, 'output.nq'))
    assert_isomorphic(g, morph_kgc.materialize(_config(case)))


@pytest.mark.parametrize('case', CASES)
def test_self_join_removed(case):
    """The self join is removed only when one of the subjects depends on the join column
    only."""
    rules = retrieve_mappings(load_config(_config(case))).rules
    joins = [r for r in rules if r.object_ and r.object_.map_type == RML_PARENT_TRIPLES_MAP]
    assert (not joins) == CASES[case]
