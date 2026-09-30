__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os
import sys

import morph_kgc
import pytest

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic

sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))
from server import PASSWORD, USERNAME, VocabularyServer   # noqa: E402

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
MAPPING = os.path.join(TEST_DIR, 'mapping.ttl')
VOCABULARY = os.path.join(TEST_DIR, 'disease_vocabulary.ttl')


def config(resource_options, mapping=MAPPING, processes=1):
    return (
        f'[CONFIGURATION]\n'
        f'output_format=N-QUADS\n'
        f'number_of_processes={processes}\n'
        f'[RESOURCE:disease_vocabulary]\n'
        f'{resource_options}\n'
        f'[DataSource]\n'
        f'mappings={mapping}'
    )


def expected_graph():
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, 'output.nq'))
    return g


# ── SKOS vocabulary ───────────────────────────────────────────────────────────

def test_reconcile_vocabulary_from_file():
    """A vocabulary declared as a local path is indexed and reconciled against."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}'
    ))

    assert_isomorphic(expected_graph(), g_morph)


def test_reconcile_vocabulary_over_http_with_basic_authentication():
    """The vocabulary is fetched once, with HTTP Basic Authentication."""
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(config(
            f'resource_type=SKOS_VOCABULARY\n'
            f'url={server.url}/vocabulary\n'
            f'username={USERNAME}\n'
            f'password={PASSWORD}\n'
            f'format=turtle'
        ))

        # Initialized once, before any triple was materialized, even though the
        # mapping reconciles in four mapping groups.
        assert [path for path, _ in server.requests] == ['/vocabulary']

    assert_isomorphic(expected_graph(), g_morph)


def test_reconcile_vocabulary_over_http_without_credentials():
    """A protected vocabulary reports what is missing instead of failing blindly."""
    with VocabularyServer() as server:
        with pytest.raises(ValueError, match='Basic Authentication'):
            morph_kgc.materialize(config(
                f'resource_type=SKOS_VOCABULARY\n'
                f'url={server.url}/vocabulary'
            ))


def test_reconcile_vocabulary_with_multiple_processes():
    """Worker processes read the shared context back from the state directory."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        processes=4,
    ))

    assert_isomorphic(expected_graph(), g_morph)


def test_reconcile_vocabulary_referenced_by_its_iri():
    """
    A mapping may name the vocabulary by the IRI that identifies it, while
    where it is downloaded from stays in the configuration file.
    """
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'iri=https://example.org/vocabulary/disease\n'
        f'url={VOCABULARY}',
        mapping=os.path.join(TEST_DIR, 'mapping_vocabulary_iri.ttl'),
    ))

    assert_isomorphic(expected_graph(), g_morph)


def test_reconcile_case_insensitive_matching():
    """Case-insensitive matching reconciles values that differ in case only."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}\n'
        f'matching=CASE-INSENSITIVE',
        mapping=os.path.join(TEST_DIR, 'mapping_uppercase.ttl'),
    ))

    g = Dataset()
    g.parse(os.path.join(TEST_DIR, 'output_reconciled.nq'))

    assert_isomorphic(g, g_morph)


def test_reconcile_from_yarrrml():
    """The same reconciliation expressed in YARRRML instead of RML-FNML."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        mapping=os.path.join(TEST_DIR, 'mapping.yarrrml'),
    ))

    g = Dataset()
    g.parse(os.path.join(TEST_DIR, 'output_reconciled.nq'))

    assert_isomorphic(g, g_morph)


def test_exact_matching_by_default():
    """Exact matching is the default, so a value in another case matches nothing."""
    g_morph = morph_kgc.materialize(config(
        f'resource_type=SKOS_VOCABULARY\n'
        f'url={VOCABULARY}',
        mapping=os.path.join(TEST_DIR, 'mapping_uppercase.ttl'),
    ))

    assert len(g_morph) == 0


def test_undeclared_resource():
    """A mapping reconciling against an undeclared resource says so."""
    with pytest.raises(ValueError, match='not declared in the configuration'):
        morph_kgc.materialize(
            f'[CONFIGURATION]\n'
            f'output_format=N-QUADS\n'
            f'[DataSource]\n'
            f'mappings={MAPPING}'
        )


def test_wrong_resource_type():
    """Reconciling a vocabulary against a SPARQL endpoint resource is reported."""
    with pytest.raises(ValueError, match='resource_type'):
        morph_kgc.materialize(config(
            f'resource_type=SPARQL_ENDPOINT\n'
            f'url={VOCABULARY}'
        ))


# ── SPARQL endpoint ───────────────────────────────────────────────────────────

def sparql_config(extra_options='', processes=1):
    return (
        f'[CONFIGURATION]\n'
        f'output_format=N-QUADS\n'
        f'number_of_processes={processes}\n'
        f'[RESOURCE:disease_endpoint]\n'
        f'{extra_options}\n'
        f'[DataSource]\n'
        f'mappings={os.path.join(TEST_DIR, "mapping_sparql.ttl")}'
    )


@pytest.mark.parametrize('method', ['GET', 'POST'])
def test_reconcile_sparql_endpoint(method):
    """The endpoint is queried once and its answer reused across all rules."""
    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            f'resource_type=SPARQL_ENDPOINT\n'
            f'url={server.url}/sparql\n'
            f'username={USERNAME}\n'
            f'password={PASSWORD}\n'
            f'method={method}'
        ))

        paths = [path for path, _ in server.requests]
        assert paths == ['/sparql']
        # No 'query' option is declared, so the default SKOS query is sent.
        assert 'skos/core#prefLabel' in server.requests[0][1]

    assert_isomorphic(expected_graph(), g_morph)


def test_reconcile_sparql_endpoint_with_declared_query():
    """A resource may declare the query the shared context is built with."""
    query = (
        'SELECT ?concept ?attribute ?value WHERE '
        '{ ?concept ?attribute ?value }'
    )

    with VocabularyServer() as server:
        g_morph = morph_kgc.materialize(sparql_config(
            f'resource_type=SPARQL_ENDPOINT\n'
            f'url={server.url}/sparql\n'
            f'username={USERNAME}\n'
            f'password={PASSWORD}\n'
            f'query={query}'
        ))

        assert server.requests[0][1] == query

    assert_isomorphic(expected_graph(), g_morph)
