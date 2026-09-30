__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Testing helpers for comparing materialized knowledge graphs.

rdflib's own isomorphism checks ignore graph names, and ``Graph.isomorphic``
does not compare triples with blank nodes at all. ``assert_isomorphic``
canonicalizes both sides as RDF datasets instead, so that named graphs and
blank nodes are compared as well.
"""

from io import BytesIO

from pyoxigraph import CanonicalizationAlgorithm, Dataset as OxigraphDataset, RdfFormat, Store
from rdflib import Dataset, Graph


def _to_canonical_dataset(graph: Graph) -> OxigraphDataset:
    if isinstance(graph, Dataset):
        data, rdf_format = graph.serialize(format='nquads'), RdfFormat.N_QUADS
    elif isinstance(graph, Graph):
        data, rdf_format = graph.serialize(format='nt'), RdfFormat.N_TRIPLES
    else:
        raise TypeError(f'Unsupported graph type: {type(graph).__name__}.')

    store = Store()
    store.bulk_load(BytesIO(data.encode('utf-8')), rdf_format)
    dataset = OxigraphDataset(store)
    dataset.canonicalize(CanonicalizationAlgorithm.RDFC_1_0)
    return dataset


def assert_isomorphic(expected: Graph, actual: Graph) -> None:
    """
    Assert that two rdflib graphs or datasets are isomorphic. A Graph is
    compared as a dataset with all its triples in the default graph.
    """
    expected_dataset = _to_canonical_dataset(expected)
    actual_dataset = _to_canonical_dataset(actual)

    missing = {str(quad) for quad in expected_dataset} - {str(quad) for quad in actual_dataset}
    unexpected = {str(quad) for quad in actual_dataset} - {str(quad) for quad in expected_dataset}
    assert expected_dataset == actual_dataset, (
        f'Graphs are not isomorphic.\n'
        f'Missing statements: {sorted(missing)}\n'
        f'Unexpected statements: {sorted(unexpected)}'
    )
