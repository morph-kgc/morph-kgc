"""Testing helpers for Morph-KGC consumers."""

from io import BytesIO

from rdflib import Dataset as RdflibDataset, Graph
from pyoxigraph import CanonicalizationAlgorithm, Dataset as OxigraphDataset, RdfFormat, Store


def assert_isomorphic(graph1, graph2):
    """
    Assert that two rdflib graphs are isomorphic.

    Works with Graph and Dataset by canonicalizing both as pyoxigraph datasets.
    """
    def to_canonical_dataset(graph):
        store = Store()
        if isinstance(graph, RdflibDataset):
            data = graph.serialize(format='nquads')
            rdf_format = RdfFormat.N_QUADS
        elif isinstance(graph, Graph):
            data = graph.serialize(format='nt')
            rdf_format = RdfFormat.N_TRIPLES
        else:
            raise TypeError(f'Unsupported graph type: {type(graph)}')

        store.bulk_load(BytesIO(data.encode('utf-8')), rdf_format)
        oxigraph_dataset = OxigraphDataset(store)
        oxigraph_dataset.canonicalize(CanonicalizationAlgorithm.RDFC_1_0)
        return oxigraph_dataset

    ds1 = to_canonical_dataset(graph1)
    ds2 = to_canonical_dataset(graph2)
    assert ds1 == ds2
