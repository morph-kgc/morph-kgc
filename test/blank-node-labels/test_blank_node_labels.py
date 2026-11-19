__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"


import os
import subprocess
import sys

import morph_kgc
import pyoxigraph

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


def config(*options):
    return '\n'.join([
        '[CONFIGURATION]', 'output_format=N-QUADS', 'number_of_processes=1', *options,
        '[DataSource]', f'mappings={os.path.join(TEST_DIR, "mapping.ttl")}',
    ])


def test_blank_node_labels():
    """
    Blank nodes from templates, references and functions whose values hold
    characters a blank node label cannot (spaces, '/', '#', '_', a leading
    '-', non-ASCII) get valid labels: the same value is the same blank node,
    in any triples map, and distinct values (such as 'a b', 'a_20b' and 'a_b')
    distinct blank nodes.
    """
    expected = Dataset()
    expected.parse(os.path.join(TEST_DIR, 'output.nq'))

    assert_isomorphic(expected, morph_kgc.materialize(config()))
    assert len(morph_kgc.materialize_oxigraph(config())) == 16


def test_blank_node_labels_written_to_a_file(tmp_path):
    """The file the command line writes holds N-Quads that a strict parser reads."""
    output = tmp_path / 'knowledge-graph.nq'
    config_file = tmp_path / 'config.ini'
    config_file.write_text(config(f'output_file={output}'), encoding='utf-8')

    subprocess.run([sys.executable, '-m', 'morph_kgc', str(config_file)], check=True)

    assert len(list(pyoxigraph.parse(path=str(output), format=pyoxigraph.RdfFormat.N_QUADS))) == 16
