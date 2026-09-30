__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"


import os

import morph_kgc
import pytest

from rdflib import Dataset
from morph_kgc.testing import assert_isomorphic


TEST_DIR = os.path.dirname(os.path.realpath(__file__))


def config(state_dir=''):
    mapping_path = os.path.join(TEST_DIR, 'mapping.ttl')
    udf_path = os.path.join(TEST_DIR, 'udf.py')
    names_path = os.path.join(TEST_DIR, 'country_names.json')

    return (
        f'[CONFIGURATION]\n'
        f'output_format=N-QUADS\n'
        f'udfs={udf_path}\n'
        f'state_dir={state_dir}\n'
        f'[RESOURCE:country_names]\n'
        f'resource_type=JSON_FILE\n'
        f'url={names_path}\n'
        f'[DataSource]\n'
        f'mappings={mapping_path}'
    )


def expected_graph():
    g = Dataset()
    g.parse(os.path.join(TEST_DIR, 'output.nq'))
    return g


def test_stateful_udf():
    """The shared context is built once and used by every function invocation."""
    g_morph = morph_kgc.materialize(config())

    assert_isomorphic(expected_graph(), g_morph)


def test_stateful_udf_temporary_state_is_removed():
    """The state directory the engine creates does not outlive the run."""
    import tempfile
    from glob import glob
    from morph_kgc.config.loaders import load_config

    pattern = os.path.join(tempfile.gettempdir(), 'morph-kgc-state-*')
    before = set(glob(pattern))

    cfg = load_config(config())
    morph_kgc.materialize(cfg)

    # Neither on disk nor in the configuration the run was handed.
    assert set(glob(pattern)) == before
    assert cfg.state_dir == ''


def test_stateful_udf_failing_initializer_leaves_no_state():
    """A failing initializer cleans up the state directory it opened."""
    import tempfile
    from glob import glob
    from morph_kgc.config.loaders import load_config
    from morph_kgc.functions.registry import FunctionRegistry

    pattern = os.path.join(tempfile.gettempdir(), 'morph-kgc-state-*')

    cfg = load_config(config())
    # Load the UDFs so that the registry cache holds the entry to patch.
    FunctionRegistry.get('http://example.com/countryName', cfg)
    entry = FunctionRegistry._udf_caches[cfg.udfs]['http://example.com/countryName']
    initializer = entry['initializer']

    def failing_initializer(initialization):
        raise ValueError('the vocabulary is unreachable')

    entry['initializer'] = failing_initializer
    before = set(glob(pattern))

    try:
        with pytest.raises(ValueError, match='unreachable'):
            morph_kgc.materialize(cfg)
    finally:
        entry['initializer'] = initializer

    assert set(glob(pattern)) == before
    assert cfg.state_dir == ''


def test_stateful_udf_persists_the_context_to_disk(tmp_path):
    """A configured state directory holds the context after materialization."""
    state_dir = str(tmp_path / 'state')
    g_morph = morph_kgc.materialize(config(state_dir=state_dir))

    assert_isomorphic(expected_graph(), g_morph)
    assert os.path.isdir(state_dir)
    assert [f for f in os.listdir(state_dir) if f.endswith('.pickle')]


def test_stateful_udf_initializer_runs_once():
    """The initializer is executed a single time, before any triple is built."""
    from morph_kgc.config.loaders import load_config
    from morph_kgc.functions.registry import FunctionRegistry
    from morph_kgc.functions import state

    cfg = load_config(config())
    # Loading the UDFs eagerly lets us count the initializer invocations.
    initializer = FunctionRegistry.get('http://example.com/countryName', cfg).initializer

    calls = []

    def counting_initializer(initialization):
        calls.append(initialization.function_iri)
        return initializer(initialization)

    FunctionRegistry._udf_caches[cfg.udfs]['http://example.com/countryName'][
        'initializer'] = counting_initializer
    state._LOADED_CONTEXTS.clear()

    try:
        # A single process, so that the count is not spread over workers.
        cfg.number_of_processes = 1
        g_morph = morph_kgc.materialize(cfg)
    finally:
        FunctionRegistry._udf_caches[cfg.udfs]['http://example.com/countryName'][
            'initializer'] = initializer
        state._LOADED_CONTEXTS.clear()

    assert calls == ['http://example.com/countryName']
    assert_isomorphic(expected_graph(), g_morph)


def test_no_state_directory_without_stateful_functions():
    """A mapping using no stateful function pays nothing for the feature."""
    from morph_kgc.config.loaders import load_config

    udf_dir = os.path.join(os.path.dirname(TEST_DIR), 'udf')
    cfg = load_config(
        f'[CONFIGURATION]\n'
        f'output_format=N-QUADS\n'
        f'udfs={os.path.join(udf_dir, "udf.py")}\n'
        f'[DataSource]\n'
        f'mappings={os.path.join(udf_dir, "mapping.ttl")}'
    )
    morph_kgc.materialize(cfg)

    assert cfg.state_dir == ''


def test_stateful_udf_without_initialization():
    """A stateful function cannot be executed before its context is built."""
    from morph_kgc.config.loaders import load_config
    from morph_kgc.functions.state import get_context
    from morph_kgc.functions import state

    cfg = load_config(config())
    state._LOADED_CONTEXTS.clear()

    with pytest.raises(FileNotFoundError):
        get_context('http://example.com/countryName', cfg)
