__author__ = "Julián Arenas-Guerrero"
__copyright__ = "Copyright © 2020 Julián Arenas-Guerrero"
__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"

"""
morph-kgc — public library API
================================
The four public ``materialize*`` wrappers accept any of the input shapes
supported by the new ``config`` sub-package:

- a file path (str / Path) to an INI config file
- a raw INI string
- a plain Python dict (flat or sectioned)
- an already-constructed ``MorphConfig`` instance

All execution logic lives in ``materializer.pipeline.materialize_pipeline``.  Nothing
related to DataFrames, mapping parsing, or source dispatching lives here.

Usage::

    import morph_kgc

    # From an INI file
    graph = morph_kgc.materialize("config.ini")

    # From a dict (flat or sectioned)
    graph = morph_kgc.materialize({
        "CONFIGURATION": {"output_format": "N-QUADS"},
        "MY_SOURCE":     {"mappings": "mapping.ttl", "db_url": "sqlite:///db"},
    })

    # Passing in-memory data
    triples = morph_kgc.materialize_set("config.ini", python_source={"df": my_df})
"""

import logging
import sys
from typing import Any

from .config.model import MorphConfig
from .config.loaders import load_config
from .constants.misc import LOGGING_NAMESPACE
from .materializer.pipeline import materialize_pipeline

LOGGER = logging.getLogger(LOGGING_NAMESPACE)


# ── Library-mode parallelization guard ───────────────────────────────────────
# mp.Pool requires the 'fork' start method, only available on Linux.
# Cap processes=1 when running as a library on macOS/Windows (see issue #94).

def _apply_library_process_guard(config: MorphConfig) -> MorphConfig:
    if "linux" not in sys.platform and config.is_multiprocessing_enabled():
        LOGGER.info(
            "Parallelization is not supported for %r when running as a library "
            "(see issue #94). Use the command line for multi-process "
            "materialization.", sys.platform
        )
        # MorphConfig is a mutable dataclass — reassign the field directly.
        object.__setattr__(config, "number_of_processes", 1)
    return config


# ── Public API ────────────────────────────────────────────────────────────────

def materialize_set(config: Any, python_source: dict | None = None) -> set[str]:
    """
    Materialize and return all triples as ``set[str]`` of N-Triples/N-Quads
    lines (without the trailing `` .``).
    """
    cfg = _apply_library_process_guard(load_config(config))
    return materialize_pipeline(cfg, python_source=python_source, output="set")


def materialize(config: Any, python_source: dict | None = None):
    """
    Materialize and return an ``rdflib.Graph`` populated with all generated
    triples, or an ``rdflib.Dataset`` when ``output_format`` is N-QUADS so that
    the statements in named graphs are kept.
    """
    cfg = _apply_library_process_guard(load_config(config))
    return materialize_pipeline(cfg, python_source=python_source, output="graph")


def materialize_oxigraph(config: Any, python_source: dict | None = None):
    """
    Materialize and return a ``pyoxigraph.Store`` populated with all
    generated triples.
    """
    cfg = _apply_library_process_guard(load_config(config))
    return materialize_pipeline(cfg, python_source=python_source, output="oxigraph")