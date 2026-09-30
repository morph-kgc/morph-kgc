__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
morph-kgc CLI entry point
==========================
Invoked as::

    python -m morph_kgc <config.ini>

Flow
----
1. ``config.load_from_cli()`` parses sys.argv (argparse lives in
   ``config/loaders.py``) and returns a validated ``MorphConfig``.
2. The materialization pipeline runs via
   ``materializer.pipeline.materialize_pipeline``, which parses the mappings,
   wipes the output files and appends the triples it generates to them.
3. Timing + triple count are logged.

JELLY note
----------
pyjelly cannot stream triples to an append-only file in parallel chunks; it
needs a complete ``rdflib.Graph`` serialized in one shot. The JELLY path
therefore materializes to a graph and serializes it via
``graph.serialize(format="jelly")``.
"""

import logging
import time

from .config                import load_from_cli
from .config.model          import MorphConfig
from .constants.misc        import LOGGING_NAMESPACE
from .constants.output      import JELLY
from .materializer.pipeline import materialize_pipeline
from .utils                 import create_dirs_in_path, get_delta_time

LOGGER = logging.getLogger(LOGGING_NAMESPACE)


# ── JELLY-specific helpers ────────────────────────────────────────────────────

def _assert_pyjelly_available() -> None:
    """Raise a helpful RuntimeError when pyjelly[rdflib] is not installed."""
    try:
        import pyjelly  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "JELLY output requested but pyjelly[rdflib] is not installed. "
            "Install it with:  pip install 'morph-kgc[jelly]'"
        ) from exc


def _run_jelly(config: MorphConfig) -> None:
    """Materialize into an rdflib Graph and serialize it as Jelly in one shot."""
    _assert_pyjelly_available()

    graph = materialize_pipeline(config, output="graph")

    output_path = config.get_output_file_path()
    create_dirs_in_path(output_path)
    graph.serialize(destination=output_path, format="jelly")

    LOGGER.info(f"Jelly file generated: {output_path}.")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    # load_from_cli() owns argparse: parses sys.argv, reads the INI file,
    # validates all fields, configures logging.
    config: MorphConfig = load_from_cli()

    start = time.time()

    if config.output_format == JELLY:
        _run_jelly(config)
    else:
        num_triples = materialize_pipeline(config, output="file")
        LOGGER.info(f"Number of triples generated in total: {num_triples}.")

    LOGGER.info(f"Materialization finished in {get_delta_time(start)} seconds.")


if __name__ == "__main__":
    main()
