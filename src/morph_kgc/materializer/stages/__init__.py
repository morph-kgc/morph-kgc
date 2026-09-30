__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
materializer.stages
===================
Pipeline stages — each module owns exactly one transformation step.

Stage 1  load.py        load_data()          fetch + preprocess source data
Stage 2  references.py  collect_references() determine required source columns
Stage 3  terms.py       materialize_terms()  serialize RDF terms into DataFrame
Stage 4  join.py        merge_data()         inner-join child/parent DataFrames
Stage 5  assemble.py    assemble_triples()   combine S/P/O into triple strings
"""

from .load import load_data
from .references import collect_references, join_pairs
from .terms import materialize_terms
from .join import merge_data
from .assemble import assemble_triples

__all__ = [
    "load_data",
    "collect_references",
    "join_pairs",
    "materialize_terms",
    "merge_data",
    "assemble_triples",
]
