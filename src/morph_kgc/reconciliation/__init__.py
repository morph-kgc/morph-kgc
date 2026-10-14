__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Reconciliation
==============
Mapping a value of the input data to the entity it identifies in a controlled
vocabulary or a knowledge graph: fetching the SKOS vocabulary declared by a
``[RESOURCE:<name>]`` config section, or sending the SPARQL query of the mapping
to its endpoint, and indexing the answer into the shared context the
reconciliation functions are initialized with.

The functions themselves are declared in ``functions/reconciliation.py``, next
to the other functions the engine can execute from a mapping.
"""

from .index import ConceptIndex, ReconciliationContext
