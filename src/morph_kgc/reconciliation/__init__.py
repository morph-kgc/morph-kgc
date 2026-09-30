__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Reconciliation
==============
Mapping a value of the input data to the concept it identifies in a controlled
vocabulary: fetching the vocabulary (or querying the endpoint) declared by a
``[RESOURCE:<name>]`` config section, and indexing it into the shared context
the reconciliation functions are initialized with.

The functions themselves are declared in ``functions/reconciliation.py``, next
to the other functions the engine can execute from a mapping.
"""

from .index import ConceptIndex, ReconciliationContext
