__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
the dictionary of specific functions. The IRIs that appear as values in the slots those predicates define:
urn:morph:function:reconciliation:reconcileVocabularyConcept is what sits at the end of rml:functionMap/rml:constant;
urn:morph:function:attributeIRI is what sits at the end of rml:parameterMap/rml:constant. 
It imports nothing — it's leaf data. Consumed by code that implements functions (functions/reconciliation.py).

Namespaces
----------
``urn:morph:function:``                 parameters shared by Morph-KGC functions
``urn:morph:function:reconciliation:``  reconciliation functions

The GREL parameter IRIs are listed here as accepted aliases so that mappings
written against the GREL vocabulary keep working.
"""

# ── Namespaces ────────────────────────────────────────────────────────────────
MORPH_FUNCTION_NAMESPACE       = "urn:morph:function:"
MORPH_RECONCILIATION_NAMESPACE = "urn:morph:function:reconciliation:"
GREL_NAMESPACE                 = "http://users.ugent.be/~bjdmeest/function/grel.ttl#"

# ── Reconciliation functions ─────────────────────────────────────────────────
MORPH_RECONCILE_VOCABULARY_CONCEPT = f"{MORPH_RECONCILIATION_NAMESPACE}reconcileVocabularyConcept"
MORPH_RECONCILE_SPARQL_CONCEPT     = f"{MORPH_RECONCILIATION_NAMESPACE}reconcileSPARQLConcept"

# ── Parameters ────────────────────────────────────────────────────────────────
# Name of the [RESOURCE:<name>] config section holding the accessed resource.
MORPH_FN_RESOURCE       = f"{MORPH_FUNCTION_NAMESPACE}resource"
# Accepted aliases of morph-fn:resource. They also accept the URL of a declared
# resource, so mappings that name the vocabulary/endpoint directly keep working
# as long as a matching [RESOURCE:<name>] section is declared in the config.
MORPH_FN_VOCABULARY_IRI = f"{MORPH_FUNCTION_NAMESPACE}vocabularyIRI"
MORPH_FN_ENDPOINT_IRI   = f"{MORPH_FUNCTION_NAMESPACE}endpointIRI"
# Vocabulary property (or properties) the value is matched against.
MORPH_FN_ATTRIBUTE_IRI  = f"{MORPH_FUNCTION_NAMESPACE}attributeIRI"
GREL_ATTRIBUTE_IRI      = f"{GREL_NAMESPACE}attributeIRI"
# Value to reconcile.
GREL_VALUE_PARAM        = f"{GREL_NAMESPACE}valueParam"
GREL_VALUE_PARAMETER    = f"{GREL_NAMESPACE}valueParameter"

# Parameter IRIs (in precedence order) that identify the accessed resource.
MORPH_RESOURCE_PARAMETERS = (
    MORPH_FN_RESOURCE,
    MORPH_FN_VOCABULARY_IRI,
    MORPH_FN_ENDPOINT_IRI,
)
# Parameter IRIs (in precedence order) that carry the matched attributes.
MORPH_ATTRIBUTE_PARAMETERS = (
    MORPH_FN_ATTRIBUTE_IRI,
    GREL_ATTRIBUTE_IRI,
)

# ── SKOS terms used by the default reconciliation index ──────────────────────
SKOS_NAMESPACE     = "http://www.w3.org/2004/02/skos/core#"
SKOS_PREF_LABEL    = f"{SKOS_NAMESPACE}prefLabel"
SKOS_ALT_LABEL     = f"{SKOS_NAMESPACE}altLabel"
SKOS_HIDDEN_LABEL  = f"{SKOS_NAMESPACE}hiddenLabel"
SKOS_NOTATION      = f"{SKOS_NAMESPACE}notation"

# Attributes indexed when the mapping does not declare any.
DEFAULT_RECONCILIATION_ATTRIBUTES = (
    SKOS_PREF_LABEL,
    SKOS_ALT_LABEL,
    SKOS_HIDDEN_LABEL,
    SKOS_NOTATION,
)
