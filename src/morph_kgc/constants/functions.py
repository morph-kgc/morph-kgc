__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
the dictionary of specific functions. The IRIs that appear as values in the slots those predicates define:
urn:morph:function:reconciliation:reconcileVocabularyConcept is what sits at the end of rml:functionMap/rml:constant;
urn:morph:function:attributeIRI is what sits at the end of rml:parameterMap/rml:constant. 
It imports nothing — it's leaf data. Consumed by code that implements functions (functions/reconciliation.py).

Namespaces
----------
``urn:morph:function:``                 parameters shared by Morph-KGC functions, and
                                        the SPARQL reconciliation function, named
                                        as its feature request does
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
# https://github.com/morph-kgc/morph-kgc/discussions/339
MORPH_RECONCILE_ENTITY_OVER_SPARQL = f"{MORPH_FUNCTION_NAMESPACE}reconcileEntityOverSPARQL"

# ── Parameters ────────────────────────────────────────────────────────────────
# Name of the [RESOURCE:<name>] config section holding the accessed resource.
MORPH_FN_RESOURCE       = f"{MORPH_FUNCTION_NAMESPACE}resource"
# Accepted alias of morph-fn:resource. It also accepts the URL of a declared
# resource, so mappings that name the vocabulary directly keep working as long
# as a matching [RESOURCE:<name>] section is declared in the config.
MORPH_FN_VOCABULARY_IRI = f"{MORPH_FUNCTION_NAMESPACE}vocabularyIRI"
# Vocabulary property (or properties) the value is matched against.
MORPH_FN_ATTRIBUTE_IRI  = f"{MORPH_FUNCTION_NAMESPACE}attributeIRI"
GREL_ATTRIBUTE_IRI      = f"{GREL_NAMESPACE}attributeIRI"
# How the value is matched against the vocabulary: EXACT (the default) or
# CASE-INSENSITIVE. A constant: the vocabulary is indexed before any data is read.
MORPH_FN_MATCHING       = f"{MORPH_FUNCTION_NAMESPACE}matching"
# URL of the SPARQL endpoint a query is sent to.
MORPH_FN_SPARQL_ENDPOINT_URL = f"{MORPH_FUNCTION_NAMESPACE}sparqlEndpointUrl"
# SELECT query the entities and the values they are matched by are read with.
MORPH_FN_SPARQL_QUERY        = f"{MORPH_FUNCTION_NAMESPACE}sparqlQuery"
# Value to reconcile.
GREL_VALUE_PARAM        = f"{GREL_NAMESPACE}valueParam"
GREL_VALUE_PARAMETER    = f"{GREL_NAMESPACE}valueParameter"

# Parameter IRIs (in precedence order) that identify the accessed resource.
MORPH_RESOURCE_PARAMETERS = (
    MORPH_FN_RESOURCE,
    MORPH_FN_VOCABULARY_IRI,
)
# Parameter IRIs (in precedence order) that carry the matched attributes.
MORPH_ATTRIBUTE_PARAMETERS = (
    MORPH_FN_ATTRIBUTE_IRI,
    GREL_ATTRIBUTE_IRI,
)
