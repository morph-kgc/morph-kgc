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
# How the value is matched against the vocabulary or the answer to a SPARQL
# query: EXACT (the default), LOWERCASE or CASE-INSENSITIVE. A constant: what
# the value is matched against is indexed before any data is read.
MORPH_FN_MATCHING       = f"{MORPH_FUNCTION_NAMESPACE}matching"
# What a value matching several entities is reconciled to: ALL of them (the
# default), NONE, or FAIL to abort the run. A constant; the 'ambiguity' option
# of a [RESOURCE:<name>] section sets the default for the resource.
MORPH_FN_AMBIGUITY      = f"{MORPH_FUNCTION_NAMESPACE}ambiguity"
# URL of the SPARQL endpoint a query is sent to.
MORPH_FN_SPARQL_ENDPOINT_URL = f"{MORPH_FUNCTION_NAMESPACE}sparqlEndpointUrl"
# SELECT query the entities and the values they are matched by are read with.
MORPH_FN_SPARQL_QUERY        = f"{MORPH_FUNCTION_NAMESPACE}sparqlQuery"
# Value to reconcile.
GREL_VALUE_PARAM        = f"{GREL_NAMESPACE}valueParam"
GREL_VALUE_PARAMETER    = f"{GREL_NAMESPACE}valueParameter"

# The fno:predicate of the GREL fno:Parameters (grel.ttl). A parameter map names
# the fno:Parameter (grel:valueParam), as the RML-FNML test cases of the W3C
# KG-Construct CG do, or its predicate (grel:valueParameter), which the built-in
# functions are declared with; both bind the same argument.
GREL_PARAMETER_PREDICATES = {
    f"{GREL_NAMESPACE}bool_array_b": f"{GREL_NAMESPACE}param_rep_b",
    f"{GREL_NAMESPACE}modeParam": f"{GREL_NAMESPACE}modeParameter",
    f"{GREL_NAMESPACE}p_int_i_from": f"{GREL_NAMESPACE}param_int_i_from",
    f"{GREL_NAMESPACE}p_int_i_opt_to": f"{GREL_NAMESPACE}param_int_i_opt_to",
    f"{GREL_NAMESPACE}param_a": f"{GREL_NAMESPACE}p_array_a",
    f"{GREL_NAMESPACE}param_any_e": f"{GREL_NAMESPACE}p_any_e",
    f"{GREL_NAMESPACE}param_any_rep_e": f"{GREL_NAMESPACE}p_any_rep_e",
    f"{GREL_NAMESPACE}param_b": f"{GREL_NAMESPACE}bool_b",
    f"{GREL_NAMESPACE}param_bool_opt_b": f"{GREL_NAMESPACE}p_bool_opt_b",
    f"{GREL_NAMESPACE}param_date_d": f"{GREL_NAMESPACE}p_date_d",
    f"{GREL_NAMESPACE}param_datetime_d": f"{GREL_NAMESPACE}p_datetime_d",
    f"{GREL_NAMESPACE}param_datetime_d2": f"{GREL_NAMESPACE}p_datetime_d2",
    f"{GREL_NAMESPACE}param_dec_n": f"{GREL_NAMESPACE}p_dec_n",
    f"{GREL_NAMESPACE}param_dec_n2": f"{GREL_NAMESPACE}param_n2",
    f"{GREL_NAMESPACE}param_dec_n_exp": f"{GREL_NAMESPACE}p_any_exp",
    f"{GREL_NAMESPACE}param_false": f"{GREL_NAMESPACE}any_false",
    f"{GREL_NAMESPACE}param_find": f"{GREL_NAMESPACE}p_string_find",
    f"{GREL_NAMESPACE}param_fragment": f"{GREL_NAMESPACE}p_fragment",
    f"{GREL_NAMESPACE}param_int_i": f"{GREL_NAMESPACE}p_int_i",
    f"{GREL_NAMESPACE}param_int_i2": f"{GREL_NAMESPACE}p_int_i2",
    f"{GREL_NAMESPACE}param_int_rep_i": f"{GREL_NAMESPACE}p_rep_i",
    f"{GREL_NAMESPACE}param_opt_sep": f"{GREL_NAMESPACE}p_opt_sep",
    f"{GREL_NAMESPACE}param_regex": f"{GREL_NAMESPACE}p_string_regex",
    f"{GREL_NAMESPACE}param_replace": f"{GREL_NAMESPACE}p_string_replace",
    f"{GREL_NAMESPACE}param_string_encoder": f"{GREL_NAMESPACE}p_string_encoder",
    f"{GREL_NAMESPACE}param_string_encoding": f"{GREL_NAMESPACE}p_string_encoding",
    f"{GREL_NAMESPACE}param_string_field": f"{GREL_NAMESPACE}p_string_field",
    f"{GREL_NAMESPACE}param_string_pattern": f"{GREL_NAMESPACE}p_string_pattern",
    f"{GREL_NAMESPACE}param_string_sep": f"{GREL_NAMESPACE}p_string_sep",
    f"{GREL_NAMESPACE}param_string_sub": f"{GREL_NAMESPACE}string_sub",
    f"{GREL_NAMESPACE}param_string_timeunit": f"{GREL_NAMESPACE}p_string_timeunit",
    f"{GREL_NAMESPACE}param_string_unit": f"{GREL_NAMESPACE}p_string_unit",
    f"{GREL_NAMESPACE}param_true": f"{GREL_NAMESPACE}any_true",
    f"{GREL_NAMESPACE}valueParam": f"{GREL_NAMESPACE}valueParameter",
    f"{GREL_NAMESPACE}valueParam2": f"{GREL_NAMESPACE}valueParameter2",
}

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
