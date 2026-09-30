from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Reconciliation functions
========================
Stateful built-in functions that map a value of the input data to the concept
it identifies in a controlled vocabulary — the reconciliation step of an ETL
pipeline that builds a knowledge graph.

Both functions are initialized once, before any triple is materialized: the
vocabulary is fetched (or the endpoint queried) a single time and the resulting
index is shared by every mapping rule and worker process.

The accessed resource lives in the configuration file, not in the mapping::

    [RESOURCE:disease_vocabulary]
    resource_type=SKOS_VOCABULARY
    url=https://example.org/vocabulary/disease
    username={VOCABULARY_USER}
    password={VOCABULARY_PASSWORD}

    <#DiseaseReconciliation>
        rml:function morph-fr:reconcileVocabularyConcept ;
        rml:input [
            rml:parameter morph-fn:resource ;
            rml:inputValue "disease_vocabulary"
        ] ;
        rml:input [
            rml:parameter grel:valueParam ;
            rml:inputValueMap [ rml:reference "disease_label" ]
        ] ;
        rml:input [
            rml:parameter morph-fn:attributeIRI ;
            rml:inputValue skos:prefLabel, skos:altLabel
        ] .

A function returns the IRI of the matched concept, ``None`` when the value
matches no concept (the triple is then not generated), and the list of matched
concept IRIs when a value is ambiguous.
"""

import logging

from ..config.model import SKOS_VOCABULARY_RESOURCE, SPARQL_ENDPOINT_RESOURCE
from ..constants import (
    LOGGING_NAMESPACE,
    GREL_VALUE_PARAM,
    GREL_VALUE_PARAMETER,
    MORPH_ATTRIBUTE_PARAMETERS,
    MORPH_RECONCILE_SPARQL_CONCEPT,
    MORPH_RECONCILE_VOCABULARY_CONCEPT,
    MORPH_RESOURCE_PARAMETERS,
)
from ..reconciliation import skos, sparql
from ..reconciliation.index import ReconciliationContext
from .bif_decorator import stateful_bif

LOGGER = logging.getLogger(LOGGING_NAMESPACE)


# ── Context initialization ────────────────────────────────────────────────────

def _build_context(initialization, resource_type, build_index) -> ReconciliationContext:
    """
    Build the shared context of a reconciliation function.

    Only the resources the mapping reconciles against are accessed. When the
    mapping names none, every resource of *resource_type* declared in the
    configuration file is indexed, which lets a mapping with a single resource
    omit the parameter altogether.
    """
    resources = initialization.resources(*MORPH_RESOURCE_PARAMETERS)
    if not resources:
        resources = initialization.config.get_resources_of_type(resource_type)

    if not resources:
        raise ValueError(
            f"Function '{initialization.function_iri}' is used by the mapping "
            f"but no resource to reconcile against is declared. Add a "
            f"'[RESOURCE:<name>]' section with 'resource_type={resource_type}' "
            "to the configuration file."
        )

    attributes = initialization.constants(*MORPH_ATTRIBUTE_PARAMETERS)

    context = ReconciliationContext()
    for mapping_value, resource in resources.items():
        if resource.resource_type and resource.resource_type != resource_type:
            raise ValueError(
                f"Function '{initialization.function_iri}' reconciles against "
                f"resource '{resource.name}', whose 'resource_type' is "
                f"'{resource.resource_type}' instead of '{resource_type}'."
            )

        # Several mapping values (the resource name and its URL) may point at
        # the same resource; it is fetched once and reachable through all of them.
        if resource.name not in context.indexes:
            context.add(resource.name, build_index(resource, attributes))
        context.alias(resource.name, mapping_value, *resource.identifiers())

    return context


def _initialize_vocabulary(initialization) -> ReconciliationContext:
    return _build_context(
        initialization, SKOS_VOCABULARY_RESOURCE, skos.build_index
    )


def _initialize_sparql(initialization) -> ReconciliationContext:
    return _build_context(
        initialization, SPARQL_ENDPOINT_RESOURCE, sparql.build_index
    )


# ── Reconciliation ────────────────────────────────────────────────────────────

def _reconcile(context, value, resource, attribute):
    """Look *value* up in the index of *resource* and return the concept(s)."""
    index = context.get(resource)

    # A parameter bound several times (e.g. skos:prefLabel and skos:altLabel)
    # arrives as a list; the value is matched against all of them.
    if attribute is None:
        attributes = ()
    elif isinstance(attribute, (list, tuple)):
        attributes = tuple(attribute)
    else:
        attributes = (attribute,)

    concepts = index.lookup(value, attributes)

    if not concepts:
        LOGGER.debug(f"Value '{value}' was reconciled against no concept.")
        return None

    return concepts[0] if len(concepts) == 1 else concepts


@stateful_bif(
    fun_id      = MORPH_RECONCILE_VOCABULARY_CONCEPT,
    initializer = _initialize_vocabulary,
    value       = (GREL_VALUE_PARAM, GREL_VALUE_PARAMETER),
    resource    = MORPH_RESOURCE_PARAMETERS,
    attribute   = MORPH_ATTRIBUTE_PARAMETERS,
)
def reconcile_vocabulary_concept(context, value, resource=None, attribute=None):
    """Reconcile *value* against a SKOS vocabulary fetched from a URL."""
    return _reconcile(context, value, resource, attribute)


@stateful_bif(
    fun_id      = MORPH_RECONCILE_SPARQL_CONCEPT,
    initializer = _initialize_sparql,
    value       = (GREL_VALUE_PARAM, GREL_VALUE_PARAMETER),
    resource    = MORPH_RESOURCE_PARAMETERS,
    attribute   = MORPH_ATTRIBUTE_PARAMETERS,
)
def reconcile_sparql_concept(context, value, resource=None, attribute=None):
    """Reconcile *value* against the concepts held by a SPARQL endpoint."""
    return _reconcile(context, value, resource, attribute)
