from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Reconciliation functions
========================
Stateful built-in functions that map a value of the input data to the entity it
identifies in a controlled vocabulary or a knowledge graph — the reconciliation
step of an ETL pipeline that builds a knowledge graph.

Both functions are initialized once, before any triple is materialized: the
vocabulary is fetched (or the endpoint queried) a single time and the resulting
index is shared by every mapping rule and worker process. The index is kept on
disk, and the functions are vectorized: they look up all the values of a
mapping rule in one query, rather than one value at a time.

``reconcileVocabularyConcept`` reconciles against a SKOS vocabulary, which lives
in the configuration file, not in the mapping::

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
        ] ;
        rml:input [
            rml:parameter morph-fn:matching ;
            rml:inputValue "CASE-INSENSITIVE"
        ] .

``morph-fn:matching`` is ``EXACT`` (the default) or ``CASE-INSENSITIVE``, which
also normalizes Unicode (NFKC) and collapses whitespace. It is a constant: the
vocabulary is indexed before any data is read. Executions matching the same
vocabulary differently share a single download of it.

``reconcileEntityOverSPARQL`` reconciles against the entities of any knowledge
graph a SPARQL endpoint answers for. The mapping gives the endpoint and the
query, which projects ``?entity_iri`` and the ``?matching_value_1`` it is
matched by::

    <#ClinicalTrialReconciliation>
        rml:function morph-fn:reconcileEntityOverSPARQL ;
        rml:input [
            rml:parameter morph-fn:sparqlEndpointUrl ;
            rml:inputValue "https://example.org/sparql"
        ] ;
        rml:input [
            rml:parameter morph-fn:sparqlQuery ;
            rml:inputValue \"\"\"
                PREFIX schema: <https://schema.org/>
                SELECT ?entity_iri ?matching_value_1 WHERE {
                    GRAPH ?g {
                        ?entity_iri a schema:MedicalStudy ;
                                    schema:identifier ?matching_value_1 .
                    }
                }\"\"\"
        ] ;
        rml:input [
            rml:parameter grel:valueParam ;
            rml:inputValueMap [ rml:reference "clinical_trial_id" ]
        ] .

Its configuration file only declares the endpoints that need credentials or
other access options, in a ``[RESOURCE:<name>]`` section whose ``url`` is the
endpoint.

A function returns the IRI of the matched entity, ``None`` when the value
matches none (the triple is then not generated), and the list of matched IRIs
when a value is ambiguous. An object map using a function needs
``rml:termType rml:IRI`` (``rr:termType rr:IRI``, ``type: iri`` in YARRRML):
one whose value is the result of a function generates literals by default
(RML-FNML, section 5.1), while a subject map generates IRIs.
"""

import logging
from urllib.parse import urlsplit

from ..config.model import (
    ResourceConfig,
    SKOS_VOCABULARY_RESOURCE,
    SPARQL_ENDPOINT_RESOURCE,
)
from ..constants import (
    LOGGING_NAMESPACE,
    GREL_VALUE_PARAM,
    GREL_VALUE_PARAMETER,
    MORPH_ATTRIBUTE_PARAMETERS,
    MORPH_FN_MATCHING,
    MORPH_FN_SPARQL_ENDPOINT_URL,
    MORPH_FN_SPARQL_QUERY,
    MORPH_RECONCILE_ENTITY_OVER_SPARQL,
    MORPH_RECONCILE_VOCABULARY_CONCEPT,
    MORPH_RESOURCE_PARAMETERS,
)
from ..reconciliation import skos, sparql
from ..reconciliation.index import (
    EXACT_MATCHING,
    VALID_MATCHINGS,
    ReconciliationContext,
    parse_matching,
)
from .bif_decorator import stateful_bif
from .param_resolver import MergedAliases

LOGGER = logging.getLogger(LOGGING_NAMESPACE)


def _result(value, matches):
    """The IRI matched by *value*, the list of them when ambiguous, or None."""
    if not matches:
        LOGGER.debug(f"Value '{value}' was reconciled against no entity.")
        return None

    return matches[0] if len(matches) == 1 else matches


# ── SKOS vocabulary ───────────────────────────────────────────────────────────

def _matching(initialization, execution) -> str:
    """How *execution* matches values: the constant it binds, EXACT when none."""
    values = execution.constants.get(MORPH_FN_MATCHING, [])
    if len(values) > 1 or MORPH_FN_MATCHING in execution.bound_to_data:
        raise ValueError(
            f"Execution '{execution.execution_id}' of function "
            f"'{initialization.function_iri}' must bind '{MORPH_FN_MATCHING}' at "
            "most once, to a constant: the vocabulary is indexed before any data "
            "is read, so how values are matched against it cannot depend on the data."
        )
    if not values:
        return EXACT_MATCHING

    matching = parse_matching(values[0])
    if matching is None:
        raise ValueError(
            f"Execution '{execution.execution_id}' of function "
            f"'{initialization.function_iri}' binds '{MORPH_FN_MATCHING}' to "
            f"'{values[0]}', which is not valid. Must be one of: "
            f"{', '.join(sorted(VALID_MATCHINGS))}."
        )
    return matching


def _matched_attributes(initialization, resource) -> list:
    """
    How each execution of the function that may reconcile against *resource*
    matches values, and the attributes it matches against, as
    :func:`skos.build_index` takes them: the constant ones it binds, under any
    parameter IRI, ``()`` when it binds none, and ``None`` when it reads them
    from the data. Each execution is accounted for on its own, so that what one
    binds does not limit another.
    """
    matched = []
    for execution in initialization.executions:
        # An execution not naming its resource as a constant may use any.
        named = {
            initialization.resource(value).name
            for parameter_iri in MORPH_RESOURCE_PARAMETERS
            for value in execution.constants.get(parameter_iri, [])
        }
        if (
            named
            and resource.name not in named
            and not execution.bound_to_data.intersection(MORPH_RESOURCE_PARAMETERS)
        ):
            continue

        matching = _matching(initialization, execution)
        if execution.bound_to_data.intersection(MORPH_ATTRIBUTE_PARAMETERS):
            matched.append((matching, None))
        else:
            matched.append((matching, tuple(
                value
                for parameter_iri in MORPH_ATTRIBUTE_PARAMETERS
                for value in execution.constants.get(parameter_iri, [])
            )))

    return matched


def _initialize_vocabulary(initialization) -> ReconciliationContext:
    """
    Build the shared context of the vocabulary reconciliation function.

    Only the vocabularies the mapping reconciles against are fetched. When the
    mapping names none, every SKOS vocabulary declared in the configuration
    file is indexed, which lets a mapping with a single vocabulary omit the
    parameter altogether.
    """
    # A mistake in how an execution matches values is reported before any
    # vocabulary is downloaded.
    for execution in initialization.executions:
        _matching(initialization, execution)

    resources = initialization.resources(*MORPH_RESOURCE_PARAMETERS)
    if not resources:
        resources = initialization.config.get_resources_of_type(SKOS_VOCABULARY_RESOURCE)

    if not resources:
        raise ValueError(
            f"Function '{initialization.function_iri}' is used by the mapping "
            f"but no resource to reconcile against is declared. Add a "
            f"'[RESOURCE:<name>]' section with "
            f"'resource_type={SKOS_VOCABULARY_RESOURCE}' to the configuration file."
        )

    context = ReconciliationContext()
    for mapping_value, resource in resources.items():
        if resource.resource_type and resource.resource_type != SKOS_VOCABULARY_RESOURCE:
            raise ValueError(
                f"Function '{initialization.function_iri}' reconciles against "
                f"resource '{resource.name}', whose 'resource_type' is "
                f"'{resource.resource_type}' instead of '{SKOS_VOCABULARY_RESOURCE}'."
            )

        # Several mapping values (the resource name and its URL) may point at
        # the same resource; it is fetched once and reachable through all of them.
        if resource.name not in context.indexes:
            context.add(
                resource.name,
                skos.build_index(
                    resource,
                    _matched_attributes(initialization, resource),
                    state_dir    = initialization.state_dir,
                    memory_limit = initialization.memory_limit,
                ),
            )
        # A value of the mapping reaches the resource the configuration file
        # resolves it to, which the index was built for.
        context.alias(resource.name, mapping_value)

    # A resource is also reachable through its IRI and URL, unless the mapping
    # uses them for another resource or a resource declared before it in the
    # configuration file has them, which is how the configuration resolves them.
    # The IRI is expanded, unless it names an unset variable, but the URL is kept
    # as declared: expanded, it may hold a secret read from the environment (an
    # API key), and the context is written to disk.
    for resource in initialization.config.resources.values():
        if resource.name not in context.indexes:
            continue
        try:
            iri = resource.get_iri()
        except ValueError:
            iri = ""
        context.alias(resource.name, iri, resource.url, replace=False)

    return context


def _per_row(argument, rows: int) -> list:
    """The value of a vectorized *argument* for each row, when the mapping binds it."""
    return argument if isinstance(argument, list) else [argument] * rows


def _attributes(attribute) -> tuple:
    """
    The attributes a row matches against: a parameter bound several times
    (e.g. skos:prefLabel and skos:altLabel), under any of its IRIs, arrives as
    a list, and the value is matched against all of them.
    """
    if attribute is None:
        return ()
    if isinstance(attribute, (list, tuple)):
        return tuple(attribute)
    return (attribute,)


def _reconcile_rows(values, groups, lookup) -> list:
    """
    Reconcile *values*, the rows of each group of *groups* (key -> row
    numbers) with a single call to ``lookup(key, values)``.
    """
    results = [None] * len(values)
    for key, rows in groups.items():
        matches = lookup(key, [values[row] for row in rows])
        for row, row_matches in zip(rows, matches):
            results[row] = _result(values[row], row_matches)
    return results


@stateful_bif(
    fun_id      = MORPH_RECONCILE_VOCABULARY_CONCEPT,
    initializer = _initialize_vocabulary,
    vectorized  = True,
    value       = (GREL_VALUE_PARAM, GREL_VALUE_PARAMETER),
    resource    = MORPH_RESOURCE_PARAMETERS,
    attribute   = MergedAliases(MORPH_ATTRIBUTE_PARAMETERS),
    matching    = MORPH_FN_MATCHING,
)
def reconcile_vocabulary_concept(context, value, resource=None, attribute=None, matching=None):
    """Reconcile each of *value* against a SKOS vocabulary fetched from a URL."""
    resources  = _per_row(resource, len(value))
    attributes = _per_row(attribute, len(value))
    # A single constant, which the initializer has checked, in any case.
    matchings  = _per_row(EXACT_MATCHING if matching is None else matching, len(value))

    # The rows reconciled against the same resource and attributes, in the same
    # way, which are usually all of them, are looked up together.
    groups: dict[tuple, list[int]] = {}
    for row, (row_resource, row_attribute, row_matching) in enumerate(
        zip(resources, attributes, matchings)
    ):
        key = (row_resource, _attributes(row_attribute), row_matching)
        groups.setdefault(key, []).append(row)

    def lookup(key, values):
        row_resource, row_attributes, row_matching = key
        index = context.get(row_resource, parse_matching(row_matching))
        return index.lookup_many(values, row_attributes)

    return _reconcile_rows(value, groups, lookup)


# ── SPARQL endpoint ───────────────────────────────────────────────────────────

def _constant(initialization, execution, parameter_iri) -> str:
    """The constant value *execution* binds to *parameter_iri*."""
    values = execution.constants.get(parameter_iri, [])
    if len(values) != 1 or parameter_iri in execution.bound_to_data:
        raise ValueError(
            f"Execution '{execution.execution_id}' of function "
            f"'{initialization.function_iri}' must bind '{parameter_iri}' once, "
            "to a constant: the SPARQL endpoint and the query are sent before "
            "any data is read, so they cannot depend on it."
        )
    return values[0]


def _endpoint_resource(initialization, endpoint: str) -> ResourceConfig:
    """
    The SPARQL endpoint at the URL *endpoint*: the resource of the configuration
    file declaring it, by its 'url' or its 'iri', so that its credentials and
    access options are used, or else the URL itself, accessed with defaults.
    """
    if urlsplit(endpoint).username is not None:
        raise ValueError(
            f"The SPARQL endpoint URL of function '{initialization.function_iri}' "
            "holds credentials. Keep them out of the mapping: declare them as "
            "'username' and 'password' in the configuration file, in a "
            "'[RESOURCE:<name>]' section with 'resource_type=SPARQL_ENDPOINT' "
            "and the URL as 'url'."
        )

    declared = initialization.config.get_resources_of_type(SPARQL_ENDPOINT_RESOURCE)
    matches = []
    # Errors of identifiers naming an unset environment variable, which may
    # well name this endpoint.
    unexpanded = []
    for resource in declared.values():
        if endpoint in resource.identifiers(unexpanded):
            matches.append(resource)

    if len(matches) > 1:
        raise ValueError(
            f"The SPARQL endpoint '{endpoint}' is declared by several resources: "
            f"{', '.join(sorted(resource.name for resource in matches))}."
        )
    if not matches:
        # A URL that differs from the declared one, even by a trailing slash,
        # is accessed without the credentials declared for it.
        LOGGER.info(
            f"SPARQL endpoint '{endpoint}' is declared by no resource, so it is "
            "accessed without credentials."
        )
        for error in unexpanded:
            LOGGER.warning(
                f"{error} The resource may declare SPARQL endpoint '{endpoint}', "
                "which is then accessed without its credentials."
            )
        return ResourceConfig(
            name          = endpoint,
            resource_type = SPARQL_ENDPOINT_RESOURCE,
            url           = endpoint,
        )

    resource = matches[0]
    if not resource.url:
        raise ValueError(
            f"Resource '{resource.name}' does not declare the 'url' of the SPARQL "
            "endpoint to reconcile against."
        )

    LOGGER.info(f"SPARQL endpoint '{endpoint}' is accessed as resource '{resource.name}'.")
    return resource


def _initialize_sparql(initialization) -> dict:
    """
    Build the shared context of the SPARQL reconciliation function: the index of
    every query of the mapping, by the endpoint it is sent to and the query.

    Each execution of the function pairs a query with its endpoint, so a mapping
    may reconcile against several knowledge graphs. A query sent to the same
    endpoint by several executions is sent once.
    """
    indexes = {}
    for execution in initialization.executions:
        endpoint = _constant(initialization, execution, MORPH_FN_SPARQL_ENDPOINT_URL)
        query    = _constant(initialization, execution, MORPH_FN_SPARQL_QUERY)

        if (endpoint, query) not in indexes:
            resource = _endpoint_resource(initialization, endpoint)
            indexes[(endpoint, query)] = sparql.build_index(
                resource,
                query,
                state_dir    = initialization.state_dir,
                memory_limit = initialization.memory_limit,
            )

    return indexes


@stateful_bif(
    fun_id      = MORPH_RECONCILE_ENTITY_OVER_SPARQL,
    initializer = _initialize_sparql,
    vectorized  = True,
    value       = (GREL_VALUE_PARAM, GREL_VALUE_PARAMETER),
    endpoint    = MORPH_FN_SPARQL_ENDPOINT_URL,
    query       = MORPH_FN_SPARQL_QUERY,
)
def reconcile_entity_over_sparql(context, value, endpoint=None, query=None):
    """Reconcile each of *value* against the entities a SPARQL query returns."""
    # Every execution bound a single constant endpoint and query, which the
    # initializer has indexed.
    endpoints = _per_row(endpoint, len(value))
    queries   = _per_row(query, len(value))

    groups: dict[tuple, list[int]] = {}
    for row, key in enumerate(zip(endpoints, queries)):
        groups.setdefault(key, []).append(row)

    return _reconcile_rows(value, groups, lambda key, values: context[key].lookup_many(values))
