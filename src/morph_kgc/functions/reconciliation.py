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

``morph-fn:matching`` says how values are compared:

- ``EXACT`` (the default): as they are.
- ``LOWERCASE``: lower-cased by ``str.lower()``, and nothing else: no
  whitespace is trimmed or collapsed, and no Unicode normalization is applied,
  so that ``Café `` matches ``café `` but not ``Café`` or ``cafe``, and
  ``Straße`` does not match ``strasse``.
- ``CASE-INSENSITIVE``: normalized (NFKC), with their whitespace collapsed, and
  case-folded, so that ``Straße`` matches ``strasse``.

It is a constant: what values are matched against is indexed before any data
is read. Executions matching the same vocabulary differently share a single
download of it. Both functions match values in the same ways, normalizing them
themselves, both those they are matched against and those they reconcile: the
same data, mapping and way of matching values give the same triples whether a
vocabulary is read from a file or queried from a SPARQL endpoint holding it,
the query returning its labels as they are.

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

An entity only several values identify together, such as a code within a
coding system, is matched by all of them: the query projects
``?matching_value_2`` and so on, and the execution binds as many values,
matched to the variables by position: ``grel:valueParam1`` (or
``grel:valueParam``), ``grel:valueParam2``... The values are compared one by
one, never joined into a string.

A function returns the IRI of the matched entity, and ``None`` when the value
matches none (the triple is then not generated). A value matching several
entities is ambiguous, and ``morph-fn:ambiguity`` says what it is reconciled to:

- ``ALL`` (the default): the list of the matched IRIs, which generates one
  triple per entity.
- ``NONE``: ``None``, so that no triple is generated for it.
- ``FAIL``: nothing, as the run is aborted.

Like ``morph-fn:matching``, it is a constant, which a function checks when it
is initialized, before it downloads any vocabulary or sends any query. The
``ambiguity`` option of the
``[RESOURCE:<name>]`` section of a vocabulary or an endpoint sets the policy of
the executions that do not choose one::

    [RESOURCE:disease_vocabulary]
    resource_type=SKOS_VOCABULARY
    url=https://example.org/vocabulary/disease
    ambiguity=NONE

An ambiguous value is reported in a log record starting with ``MULTIPLE_MATCH``
that gives the value and the IRIs of every entity it matches: a warning with
``NONE``, as no triple is generated for it, and a debug record with ``ALL``. A
process writes each record once, however many rows and mapping rules hold the
value. With ``FAIL``, the report is the message of the error aborting the run.

Both functions report how many values they matched to one entity, to none and
to several, logged as a summary of each execution, and record each distinct
value matched to none or to several in the report the 'reconciliation_report'
option names (see :mod:`morph_kgc.functions.outcomes`).

An object map using a function needs
``rml:termType rml:IRI`` (``rr:termType rr:IRI``, ``type: iri`` in YARRRML):
one whose value is the result of a function generates literals by default
(RML-FNML, section 5.1), while a subject map generates IRIs.
"""

import logging
from hashlib import sha256
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
    MORPH_FN_AMBIGUITY,
    MORPH_FN_MATCHING,
    MORPH_FN_SPARQL_ENDPOINT_URL,
    MORPH_FN_SPARQL_QUERY,
    MORPH_RECONCILE_ENTITY_OVER_SPARQL,
    MORPH_RECONCILE_VOCABULARY_CONCEPT,
    MORPH_RESOURCE_PARAMETERS,
)
from ..reconciliation import skos, sparql
from ..reconciliation.index import (
    AMBIGUITY_ALL,
    AMBIGUITY_FAIL,
    AMBIGUITY_NONE,
    AMBIGUITY_OPTION,
    EXACT_MATCHING,
    VALID_AMBIGUITIES,
    VALID_MATCHINGS,
    QueryContext,
    ReconciliationContext,
    normalize,
    parse_ambiguity,
    parse_matching,
)
from .bif_decorator import stateful_bif
from .param_resolver import MergedAliases, NumberedParameter

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

# The values to reconcile, matched by position to the values an entity is
# matched by: grel:valueParam1 (or grel:valueParam), grel:valueParam2... under
# either GREL spelling.
VALUE_PARAMETERS = NumberedParameter((GREL_VALUE_PARAM, GREL_VALUE_PARAMETER))

# How a reconciled value is reported: one matching no entity, and one matching
# several.
NO_MATCH = "NO_MATCH"
MULTIPLE_MATCH = "MULTIPLE_MATCH"

# What becomes of an ambiguous value under each policy.
_AMBIGUITY_OUTCOMES = {
    AMBIGUITY_ALL:  "A triple is generated for each of them",
    AMBIGUITY_NONE: "No triple is generated for it",
    AMBIGUITY_FAIL: "The run is aborted",
}


def _result(context, target, ambiguity, value, matches):
    """
    What *value* is reconciled to, given the IRIs of the entities of *target*
    it *matches*: the one it matches, None when it matches none, and, when it
    matches several, as the *ambiguity* policy says: all of them, None, or an
    error aborting the run.
    """
    if not matches:
        LOGGER.debug(f"{NO_MATCH}: value {value!r} matches no entity of {target}.")
        return None
    if len(matches) == 1:
        return matches[0]

    if ambiguity == AMBIGUITY_ALL and not LOGGER.isEnabledFor(logging.DEBUG):
        # Reported in no record, so that the default policy costs nothing.
        return matches

    report = (
        f"{MULTIPLE_MATCH}: value {value!r} matches {len(matches)} entities of "
        f"{target}: {', '.join(f'<{match}>' for match in matches)}. "
        f"{_AMBIGUITY_OUTCOMES[ambiguity]}, as the ambiguity policy is {ambiguity}."
    )
    if ambiguity == AMBIGUITY_FAIL:
        raise ValueError(
            f"{report} To reconcile it to all the entities it matches, or to none, "
            f"set the policy to {AMBIGUITY_ALL} or {AMBIGUITY_NONE}: "
            f"'{MORPH_FN_AMBIGUITY}' in the execution, or else the "
            f"'{AMBIGUITY_OPTION}' option of the resource."
        )

    # Each record is written once by each process, however many rows and
    # mapping rules hold the value.
    if report not in context.reported:
        context.reported.add(report)
        # A value without a triple is a gap in the knowledge graph.
        LOGGER.log(logging.DEBUG if ambiguity == AMBIGUITY_ALL else logging.WARNING, report)

    return matches if ambiguity == AMBIGUITY_ALL else None


def _optional_constant(initialization, execution, parameter_iri, reason):
    """
    The constant *execution* binds to *parameter_iri*, None when it binds none.
    It may bind it at most once, to a constant, for *reason*.
    """
    values = execution.constants.get(parameter_iri, [])
    if len(values) > 1 or parameter_iri in execution.bound_to_data:
        raise ValueError(
            f"Execution '{execution.execution_id}' of function "
            f"'{initialization.function_iri}' must bind '{parameter_iri}' at "
            f"most once, to a constant: {reason}"
        )
    return values[0] if values else None


def _invalid_constant(initialization, execution, parameter_iri, value, valid):
    """The error of *execution* binding *parameter_iri* to an invalid *value*."""
    return ValueError(
        f"Execution '{execution.execution_id}' of function "
        f"'{initialization.function_iri}' binds '{parameter_iri}' to "
        f"'{value}', which is not valid. Must be one of: {', '.join(sorted(valid))}."
    )


def _ambiguity(initialization, execution) -> str | None:
    """
    The policy *execution* chooses for ambiguous values: the constant it binds,
    None when it binds none, and the resource's then applies.
    """
    value = _optional_constant(
        initialization, execution, MORPH_FN_AMBIGUITY,
        "what an ambiguous value is reconciled to is checked before any data "
        "is read, so it cannot depend on the data.",
    )
    if value is None:
        return None

    ambiguity = parse_ambiguity(value)
    if ambiguity is None:
        raise _invalid_constant(
            initialization, execution, MORPH_FN_AMBIGUITY, value, VALID_AMBIGUITIES,
        )
    return ambiguity


def _resource_ambiguity(initialization, resource) -> str:
    """
    The policy of *resource* for ambiguous values: its 'ambiguity' option, ALL
    when it declares none.
    """
    value = resource.get(AMBIGUITY_OPTION)
    if not value:
        return AMBIGUITY_ALL

    ambiguity = parse_ambiguity(value)
    if ambiguity is None:
        raise ValueError(
            f"Option '{AMBIGUITY_OPTION}' of resource '{resource.name}', which "
            f"function '{initialization.function_iri}' reconciles against, is "
            f"'{value}', which is not valid. Must be one of: "
            f"{', '.join(sorted(VALID_AMBIGUITIES))}."
        )
    return ambiguity


def _value_count(initialization, execution) -> int:
    """
    The number of values *execution* reconciles, which it binds, by position,
    to the value parameters: each exactly once, numbered from 1 without gaps.
    """
    positions = {
        position: [
            parameter_iri
            for parameter_iri in parameter_iris
            for _ in range(execution.bindings.get(parameter_iri, 0))
        ]
        for position, parameter_iris in VALUE_PARAMETERS.bound_positions(execution.bindings).items()
    }
    execution_name = (
        f"Execution '{execution.execution_id}' of function '{initialization.function_iri}'"
    )
    if not positions:
        raise ValueError(
            f"{execution_name} binds no value to reconcile. Bind it to "
            f"'{GREL_VALUE_PARAM}'."
        )

    for position, parameter_iris in sorted(positions.items()):
        if len(parameter_iris) > 1:
            raise ValueError(
                f"{execution_name} binds value {position} to reconcile "
                f"{len(parameter_iris)} times, to "
                f"{', '.join(repr(iri) for iri in sorted(set(parameter_iris)))}. Bind it once: the "
                "values to reconcile are told apart by their position, as RDF "
                "puts no order on the inputs of an execution."
            )

    count = max(positions)
    if len(positions) != count:
        gap = next(position for position in range(1, count + 1) if position not in positions)
        raise ValueError(
            f"{execution_name} binds value {count} to reconcile but not value "
            f"{gap}. The values are numbered from 1, without gaps: "
            f"'{GREL_VALUE_PARAM}1' (or '{GREL_VALUE_PARAM}'), '{GREL_VALUE_PARAM}2', "
            "and so on."
        )
    return count


def _row_ambiguity(ambiguity, default) -> str:
    """
    The policy for the ambiguous values of a row: the one the execution
    chooses, which the initializer has checked, or else *default*.
    """
    return default if ambiguity is None else parse_ambiguity(ambiguity)


# ── SKOS vocabulary ───────────────────────────────────────────────────────────

def _matching(initialization, execution) -> str:
    """How *execution* matches values: the constant it binds, EXACT when none."""
    value = _optional_constant(
        initialization, execution, MORPH_FN_MATCHING,
        "what the values are matched against is indexed before any data is "
        "read, so how they are matched cannot depend on the data.",
    )
    if value is None:
        return EXACT_MATCHING

    matching = parse_matching(value)
    if matching is None:
        raise _invalid_constant(
            initialization, execution, MORPH_FN_MATCHING, value, VALID_MATCHINGS,
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
    # A mistake in how an execution matches values, or in what it reconciles
    # an ambiguous value to, is reported before any vocabulary is downloaded.
    for execution in initialization.executions:
        values = _value_count(initialization, execution)
        if values > 1:
            raise ValueError(
                f"Execution '{execution.execution_id}' of function "
                f"'{initialization.function_iri}' binds {values} values to "
                "reconcile, but the concepts of a vocabulary are matched by a "
                f"single value. Bind '{GREL_VALUE_PARAM}' only."
            )
        _matching(initialization, execution)
        _ambiguity(initialization, execution)

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

    # And so is a mistake in the options of a resource.
    ambiguities = {}
    for resource in resources.values():
        if resource.resource_type and resource.resource_type != SKOS_VOCABULARY_RESOURCE:
            raise ValueError(
                f"Function '{initialization.function_iri}' reconciles against "
                f"resource '{resource.name}', whose 'resource_type' is "
                f"'{resource.resource_type}' instead of '{SKOS_VOCABULARY_RESOURCE}'."
            )
        ambiguities[resource.name] = _resource_ambiguity(initialization, resource)

    context = ReconciliationContext()
    for mapping_value, resource in resources.items():
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
                ambiguities[resource.name],
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


def _reconcile_rows(context, values, groups, lookup, outcomes=None) -> list:
    """
    Reconcile *values*, the rows of each group of *groups* (key -> row
    numbers) with a single call to ``lookup(key, values)``, which returns the
    entities each value matches, what they are entities of, the policy for
    ambiguous values, the index the values are looked up in, and the fields
    naming what they are entities of in the records of *outcomes*.
    """
    results = [None] * len(values)
    for key, rows in groups.items():
        matches, target, ambiguity, index, fields = lookup(key, [values[row] for row in rows])
        for row, row_matches in zip(rows, matches):
            if outcomes is not None:
                # Before a value is reconciled, which may abort the run.
                _report(outcomes, values[row], row_matches, index, ambiguity, fields)
            results[row] = _result(context, target, ambiguity, values[row], row_matches)
    return results


def _report(outcomes, value, matches, index, ambiguity, fields) -> None:
    """
    Count the outcome of reconciling *value* in *index*, and record it when it
    matches no entity or several (see :mod:`morph_kgc.functions.outcomes`): the
    value as read, and as looked up, how it was matched, and what it was matched
    against.
    """
    if len(matches) == 1:
        outcomes.count("MATCH")
        return
    outcome = MULTIPLE_MATCH if matches else NO_MATCH
    outcomes.count(outcome)

    def as_looked_up(component):
        # The value of a nested function may be of any type (a date, a
        # decimal...): it is looked up, and so reported, as a string.
        if component is None:
            return None, None
        return str(component), normalize(component, index.matching)

    if index.arity > 1:
        # A value of each of the components an entity is matched by.
        reported, normalized = (list(column) for column in zip(*map(as_looked_up, value)))
    else:
        reported, normalized = as_looked_up(value)
    record = {
        "value": reported,
        "normalized_value": normalized,
        **fields,
        "matching": index.matching,
    }
    if matches:
        record["ambiguity"] = ambiguity
        record["candidates"] = list(matches)
    # A value is told apart by what it is matched against as well, which an
    # execution may read from the data.
    outcomes.record(outcome, [fields, record["value"]], **record)


@stateful_bif(
    fun_id      = MORPH_RECONCILE_VOCABULARY_CONCEPT,
    initializer = _initialize_vocabulary,
    vectorized  = True,
    value       = VALUE_PARAMETERS,
    resource    = MORPH_RESOURCE_PARAMETERS,
    attribute   = MergedAliases(MORPH_ATTRIBUTE_PARAMETERS),
    matching    = MORPH_FN_MATCHING,
    ambiguity   = MORPH_FN_AMBIGUITY,
    outcomes_parameter = "outcomes",
)
def reconcile_vocabulary_concept(
    context, value, resource=None, attribute=None, matching=None, ambiguity=None,
    outcomes=None,
):
    """Reconcile each of *value* against a SKOS vocabulary fetched from a URL."""
    # The single value of each row, which the initializer has checked.
    value       = [row_values[0] for row_values in value]
    resources   = _per_row(resource, len(value))
    attributes  = _per_row(attribute, len(value))
    # A single constant, which the initializer has checked, in any case.
    matchings   = _per_row(EXACT_MATCHING if matching is None else matching, len(value))
    ambiguities = _per_row(ambiguity, len(value))

    # The rows reconciled against the same resource and attributes, in the same
    # way, which are usually all of them, are looked up together.
    groups: dict[tuple, list[int]] = {}
    for row, (row_resource, row_attribute, row_matching, row_ambiguity) in enumerate(
        zip(resources, attributes, matchings, ambiguities)
    ):
        key = (row_resource, _attributes(row_attribute), row_matching, row_ambiguity)
        groups.setdefault(key, []).append(row)

    def lookup(key, values):
        row_resource, row_attributes, row_matching, row_ambiguity = key
        name  = context.name(row_resource)
        index = context.get(name, parse_matching(row_matching))
        return (
            index.lookup_many(values, row_attributes),
            f"resource '{name}'",
            _row_ambiguity(row_ambiguity, context.ambiguity(name)),
            index,
            {"resource": name},
        )

    return _reconcile_rows(context, value, groups, lookup, outcomes)


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


def _listed(items, quoted=True) -> str:
    """*items* as a list in a sentence: 'a', 'b' and 'c'."""
    items = [f"'{item}'" if quoted else str(item) for item in items]
    return items[0] if len(items) == 1 else f"{', '.join(items[:-1])} and {items[-1]}"


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


def _initialize_sparql(initialization) -> QueryContext:
    """
    Build the shared context of the SPARQL reconciliation function: the index of
    every query of the mapping, by the endpoint it is sent to and the query, and
    the policy of each endpoint for ambiguous values.

    Each execution of the function pairs a query with its endpoint, so a mapping
    may reconcile against several knowledge graphs. A query sent to the same
    endpoint by several executions is sent once, which is logged, and its
    answer indexed for each way of matching values of those executions.
    """
    # A mistake in an execution, or in the resource declaring an endpoint, is
    # reported before any query is sent.
    context = QueryContext()
    # (endpoint, query) -> the resource declaring the endpoint, the number of
    # values the executions sending the query reconcile, the executions, and
    # the ways they match values, each indexed from the same answer
    queries = {}
    for execution in initialization.executions:
        endpoint = _constant(initialization, execution, MORPH_FN_SPARQL_ENDPOINT_URL)
        query    = _constant(initialization, execution, MORPH_FN_SPARQL_QUERY)
        values   = _value_count(initialization, execution)
        matching = _matching(initialization, execution)
        _ambiguity(initialization, execution)

        if (endpoint, query) not in queries:
            resource = _endpoint_resource(initialization, endpoint)
            context.ambiguities[endpoint] = _resource_ambiguity(initialization, resource)
            queries[(endpoint, query)] = (resource, values, [execution.execution_id], {matching: None})
            continue

        _, other_values, executions, matchings = queries[(endpoint, query)]
        other = executions[0]
        executions.append(execution.execution_id)
        matchings[matching] = None
        if values != other_values:
            raise ValueError(
                f"Executions '{other}' and '{execution.execution_id}' of function "
                f"'{initialization.function_iri}' send the same query to "
                f"'{endpoint}', but reconcile {other_values} and {values} values "
                "with it. A query matches entities by as many values as the "
                f"?{sparql.MATCHING_VARIABLE_PREFIX}<i> variables it projects."
            )

    for (endpoint, query), (resource, values, executions, matchings) in queries.items():
        if len(executions) > 1:
            LOGGER.info(
                f"Executions {_listed(sorted(executions))} of function "
                f"'{initialization.function_iri}' send the same query to SPARQL "
                f"endpoint '{endpoint}': it is sent once, and its answer is indexed "
                f"for {_listed(sorted(matching.lower() for matching in matchings), quoted=False)} "
                "matching."
            )
        context.indexes[(endpoint, query)] = sparql.build_index(
            resource,
            query,
            state_dir    = initialization.state_dir,
            memory_limit = initialization.memory_limit,
            arity        = values,
            matchings    = tuple(matchings),
        )

    return context


@stateful_bif(
    fun_id      = MORPH_RECONCILE_ENTITY_OVER_SPARQL,
    initializer = _initialize_sparql,
    vectorized  = True,
    value       = VALUE_PARAMETERS,
    endpoint    = MORPH_FN_SPARQL_ENDPOINT_URL,
    query       = MORPH_FN_SPARQL_QUERY,
    matching    = MORPH_FN_MATCHING,
    ambiguity   = MORPH_FN_AMBIGUITY,
    outcomes_parameter = "outcomes",
)
def reconcile_entity_over_sparql(
    context, value, endpoint=None, query=None, matching=None, ambiguity=None,
    outcomes=None,
):
    """
    Reconcile each of *value* against the entities a SPARQL query returns: the
    value of each row, or the tuple of its values, when it binds several.
    """
    # Every execution bound a single constant endpoint and query, and at most
    # one matching, which the initializer has indexed, and at most one policy,
    # which it has checked.
    value       = [row_values[0] if len(row_values) == 1 else row_values for row_values in value]
    endpoints   = _per_row(endpoint, len(value))
    queries     = _per_row(query, len(value))
    matchings   = _per_row(EXACT_MATCHING if matching is None else matching, len(value))
    ambiguities = _per_row(ambiguity, len(value))

    groups: dict[tuple, list[int]] = {}
    for row, key in enumerate(zip(endpoints, queries, matchings, ambiguities)):
        groups.setdefault(key, []).append(row)

    def lookup(key, values):
        row_endpoint, row_query, row_matching, row_ambiguity = key
        index = context.indexes[(row_endpoint, row_query)][parse_matching(row_matching)]
        return (
            index.lookup_many(values),
            f"SPARQL endpoint '{row_endpoint}'",
            _row_ambiguity(row_ambiguity, context.ambiguity(row_endpoint)),
            index,
            {
                "endpoint": row_endpoint,
                "query_hash": sha256(row_query.encode("utf-8")).hexdigest(),
            },
        )

    return _reconcile_rows(context, value, groups, lookup, outcomes)
