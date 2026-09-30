from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
SPARQL endpoint resources
=========================
Builds a :class:`~morph_kgc.reconciliation.index.ConceptIndex` by querying the SPARQL
endpoint declared by a ``[RESOURCE:<name>]`` section::

    [RESOURCE:reference_kg]
    resource_type=SPARQL_ENDPOINT
    url=https://example.org/sparql
    username={ENDPOINT_USER}
    password={ENDPOINT_PASSWORD}
    query=SELECT ?concept ?attribute ?value WHERE { ?concept ?attribute ?value }
    method=POST
    matching=CASE-INSENSITIVE
    attributes=http://www.w3.org/2004/02/skos/core#prefLabel

The endpoint is queried once, during the context initialization phase, and the
resulting index is reused by every invocation of the function.

The query must be a SELECT query projecting the concept and the value it is
reconciled by; an ``?attribute`` variable may be projected as well to say which
property the value comes from. The variable names are configurable with the
``concept_variable``, ``attribute_variable`` and ``value_variable`` options.
"""

import json
import logging
from urllib.parse import urlencode

from ..constants import DEFAULT_RECONCILIATION_ATTRIBUTES, LOGGING_NAMESPACE
from ..http import DEFAULT_TIMEOUT, fetch
from .index import ConceptIndex
from .skos import get_matching, resolve_attributes

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

SPARQL_RESULTS_JSON = "application/sparql-results+json"

DEFAULT_CONCEPT_VARIABLE   = "concept"
DEFAULT_ATTRIBUTE_VARIABLE = "attribute"
DEFAULT_VALUE_VARIABLE     = "value"

VALID_METHODS = {"GET", "POST"}

_DEFAULT_QUERY = """SELECT ?{concept} ?{attribute} ?{value} WHERE {{
    ?{concept} ?{attribute} ?{value} .
    VALUES ?{attribute} {{ {attributes} }}
}}"""


def build_query(resource, attributes) -> str:
    """
    The query to initialize the context with: the 'query' option of the
    resource, or a SKOS query over the attributes the mapping matches against.
    """
    query = resource.get("query")
    if query:
        return query

    attributes = attributes or DEFAULT_RECONCILIATION_ATTRIBUTES

    return _DEFAULT_QUERY.format(
        concept    = resource.get("concept_variable", DEFAULT_CONCEPT_VARIABLE),
        attribute  = resource.get("attribute_variable", DEFAULT_ATTRIBUTE_VARIABLE),
        value      = resource.get("value_variable", DEFAULT_VALUE_VARIABLE),
        attributes = " ".join(f"<{attribute}>" for attribute in attributes),
    )


def get_method(resource) -> str:
    method = resource.get("method", "GET").strip().upper()
    if method not in VALID_METHODS:
        raise ValueError(
            f"Option 'method' of resource '{resource.name}' is '{method}', "
            f"which is not valid. Must be one of: {sorted(VALID_METHODS)}."
        )
    return method


def run_query(resource, query: str) -> list[dict]:
    """Send *query* to the endpoint of *resource* and return its bindings."""
    url = resource.get_url()
    if not url:
        raise ValueError(
            f"Resource '{resource.name}' does not declare the 'url' of the "
            "SPARQL endpoint to reconcile against."
        )

    method = get_method(resource)
    body = None
    content_type = ""

    if method == "POST":
        body = urlencode({"query": query}).encode("utf-8")
        content_type = "application/x-www-form-urlencoded"
    else:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}{urlencode({'query': query})}"

    response = fetch(
        url,
        username     = resource.get_username(),
        password     = resource.get_password(),
        accept       = SPARQL_RESULTS_JSON,
        data         = body,
        content_type = content_type,
        method       = method,
        timeout      = resource.get_int("timeout", DEFAULT_TIMEOUT),
        description  = "SPARQL endpoint",
    )

    try:
        results = json.loads(response.body.decode("utf-8"))
        return results["results"]["bindings"]
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise ValueError(
            f"The SPARQL endpoint of resource '{resource.name}' did not answer "
            f"with SPARQL results in JSON: {exc}. The query must be a SELECT "
            "query."
        ) from exc


def build_index(resource, attributes=()) -> ConceptIndex:
    """Build the concept index of a SPARQL endpoint resource."""
    attributes = resolve_attributes(resource, attributes)
    bindings = run_query(resource, build_query(resource, attributes))

    concept_variable   = resource.get("concept_variable", DEFAULT_CONCEPT_VARIABLE)
    attribute_variable = resource.get("attribute_variable", DEFAULT_ATTRIBUTE_VARIABLE)
    value_variable     = resource.get("value_variable", DEFAULT_VALUE_VARIABLE)

    index = ConceptIndex(
        matching           = get_matching(resource),
        default_attributes = attributes,
    )

    for binding in bindings:
        concept = binding.get(concept_variable, {}).get("value")
        value   = binding.get(value_variable, {}).get("value")
        if concept is None or value is None:
            continue

        # A query that does not project the attribute reconciles against a
        # single, unnamed one; the mapping then needs not name it either.
        attribute = binding.get(attribute_variable, {}).get("value", "")
        index.add(attribute, value, concept)

    # The endpoint answered without saying which property each value comes from,
    # so the index cannot tell attributes apart and lookups match on value only.
    index.attribute_agnostic = set(index.entries) == {""}

    LOGGER.info(
        f"Resource '{resource.name}': indexed {len(index)} value(s) of "
        f"{len(index.entries)} attribute(s) from the SPARQL endpoint."
    )

    return index
