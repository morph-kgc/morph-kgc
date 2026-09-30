from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
SKOS vocabulary resources
=========================
Builds a :class:`~morph_kgc.reconciliation.index.ConceptIndex` from a SKOS
vocabulary fetched from the URL declared by a ``[RESOURCE:<name>]`` section::

    [RESOURCE:disease_vocabulary]
    resource_type=SKOS_VOCABULARY
    url=https://example.org/vocabulary/disease
    username={VOCABULARY_USER}
    password={VOCABULARY_PASSWORD}
    format=turtle
    matching=CASE-INSENSITIVE
    attributes=skos:prefLabel,skos:altLabel
    timeout=60

The vocabulary is retrieved once, during the context initialization phase.
"""

import logging

import rdflib

from ..constants import LOGGING_NAMESPACE
from ..http import DEFAULT_TIMEOUT, fetch
from .index import ConceptIndex, EXACT_MATCHING, VALID_MATCHINGS

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

# Serializations we can ask for, most specific first.
ACCEPTED_MEDIA_TYPES = (
    "text/turtle, application/rdf+xml;q=0.9, application/n-triples;q=0.9, "
    "application/ld+json;q=0.8, application/n-quads;q=0.8, text/n3;q=0.7, */*;q=0.1"
)

CONTENT_TYPE_FORMATS = {
    "text/turtle": "turtle",
    "application/x-turtle": "turtle",
    "application/rdf+xml": "xml",
    "text/rdf+xml": "xml",
    "application/n-triples": "nt",
    "text/plain": "nt",
    "application/n-quads": "nquads",
    "application/trig": "trig",
    "application/ld+json": "json-ld",
    "application/json": "json-ld",
    "text/n3": "n3",
}


def get_matching(resource) -> str:
    """Read and validate the 'matching' option of *resource*."""
    matching = resource.get("matching", EXACT_MATCHING).strip().upper()
    if matching not in VALID_MATCHINGS:
        raise ValueError(
            f"Option 'matching' of resource '{resource.name}' is '{matching}', "
            f"which is not valid. Must be one of: {sorted(VALID_MATCHINGS)}."
        )
    return matching


def resolve_attributes(resource, attributes) -> tuple[str, ...]:
    """
    Attributes to index: those the mapping matches against, falling back to the
    'attributes' option of the resource.
    """
    return tuple(attributes) or tuple(resource.get_list("attributes"))


def guess_format(resource, url: str, content_type: str) -> str | None:
    """
    Determine the RDF serialization of a fetched vocabulary: the 'format'
    option wins, then the content type it was served with, then its extension.
    """
    declared = resource.get("format")
    if declared:
        return declared

    if content_type in CONTENT_TYPE_FORMATS:
        return CONTENT_TYPE_FORMATS[content_type]

    return rdflib.util.guess_format(url.split("?")[0])


def load_vocabulary_graph(resource) -> rdflib.Graph:
    """Fetch and parse the SKOS vocabulary declared by *resource*."""
    url = resource.get_url()
    if not url:
        raise ValueError(
            f"Resource '{resource.name}' does not declare the 'url' of the "
            "vocabulary to reconcile against."
        )

    response = fetch(
        url,
        username    = resource.get_username(),
        password    = resource.get_password(),
        accept      = ACCEPTED_MEDIA_TYPES,
        timeout     = resource.get_int("timeout", DEFAULT_TIMEOUT),
        description = "vocabulary",
    )

    rdf_format = guess_format(resource, url, response.content_type)

    graph = rdflib.Graph()
    try:
        graph.parse(data=response.body, format=rdf_format)
    except Exception as exc:
        raise ValueError(
            f"The vocabulary of resource '{resource.name}' fetched from '{url}' "
            f"could not be parsed as RDF ({rdf_format or 'unknown format'}): "
            f"{exc}. Set the 'format' option of the resource to its "
            "serialization."
        ) from exc

    return graph


def build_index(resource, attributes=()) -> ConceptIndex:
    """
    Build the concept index of a SKOS vocabulary resource.

    Only *attributes* are indexed, which keeps the shared context to the part of
    the vocabulary the mapping actually reconciles against. When neither the
    mapping nor the resource names any, every property of the vocabulary whose
    value is a literal is indexed.
    """
    graph = load_vocabulary_graph(resource)
    attributes = resolve_attributes(resource, attributes)

    index = ConceptIndex(
        matching           = get_matching(resource),
        default_attributes = attributes,
    )

    if attributes:
        for attribute in attributes:
            predicate = rdflib.term.URIRef(attribute)
            for concept, value in graph.subject_objects(predicate):
                index.add(attribute, value, str(concept))
    else:
        for concept, predicate, value in graph:
            if isinstance(value, rdflib.term.Literal):
                index.add(str(predicate), value, str(concept))

    LOGGER.info(
        f"Resource '{resource.name}': indexed {len(index)} value(s) of "
        f"{len(index.entries)} attribute(s) of the vocabulary."
    )

    return index
