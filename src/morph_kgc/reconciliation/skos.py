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
    attributes=skos:prefLabel,skos:altLabel
    timeout=60

The vocabulary is retrieved once, during the context initialization phase. It
may be serialized as triples or as quads (N-Quads, TriG): the concepts of every
graph of a vocabulary split across named graphs are indexed.

How values are matched against the vocabulary is chosen by each execution of
the mapping, not here. A vocabulary that some executions match exactly and
others case-insensitively gets an index for each, built from a single download.

The vocabulary is downloaded to the state directory of the run and parsed as a
stream, its labels going to the index on disk as they are read, so that indexing
a vocabulary takes the same memory whatever its size. A serialization the
streaming parser cannot read (TriX, a JSON-LD document with a remote context, a
document breaking the syntax in ways rdflib tolerates) is parsed in memory with
rdflib instead, as a fallback, with a warning. Where the streaming parser keeps
a typed literal as written (``"01"^^xsd:integer`` is matched by ``01``), rdflib
normalizes it (matched by ``1``). A JSON-LD document whose top level is an
object (``@context`` and ``@graph``) is read whole before its first statement,
also with a warning: only a top-level array of nodes is read as a stream.

The ``attributes`` are absolute IRIs, or prefixed names with one of the
well-known prefixes in :data:`WELL_KNOWN_PREFIXES` (``skos:``, ``dcterms:``,
``schema:``, ...). An attribute in any other namespace is written as a full IRI.
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote, urlsplit, urlunsplit

import duckdb
import pyoxigraph
import rdflib
from rdflib.namespace import DC, DCTERMS, FOAF, OWL, RDF, RDFS, SDO, SKOS, XSD

from ..constants import LOGGING_NAMESPACE
from ..http import DEFAULT_TIMEOUT, download
from .index import ConceptIndex, ConceptIndexBuilder, EXACT_MATCHING

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

# Prefixes the 'attributes' option may use. The prefixes a vocabulary declares
# are not used: rdflib renames them when they clash with its own bindings.
WELL_KNOWN_PREFIXES = {
    "skos":    str(SKOS),
    "rdf":     str(RDF),
    "rdfs":    str(RDFS),
    "owl":     str(OWL),
    "xsd":     str(XSD),
    "dc":      str(DC),
    "dcterms": str(DCTERMS),
    "dct":     str(DCTERMS),
    "schema":  str(SDO),
    "foaf":    str(FOAF),
}

# SKOS makes these classes disjoint from skos:Concept: their resources are
# labelled as concepts are, but no value identifies one of them as a concept.
NON_CONCEPT_CLASSES = frozenset(
    str(concept_class)
    for concept_class in (SKOS.ConceptScheme, SKOS.Collection, SKOS.OrderedCollection)
)
RDF_TYPE = str(RDF.type)

# The serializations the streaming parser reads, by the names the 'format'
# option and guess_format() give them (rdflib's, and other usual spellings).
STREAMING_FORMATS = {
    "turtle":    pyoxigraph.RdfFormat.TURTLE,
    "ttl":       pyoxigraph.RdfFormat.TURTLE,
    "n3":        pyoxigraph.RdfFormat.N3,
    "nt":        pyoxigraph.RdfFormat.N_TRIPLES,
    "nt11":      pyoxigraph.RdfFormat.N_TRIPLES,
    "ntriples":  pyoxigraph.RdfFormat.N_TRIPLES,
    "n-triples": pyoxigraph.RdfFormat.N_TRIPLES,
    "nquads":    pyoxigraph.RdfFormat.N_QUADS,
    "nq":        pyoxigraph.RdfFormat.N_QUADS,
    "n-quads":   pyoxigraph.RdfFormat.N_QUADS,
    "trig":      pyoxigraph.RdfFormat.TRIG,
    "xml":       pyoxigraph.RdfFormat.RDF_XML,
    "rdf/xml":   pyoxigraph.RdfFormat.RDF_XML,
    "rdfxml":    pyoxigraph.RdfFormat.RDF_XML,
    "json-ld":   pyoxigraph.RdfFormat.JSON_LD,
    "jsonld":    pyoxigraph.RdfFormat.JSON_LD,
}

# Serializations we can ask for, most specific first.
ACCEPTED_MEDIA_TYPES = (
    "text/turtle, application/rdf+xml;q=0.9, application/n-triples;q=0.9, "
    "application/ld+json;q=0.8, application/n-quads;q=0.8, application/trig;q=0.8, "
    "text/n3;q=0.7, */*;q=0.1"
)

CONTENT_TYPE_FORMATS = {
    "text/turtle": "turtle",
    "application/x-turtle": "turtle",
    "application/rdf+xml": "xml",
    "text/rdf+xml": "xml",
    "application/n-triples": "nt",
    "application/n-quads": "nquads",
    "text/x-nquads": "nquads",
    "application/trig": "trig",
    "application/x-trig": "trig",
    "application/ld+json": "json-ld",
    "application/json": "json-ld",
    "text/n3": "n3",
}

# Content types too generic to say which serialization they carry: the extension
# of the URL is a better hint (a '.nq' file served as text/plain holds quads).
GENERIC_CONTENT_TYPE_FORMATS = {
    "text/plain": "nt",
}


def is_absolute_iri(value: str) -> bool:
    """
    True when *value* is an absolute IRI that no prefixed name can be mistaken
    for: one with an authority (``http://...``) or an absolute path
    (``file:///...``), or a URN or tag IRI.
    """
    parts = urlsplit(value)
    return bool(parts.scheme) and (
        bool(parts.netloc)
        or parts.path.startswith("/")
        or parts.scheme.lower() in ("urn", "tag")
    )


def resolve_attributes(resource) -> tuple[str, ...]:
    """
    The 'attributes' option of *resource*, its prefixed names expanded with the
    well-known prefixes.
    """
    attributes = []
    for attribute in resource.get_list("attributes"):
        prefix, separator, local_name = attribute.partition(":")
        if is_absolute_iri(attribute):
            attributes.append(attribute)
        elif separator and prefix in WELL_KNOWN_PREFIXES:
            attributes.append(WELL_KNOWN_PREFIXES[prefix] + local_name)
        else:
            raise ValueError(
                f"Option 'attributes' of resource '{resource.name}' holds "
                f"'{attribute}', which is neither an absolute IRI nor a prefixed "
                "name with one of the prefixes "
                f"{', '.join(sorted(WELL_KNOWN_PREFIXES))}. Write it as a full IRI."
            )
    return tuple(attributes)


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

    return (
        rdflib.util.guess_format(url.split("?")[0])
        or GENERIC_CONTENT_TYPE_FORMATS.get(content_type)
    )


def download_vocabulary(resource, state_dir: str):
    """Download the SKOS vocabulary declared by *resource* to *state_dir*."""
    url = resource.get_url()
    if not url:
        raise ValueError(
            f"Resource '{resource.name}' does not declare the 'url' of the "
            "vocabulary to reconcile against."
        )
    if urlsplit(url).username is not None:
        raise ValueError(
            f"The 'url' of resource '{resource.name}' holds credentials. Keep "
            "them out of the URL: declare them as 'username' and 'password' in "
            f"the configuration file, in its '[RESOURCE:{resource.name}]' section."
        )

    return download(
        url,
        state_dir,
        username    = resource.get_username(),
        password    = resource.get_password(),
        accept      = ACCEPTED_MEDIA_TYPES,
        timeout     = resource.get_int("timeout", DEFAULT_TIMEOUT),
        description = "vocabulary",
    )


class _StreamingParseError(Exception):
    """The streaming parser cannot read a vocabulary, which rdflib may."""


UTF8_BOM = b"\xef\xbb\xbf"


def _valid_base_iri(iri: str) -> str | None:
    """
    *iri* as the base IRI of the streaming parser, percent-encoded where it is
    no IRI ('|', '^', '%zz' in a path urllib fetches anyway); None when it
    cannot be one.
    """
    escaped = re.sub(r"%(?![0-9A-Fa-f]{2})", "%25", iri)
    for candidate in (iri, quote(escaped, safe=":/?#[]@!$&'()*+,;=%~")):
        try:
            return pyoxigraph.NamedNode(candidate).value
        except ValueError:
            pass
    return None


def _starts_with(path: str, prefix: bytes) -> bool:
    """Whether the file at *path*, after a byte order mark and spaces, starts with *prefix*."""
    with open(path, "rb") as document:
        head = document.read(4096)
    return head.removeprefix(UTF8_BOM).lstrip().startswith(prefix)


def _term(term) -> tuple[str, str | None]:
    """The kind of an RDF term (iri, bnode or literal) and its value."""
    if isinstance(term, (pyoxigraph.NamedNode, rdflib.URIRef)):
        return "iri", str(term.value if isinstance(term, pyoxigraph.NamedNode) else term)
    if isinstance(term, (pyoxigraph.Literal, rdflib.Literal)):
        return "literal", str(term.value if isinstance(term, pyoxigraph.Literal) else term)
    if isinstance(term, (pyoxigraph.BlankNode, rdflib.BNode)):
        return "bnode", None
    # A triple term, which identifies no concept and is no label.
    return "other", None


def _streamed_statements(path: str, rdf_format, base_iri: str | None, predicates=None):
    """
    The statements of the vocabulary at *path*, read as a stream: those of
    *predicates* only, unless it is None.
    """
    if rdf_format is None:
        raise _StreamingParseError("the streaming parser does not read its serialization")
    # The contents of an N3 formula ({ ... }) are quoted, not asserted: they
    # come in a graph of their own.
    asserted_only = rdf_format == pyoxigraph.RdfFormat.N3
    with open(path, "rb") as document:
        # The parser takes a byte order mark for the start of the document.
        if document.read(len(UTF8_BOM)) != UTF8_BOM:
            document.seek(0)
        try:
            quads = pyoxigraph.parse(document, format=rdf_format, base_iri=base_iri, lenient=True)
            for quad in quads:
                predicate = quad.predicate.value
                if predicates is not None and predicate not in predicates:
                    continue
                if asserted_only and not isinstance(quad.graph_name, pyoxigraph.DefaultGraph):
                    continue
                yield _term(quad.subject), predicate, _term(quad.object)
        except SyntaxError as exc:
            raise _StreamingParseError(str(exc)) from exc


def _parsed_statements(path: str, rdf_format: str | None, base_iri: str):
    """The statements of the vocabulary at *path*, parsed in memory by rdflib."""
    # Its default graph is the union of all the graphs of the vocabulary, so
    # that the triples of a vocabulary serialized as quads are not lost.
    graph = rdflib.Dataset(default_union=True)
    graph.parse(source=path, format=rdf_format, publicID=base_iri)
    for subject, predicate, value in graph.triples((None, None, None)):
        subject, predicate, value = _term(subject), str(predicate), _term(value)
        # rdflib reads a lone surrogate escape ("\uD800"), which no text holds
        # and the index cannot store; the streaming parser rejects it.
        if _is_text(subject[1]) and _is_text(predicate) and _is_text(value[1]):
            yield subject, predicate, value


def _is_text(value: str | None) -> bool:
    """False for a string holding a lone surrogate."""
    try:
        value is None or value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


@dataclass
class _Selection:
    """
    The attributes the index of one way of matching values holds: those the
    executions matching that way match against, and every property whose value
    is a literal when one of them matches against any.
    """

    attributes: dict = field(default_factory=dict)
    every_attribute: bool = False


def _index_statements(statements, builder, selections) -> dict:
    """
    Add to *builder*, for each matching of *selections*, the values the concepts
    of *statements* take for the attributes of its selection, and for every
    property whose value is a literal when it selects every attribute. Returns
    those properties, by matching.
    """
    literal_attributes = {matching: {} for matching in selections}
    for (subject_kind, subject), predicate, (value_kind, value) in statements:
        # A blank node identifies no concept outside the vocabulary.
        if subject_kind != "iri":
            continue
        if predicate == RDF_TYPE and value_kind == "iri" and value in NON_CONCEPT_CLASSES:
            builder.exclude(subject)
        for matching, selection in selections.items():
            if value_kind == "literal" and selection.every_attribute:
                builder.add(predicate, value, subject, matching)
                literal_attributes[matching][predicate] = None
            elif value_kind in ("literal", "iri") and predicate in selection.attributes:
                builder.add(predicate, value, subject, matching)
    return literal_attributes


def _build(builder, literal_attributes, default_attributes) -> dict[str, ConceptIndex]:
    """Write the index of each matching of *builder* to its file, by matching."""
    return {
        matching: builder.build(
            default_attributes or tuple(literal_attributes[matching]), matching,
        )
        for matching in builder.matchings
    }


def build_index(
    resource,
    matched_attributes=((EXACT_MATCHING, ()),),
    *,
    state_dir: str,
    memory_limit: str,
) -> dict[str, ConceptIndex]:
    """
    Build the concept indexes of a SKOS vocabulary resource, in *state_dir*: one
    for each way the executions of the function match values against it, by
    matching. The vocabulary is downloaded and read once for all of them.

    *matched_attributes* holds, for each execution of the function reconciling
    against the resource, how it matches values (``EXACT`` or
    ``CASE-INSENSITIVE``) and the attributes it matches against: the ones it
    names, ``()`` when it names none, and ``None`` when it reads them from the
    data. An execution naming none matches against the 'attributes' option of
    the resource or, without it, against every property of the vocabulary whose
    value is a literal.

    Only those attributes are indexed, and only for the ways of matching of the
    executions matching against them, which keeps the shared context to the
    part of the vocabulary the mapping actually reconciles against. Concept
    schemes and collections are left out.
    """
    # The option only concerns the executions naming no attribute: a mistake in
    # it does not stop a mapping none of whose executions relies on it.
    default_attributes = ()
    if any(attributes == () for _, attributes in matched_attributes):
        default_attributes = resolve_attributes(resource)

    selections: dict[str, _Selection] = {}
    for matching, execution_attributes in matched_attributes:
        selection = selections.setdefault(matching, _Selection())
        if execution_attributes is None:
            selection.every_attribute = True
        elif execution_attributes:
            selection.attributes.update(dict.fromkeys(execution_attributes))
        elif default_attributes:
            selection.attributes.update(dict.fromkeys(default_attributes))
        else:
            selection.every_attribute = True
    if not selections:
        selections[EXACT_MATCHING] = _Selection()

    vocabulary = download_vocabulary(resource, state_dir)
    try:
        url = resource.get_url()
        rdf_format = guess_format(resource, url, vocabulary.content_type)
        # Relative IRIs resolve against the URL of the vocabulary, but not its
        # query, which may hold a secret (an API key) read from the environment.
        if vocabulary.temporary:
            base_iri = urlunsplit(urlsplit(url)._replace(query="", fragment=""))
        else:
            base_iri = Path(url).resolve().as_uri()
        # Without a hint, as rdflib does, the vocabulary is taken for Turtle.
        # The 'format' option may name it by its media type, as rdflib allows.
        format_name = (rdf_format or "turtle").strip().lower()
        streaming_format = STREAMING_FORMATS.get(CONTENT_TYPE_FORMATS.get(format_name, format_name))
        if streaming_format == pyoxigraph.RdfFormat.JSON_LD and _starts_with(vocabulary.path, b"{"):
            LOGGER.warning(
                f"The vocabulary of resource '{resource.name}' is a JSON-LD "
                "document whose top level is an object (@context and @graph), "
                "which is read whole into memory before it is indexed. Serve it "
                "as N-Triples, Turtle or RDF/XML to keep memory bounded."
            )
        # Every statement is read when every property with a literal value is
        # indexed; otherwise, only those that can be indexed or exclude a concept.
        predicates = None
        if not any(selection.every_attribute for selection in selections.values()):
            predicates = frozenset(
                attribute
                for selection in selections.values()
                for attribute in selection.attributes
            ) | {RDF_TYPE}

        try:
            with ConceptIndexBuilder(state_dir, memory_limit, *selections) as builder:
                statements = _streamed_statements(
                    vocabulary.path, streaming_format, _valid_base_iri(base_iri), predicates,
                )
                literal_attributes = _index_statements(statements, builder, selections)
                indexes = _build(builder, literal_attributes, default_attributes)
        except _StreamingParseError as streaming_error:
            LOGGER.warning(
                f"The vocabulary of resource '{resource.name}' cannot be read as "
                f"a stream ({streaming_error}), so it is parsed in memory."
            )
            with ConceptIndexBuilder(state_dir, memory_limit, *selections) as builder:
                try:
                    statements = _parsed_statements(vocabulary.path, rdf_format, base_iri)
                    literal_attributes = _index_statements(statements, builder, selections)
                except duckdb.Error:
                    # Writing the index failed (a full disk), not reading the vocabulary.
                    raise
                except Exception as exc:
                    raise ValueError(
                        f"The vocabulary of resource '{resource.name}' fetched from "
                        f"'{url}' could not be parsed as RDF "
                        f"({rdf_format or 'unknown format'}): {streaming_error}; "
                        f"{(str(exc).strip() or type(exc).__name__).splitlines()[0]}. "
                        "Set the 'format' option of the resource to its serialization."
                    ) from exc
                indexes = _build(builder, literal_attributes, default_attributes)
    finally:
        vocabulary.remove()

    for matching, index in indexes.items():
        LOGGER.info(
            f"Resource '{resource.name}': indexed {len(index)} value(s) of "
            f"{len(index.indexed_attributes)} attribute(s) of the vocabulary, "
            f"for {matching.lower()} matching."
        )

    return indexes
