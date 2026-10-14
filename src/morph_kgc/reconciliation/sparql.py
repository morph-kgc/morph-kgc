from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
SPARQL endpoint reconciliation
==============================
Builds a :class:`~morph_kgc.reconciliation.index.ConceptIndex` from the answer
of a SPARQL endpoint to a SELECT query of the mapping. The query may read any
knowledge graph, and projects two variables:

- ``?entity_iri``, the entity a value is reconciled with.
- ``?matching_value_1``, the value the entity is matched by.

For instance::

    PREFIX schema: <https://schema.org/>
    SELECT ?entity_iri ?matching_value_1 WHERE {
        GRAPH ?g {
            ?entity_iri a schema:MedicalStudy ;
                        schema:identifier ?matching_value_1 .
        }
    }

The query is sent once, during the context initialization phase, and exactly as
written. Its answer is downloaded to the state directory of the run and read as
a stream into the index on disk, so that an answer of any size is indexed with
the same memory. An answer in JSON is only read as a stream when it gives its
head first, as most stores write it, and when every term in it is well formed;
otherwise it is read whole into memory, with a warning. Whether its patterns
read the named graphs of the endpoint is up to the query, since stores differ on
whether their default graph includes them. Values are matched exactly; a query
may normalize the values it returns (``LCASE``), and the mapping the values it
reconciles, to match otherwise.

How the endpoint is accessed is declared in the configuration file, only when
its URL is not enough: a ``[RESOURCE:<name>]`` section whose ``url`` (or
``iri``) is the endpoint named by the mapping::

    [RESOURCE:clinical_trials]
    resource_type=SPARQL_ENDPOINT
    url=https://example.org/sparql
    username={ENDPOINT_USER}
    password={ENDPOINT_PASSWORD}
    method=POST
"""

import gzip
import json
import logging
import re
from http.client import HTTPException
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit

import pyoxigraph

from .._version import __version__
from ..constants import LOGGING_NAMESPACE
from ..http import Download, download
from .index import ConceptIndex, ConceptIndexBuilder

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

SPARQL_RESULTS_JSON = "application/sparql-results+json"

# The serializations of SPARQL results read as a stream, by content type. JSON
# is the one asked for; the others are read too, should an endpoint send them.
RESULTS_FORMATS = {
    SPARQL_RESULTS_JSON:               pyoxigraph.QueryResultsFormat.JSON,
    "application/json":                pyoxigraph.QueryResultsFormat.JSON,
    "application/sparql-results+xml":  pyoxigraph.QueryResultsFormat.XML,
    "application/xml":                 pyoxigraph.QueryResultsFormat.XML,
    "text/xml":                        pyoxigraph.QueryResultsFormat.XML,
    "text/tab-separated-values":       pyoxigraph.QueryResultsFormat.TSV,
}

# The variables the query projects.
ENTITY_VARIABLE   = "entity_iri"
MATCHING_VARIABLE = "matching_value_1"
MATCHING_VARIABLES = re.compile(r"matching_value_\d+")

VALID_METHODS = {"GET", "POST"}

# Unless the resource says otherwise, a query is sent with GET, the method every
# endpoint supports, but a query too long for a URL is sent with POST.
MAX_GET_URL_LENGTH = 4096

# The query is sent once, over a whole knowledge graph, so it is given time.
DEFAULT_TIMEOUT = 300

# Some endpoints, such as the Wikidata Query Service, refuse anonymous clients.
USER_AGENT = f"morph-kgc/{__version__}"

# How much of an error answered by the endpoint (e.g. a syntax error in the
# query) is reported.
ERROR_EXCERPT_LENGTH = 500


def get_method(resource, url: str, query: str) -> str:
    method = resource.get("method").strip().upper()
    if not method:
        get_url = f"{url}{'&' if '?' in url else '?'}{urlencode({'query': query})}"
        return "GET" if len(get_url) <= MAX_GET_URL_LENGTH else "POST"

    if method not in VALID_METHODS:
        raise ValueError(
            f"Option 'method' of resource '{resource.name}' is '{method}', "
            f"which is not valid. Must be one of: {sorted(VALID_METHODS)}."
        )
    return method


def run_query(resource, query: str, state_dir: str) -> Download:
    """
    Send *query* to the endpoint of *resource* and download its answer to
    *state_dir*.
    """
    endpoint = resource.get_url()
    if urlsplit(endpoint).scheme.lower() not in ("http", "https"):
        raise ValueError(
            f"'{endpoint or resource.name}' is not the URL of a SPARQL endpoint "
            "to reconcile against: it must be an http(s) URL."
        )

    method = get_method(resource, endpoint, query)

    if method == "POST":
        request = {
            "data": urlencode({"query": query}).encode("utf-8"),
            "content_type": "application/x-www-form-urlencoded",
        }
    else:
        request = {"params": {"query": query}}

    try:
        return download(
            endpoint,
            state_dir,
            username    = resource.get_username(),
            password    = resource.get_password(),
            accept      = SPARQL_RESULTS_JSON,
            headers     = {"User-Agent": USER_AGENT},
            method      = method,
            timeout     = resource.get_int("timeout", DEFAULT_TIMEOUT),
            description = "SPARQL endpoint",
            **request,
        )
    except (OSError, HTTPException) as exc:
        # Not wrapped by download: the endpoint took too long to answer the
        # query, or dropped the connection while answering it.
        detail = str(exc) or type(exc).__name__
        if not isinstance(exc, TimeoutError):
            raise ValueError(
                f"Could not query the SPARQL endpoint '{endpoint}': {detail}."
            ) from exc
        raise ValueError(
            f"The SPARQL endpoint '{endpoint}' did not answer the query: {detail}. "
            "If the query needs longer, declare the seconds to wait for it as "
            "'timeout' in the configuration file, in a '[RESOURCE:<name>]' "
            f"section with 'resource_type=SPARQL_ENDPOINT' and 'url={endpoint}'."
        ) from exc
    except ValueError as exc:
        cause = exc.__cause__
        if not isinstance(cause, HTTPError):
            raise

        if cause.code in (401, 403) and not resource.has_credentials():
            # The endpoint may well be named by the mapping only, so the
            # configuration file may have no section to set credentials in yet.
            raise ValueError(
                f"The SPARQL endpoint '{endpoint}' denied access ({cause.code} "
                f"{cause.reason}). To access it with HTTP Basic Authentication, "
                "declare its 'username' and 'password' in the configuration "
                "file, in a '[RESOURCE:<name>]' section with "
                f"'resource_type=SPARQL_ENDPOINT' and 'url={endpoint}'."
            ) from exc

        # The endpoint says what is wrong with the query far better than the
        # status code does.
        detail = _error_body(cause).decode("utf-8", errors="replace").strip()
        if not detail:
            raise
        raise ValueError(
            f"The SPARQL endpoint '{endpoint}' answered {cause.code} "
            f"{cause.reason}: {detail[:ERROR_EXCERPT_LENGTH]}"
        ) from exc


def read_solutions(endpoint: str, answer: Download):
    """
    The variables the answer of *endpoint* in the file *answer* projects, if
    it says, and an iterator of its solutions, read as a stream: for each, the
    term bound to ?entity_iri and to ?matching_value_1, as (kind, value) pairs
    whose kind is ``uri`` for an IRI.
    """
    json_format = pyoxigraph.QueryResultsFormat.JSON
    results_format = RESULTS_FORMATS.get(answer.content_type, json_format)
    try:
        results = pyoxigraph.parse_query_results(path=answer.path, format=results_format)
    except SyntaxError as exc:
        if results_format != json_format:
            raise _not_results(endpoint, answer, exc) from exc
        # The head is mandatory in SPARQL results, but an endpoint omitting it
        # is not worth failing for: the bindings then speak for the projection.
        return _read_json_solutions(endpoint, answer, exc)

    if not isinstance(results, pyoxigraph.QuerySolutions):
        raise _not_results(endpoint, answer, "it is the answer to an ASK query")

    if results_format == json_format and not _starts_with_head(answer.path):
        # The parser holds the solutions until it reads which variables they bind.
        LOGGER.warning(
            f"The answer of the SPARQL endpoint '{endpoint}' gives its results "
            "before its head, so it is read whole into memory before it is indexed."
        )

    variables = [variable.value for variable in results.variables]
    if ENTITY_VARIABLE not in variables or MATCHING_VARIABLE not in variables:
        # check_variables says which is missing.
        return variables, iter(())
    # By position, much faster than by name over millions of solutions.
    entity, value = variables.index(ENTITY_VARIABLE), variables.index(MATCHING_VARIABLE)

    def solutions():
        try:
            for solution in results:
                yield _solution_term(solution[entity]), _solution_term(solution[value])
        except SyntaxError as exc:
            if results_format == json_format:
                # A term the streaming parser rejects and json.load may not: a
                # Virtuoso 'nodeID://' blank node, a language tag that is not
                # BCP 47, an IRI with a space.
                raise _StreamingError(exc) from exc
            raise _not_results(endpoint, answer, exc) from exc

    return variables, solutions()


class _StreamingError(Exception):
    """The streaming parser rejects a solution of an answer in JSON."""


def _starts_with_head(path: str) -> bool:
    """Whether the answer in JSON at *path* gives its head first."""
    with open(path, "rb") as answer_file:
        start = answer_file.read(4096)
    return re.match(rb'(\xef\xbb\xbf)?\s*\{\s*"head"', start) is not None


def _solution_term(term) -> tuple[str, str] | None:
    """The kind and value of a term bound in a solution, None when unbound."""
    if term is None:
        return None
    if isinstance(term, pyoxigraph.NamedNode):
        return "uri", term.value
    # A triple term has no value of its own to match.
    return type(term).__name__.lower(), getattr(term, "value", None)


def _read_json_solutions(endpoint: str, answer: Download, reason):
    """The solutions of an answer in JSON the streaming parser rejects, in memory."""
    try:
        with open(answer.path, "rb") as answer_file:
            results = json.load(answer_file)
        bindings = results["results"]["bindings"]
        iter(bindings)
    except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as exc:
        raise _not_results(endpoint, answer, exc) from exc

    LOGGER.warning(
        f"The answer of the SPARQL endpoint '{endpoint}' cannot be read as a "
        f"stream ({reason}), so it is read in memory."
    )

    head = results.get("head")
    variables = head.get("vars") if isinstance(head, dict) else None

    def term(binding):
        if not isinstance(binding, dict):
            return None
        return binding.get("type"), binding.get("value")

    def solutions():
        for binding in bindings:
            yield term(binding.get(ENTITY_VARIABLE)), term(binding.get(MATCHING_VARIABLE))

    return variables, solutions()


def _not_results(endpoint: str, answer: Download, detail) -> ValueError:
    return ValueError(
        f"The SPARQL endpoint '{endpoint}' did not answer with SPARQL results "
        f"in JSON ({answer.content_type or 'no content type'}): {detail}. The "
        "query must be a SELECT query."
    )


def _error_body(error: HTTPError) -> bytes:
    """The body of an error answer, decompressed as fetch does a successful one."""
    body = error.read()
    if error.headers.get("Content-Encoding", "").lower() != "gzip":
        return body
    try:
        return gzip.decompress(body)
    except (OSError, EOFError):
        return b""


def check_variables(endpoint: str, variables) -> None:
    """
    Make sure the query projects the variables reconciliation reads, rather than
    letting a misspelled one silently reconcile every value against nothing.
    """
    missing = [
        variable
        for variable in (ENTITY_VARIABLE, MATCHING_VARIABLE)
        if variable not in variables
    ]
    if missing:
        raise ValueError(
            f"The SPARQL query sent to '{endpoint}' does not project "
            f"{' and '.join(f'?{variable}' for variable in missing)}. It must "
            f"project ?{ENTITY_VARIABLE}, the entity a value is reconciled with, "
            f"and ?{MATCHING_VARIABLE}, the value the entity is matched by. It "
            f"projects: {' '.join(f'?{variable}' for variable in variables) or 'nothing'}."
        )

    unsupported = [
        variable
        for variable in variables
        if MATCHING_VARIABLES.fullmatch(variable) and variable != MATCHING_VARIABLE
    ]
    if unsupported:
        raise ValueError(
            f"The SPARQL query sent to '{endpoint}' projects "
            f"{' '.join(f'?{variable}' for variable in unsupported)}, but a value "
            f"is only matched against ?{MATCHING_VARIABLE}. To match a value "
            "against several properties, bind them all to "
            f"?{MATCHING_VARIABLE}, e.g. with a UNION."
        )


def _index_solutions(solutions, state_dir: str, memory_limit: str):
    """The index of *solutions*, and how many bound ?entity_iri to no IRI."""
    not_iris = 0
    with ConceptIndexBuilder(state_dir, memory_limit) as builder:
        for entity, value in solutions:
            # Unbound in this solution, e.g. by an OPTIONAL pattern.
            if entity is None or value is None or value[1] is None:
                continue
            # A blank node or a literal identifies no entity outside the answer.
            if entity[0] != "uri":
                not_iris += 1
                continue
            # The answer says nothing of attributes: the values are indexed
            # under a single, unnamed one.
            builder.add("", value[1], entity[1])
        return builder.build(), not_iris


def build_index(resource, query: str, *, state_dir: str, memory_limit: str) -> ConceptIndex:
    """Build the index of *query* sent to the endpoint of *resource*, in *state_dir*."""
    endpoint = resource.get_url()
    answer = run_query(resource, query, state_dir)
    try:
        variables, solutions = read_solutions(endpoint, answer)
        if variables is not None:
            check_variables(endpoint, variables)

        try:
            index, not_iris = _index_solutions(solutions, state_dir, memory_limit)
        except _StreamingError as exc:
            # Indexed anew from the start, by a new builder.
            _, solutions = _read_json_solutions(endpoint, answer, exc)
            index, not_iris = _index_solutions(solutions, state_dir, memory_limit)
    finally:
        answer.remove()

    if not_iris:
        LOGGER.warning(
            f"The SPARQL query sent to '{endpoint}' bound ?{ENTITY_VARIABLE} to "
            f"something other than an IRI in {not_iris} solution(s), which were "
            "left out."
        )

    if index.concept_count:
        LOGGER.info(
            f"SPARQL endpoint '{endpoint}': indexed {index.concept_count} "
            f"entity(ies) by {len(index)} value(s)."
        )
    else:
        hint = ""
        if "GRAPH" not in query.upper():
            hint = (
                " If the knowledge graph keeps its entities in named graphs, read "
                "them with a 'GRAPH ?g { ... }' pattern: the default graph of many "
                "stores does not include them."
            )
        LOGGER.warning(
            f"The SPARQL query sent to '{endpoint}' returned no entity, so no "
            f"value will be reconciled against it.{hint}"
        )

    return index
