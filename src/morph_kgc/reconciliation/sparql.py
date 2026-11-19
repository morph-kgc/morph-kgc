from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
SPARQL endpoint reconciliation
==============================
Builds a :class:`~morph_kgc.reconciliation.index.ConceptIndex` from the answer
of a SPARQL endpoint to a SELECT query of the mapping. The query may read any
knowledge graph, and projects:

- ``?entity_iri``, the entity a value is reconciled with.
- ``?matching_value_1``, the value the entity is matched by, and, for an entity
  matched by several values together, ``?matching_value_2`` and so on.

For instance::

    PREFIX schema: <https://schema.org/>
    SELECT ?entity_iri ?matching_value_1 WHERE {
        GRAPH ?g {
            ?entity_iri a schema:MedicalStudy ;
                        schema:identifier ?matching_value_1 .
        }
    }

An entity identified by a code within a coding system, which only the two
together identify, is matched by both::

    PREFIX schema: <https://schema.org/>
    SELECT ?entity_iri ?matching_value_1 ?matching_value_2 WHERE {
        GRAPH ?g {
            ?entity_iri a schema:MedicalCode ;
                        schema:codeValue ?matching_value_1 ;
                        schema:codingSystem ?matching_value_2 .
        }
    }

The execution of the mapping then binds as many values, matched to the
variables by position (``grel:valueParam1``, or ``grel:valueParam``, to
``?matching_value_1``, ``grel:valueParam2`` to ``?matching_value_2``...), and
a row is reconciled with the entities matched by all its values, compared one
by one: each is a column of the index, never joined with the others into a
string, which could make different tuples of values look alike.

The query is sent once, during the context initialization phase, and exactly as
written. Its answer is downloaded to the state directory of the run and read as
a stream into the index on disk, so that an answer of any size is indexed with
the same memory. An answer in JSON is only read as a stream when it gives its
head first, as most stores write it, and when every term in it is well formed;
otherwise it is read whole into memory, with a warning. Whether its patterns
read the named graphs of the endpoint is up to the query, since stores differ on
whether their default graph includes them. Values are matched as the
execution says, exactly by default (see ``morph-fn:matching``), by the same
normalization as against a vocabulary file, applied here to the values of the
answer, which the query returns as they are: the answer is indexed once for
every way of matching values of the executions sending the query, which is sent
once. Answers in XML are read by the standard library, which keeps the
whitespace a literal starts or ends with, which pyoxigraph's parser trims, and
reads the carriage returns and the control characters stores write as they are
as such (see ``_XmlLiteralFile``).

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
import io
import json
import logging
import re
from http.client import HTTPException
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from xml.etree import ElementTree

import pyoxigraph

from .._version import __version__
from ..constants import GREL_VALUE_PARAM, LOGGING_NAMESPACE
from ..http import Download, download
from .index import EXACT_MATCHING, ConceptIndex, ConceptIndexBuilder

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

SPARQL_RESULTS_JSON = "application/sparql-results+json"

# The serializations of SPARQL results read as a stream, by content type. JSON
# is the one asked for; the others are read too, should an endpoint send them.
# XML is read by the standard library (see _read_xml_solutions).
RESULTS_FORMATS = {
    SPARQL_RESULTS_JSON:               pyoxigraph.QueryResultsFormat.JSON,
    "application/json":                pyoxigraph.QueryResultsFormat.JSON,
    "application/sparql-results+xml":  pyoxigraph.QueryResultsFormat.XML,
    "application/xml":                 pyoxigraph.QueryResultsFormat.XML,
    "text/xml":                        pyoxigraph.QueryResultsFormat.XML,
    "text/tab-separated-values":       pyoxigraph.QueryResultsFormat.TSV,
}

# The namespace of the elements of SPARQL results in XML.
XML_RESULTS_NAMESPACE = "{http://www.w3.org/2005/sparql-results#}"

# The kinds of terms a value an entity is matched by may be: an IRI or a
# literal, as the values of a vocabulary are ('typed-literal' in the JSON
# results of SPARQL 1.0). A blank node, whose label only means something in
# the answer, is no value.
VALUE_KINDS = {"uri", "literal", "typed-literal"}

# The variables the query projects: the entity, and the values it is matched by,
# numbered from 1.
ENTITY_VARIABLE   = "entity_iri"
MATCHING_VARIABLE_PREFIX = "matching_value_"
MATCHING_VARIABLE = f"{MATCHING_VARIABLE_PREFIX}1"
MATCHING_VARIABLES = re.compile(rf"{MATCHING_VARIABLE_PREFIX}(\d+)")
# Those numbered as positions are: from 1, without leading zeros.
POSITIONED_VARIABLES = re.compile(rf"{MATCHING_VARIABLE_PREFIX}([1-9][0-9]*)")

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


def matching_variables(arity: int = 1) -> list[str]:
    """The variables matched against the *arity* values a row is reconciled by."""
    return [f"{MATCHING_VARIABLE_PREFIX}{position}" for position in range(1, arity + 1)]


def read_solutions(endpoint: str, answer: Download, arity: int = 1):
    """
    The variables the answer of *endpoint* in the file *answer* projects, if
    it says, and an iterator of its solutions, read as a stream: for each, the
    term bound to ?entity_iri, and the tuple of the terms bound to the *arity*
    variables ?matching_value_1, ?matching_value_2..., as (kind, value) pairs
    whose kind is ``uri`` for an IRI.
    """
    json_format = pyoxigraph.QueryResultsFormat.JSON
    results_format = RESULTS_FORMATS.get(answer.content_type, json_format)
    if results_format == pyoxigraph.QueryResultsFormat.XML:
        return _read_xml_solutions(endpoint, answer, arity)
    return _read_parsed_solutions(endpoint, answer, arity, results_format)


def _read_parsed_solutions(endpoint: str, answer: Download, arity: int, results_format):
    """
    The variables the answer of *endpoint* in the file *answer*, serialized in
    *results_format*, projects and its solutions, as :func:`read_solutions`
    gives them, read as a stream by pyoxigraph.
    """
    json_format = pyoxigraph.QueryResultsFormat.JSON
    try:
        results = pyoxigraph.parse_query_results(path=answer.path, format=results_format)
    except SyntaxError as exc:
        if results_format != json_format:
            raise _not_results(endpoint, answer, exc) from exc
        # The head is mandatory in SPARQL results, but an endpoint omitting it
        # is not worth failing for: the bindings then speak for the projection.
        return _read_json_solutions(endpoint, answer, exc, arity)

    if not isinstance(results, pyoxigraph.QuerySolutions):
        raise _not_results(endpoint, answer, "it is the answer to an ASK query")

    if results_format == json_format and not _starts_with_head(answer.path):
        # The parser holds the solutions until it reads which variables they bind.
        LOGGER.warning(
            f"The answer of the SPARQL endpoint '{endpoint}' gives its results "
            "before its head, so it is read whole into memory before it is indexed."
        )

    variables = [variable.value for variable in results.variables]
    matched = matching_variables(arity)
    if any(variable not in variables for variable in (ENTITY_VARIABLE, *matched)):
        # check_variables says which is missing.
        return variables, iter(())
    # By position, much faster than by name over millions of solutions.
    entity = variables.index(ENTITY_VARIABLE)
    values = [variables.index(variable) for variable in matched]

    def solutions():
        try:
            for solution in results:
                yield (
                    _solution_term(solution[entity]),
                    tuple(_solution_term(solution[value]) for value in values),
                )
        except SyntaxError as exc:
            if results_format == json_format:
                # A term the streaming parser rejects and json.load may not: a
                # Virtuoso 'nodeID://' blank node, a language tag that is not
                # BCP 47, an IRI with a space.
                raise _StreamingError(exc) from exc
            raise _not_results(endpoint, answer, exc) from exc

    return variables, solutions()


class _StreamingError(Exception):
    """
    The streaming parser rejects a solution of an answer in JSON or in XML,
    which is then read anew another way (see :func:`_read_anew`).
    """


def _read_anew(endpoint: str, answer: Download, reason, arity: int = 1):
    """
    The variables and the solutions of an answer the streaming parser of its
    serialization rejected partway through, for *reason*, read another way: an
    answer in JSON whole into memory, and one in XML by pyoxigraph.
    """
    if RESULTS_FORMATS.get(answer.content_type) == pyoxigraph.QueryResultsFormat.XML:
        return _read_lenient_xml_solutions(endpoint, answer, reason, arity)
    return _read_json_solutions(endpoint, answer, reason, arity)


def _read_lenient_xml_solutions(endpoint: str, answer: Download, reason, arity: int = 1):
    """
    The variables and the solutions of an answer in XML that is not well-formed,
    for *reason*, read by pyoxigraph, which reads characters XML has no room for,
    but trims the whitespace a literal starts or ends with.
    """
    LOGGER.warning(
        f"The answer in XML of the SPARQL endpoint '{endpoint}' is not well-formed "
        f"({reason}), so it is read by a lenient parser, which trims the whitespace "
        "a literal starts or ends with: values matched as they are, or lower-cased, "
        "may then be matched otherwise than against a vocabulary file. Answers in "
        "JSON keep it."
    )
    return _read_parsed_solutions(endpoint, answer, arity, pyoxigraph.QueryResultsFormat.XML)


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
    if isinstance(term, pyoxigraph.Literal):
        return "literal", term.value
    if isinstance(term, pyoxigraph.BlankNode):
        return "bnode", term.value
    # A triple term has no value of its own to match.
    return "triple", None


def _read_xml_solutions(endpoint: str, answer: Download, arity: int = 1):
    """
    The variables an answer in XML projects and its solutions, as
    :func:`read_solutions` gives them, read as a stream by the standard
    library, which keeps the text of every literal as it is: the parser of
    pyoxigraph trims the whitespace a literal starts or ends with, which a
    value is matched by as well. The characters of literals XML cannot read as
    they are written (see :class:`_XmlLiteralFile`) are read as written too. The
    answer is read twice, its head first, so that its file is only open while
    its solutions are read. An answer holding a character the standard library
    still rejects is read by pyoxigraph, as it was before.
    """
    variables = None
    try:
        with _XmlLiteralFile(answer.path) as answer_file:
            # The head, and whether results or a boolean, the answer to an ASK
            # query, come after it.
            for event, element in ElementTree.iterparse(answer_file, events=("start", "end")):
                tag = _xml_tag(element)
                if event == "end" and tag == "head":
                    variables = [
                        variable.get("name")
                        for variable in element
                        if _xml_tag(variable) == "variable"
                    ]
                elif event == "start" and tag == "boolean":
                    raise _not_results(endpoint, answer, "it is the answer to an ASK query")
                elif event == "start" and tag == "results":
                    break
    except ElementTree.ParseError as exc:
        if exc.code in _XML_CHARACTER_ERRORS:
            return _read_lenient_xml_solutions(endpoint, answer, exc, arity)
        raise _not_results(endpoint, answer, exc) from exc
    if variables is None:
        raise _not_results(endpoint, answer, "it has no head before its results")

    matched = matching_variables(arity)
    if any(variable not in variables for variable in (ENTITY_VARIABLE, *matched)):
        # check_variables says which is missing.
        return variables, iter(())

    def solutions():
        try:
            with _XmlLiteralFile(answer.path) as answer_file:
                results = None
                for event, element in ElementTree.iterparse(answer_file, events=("start", "end")):
                    tag = _xml_tag(element)
                    if event == "start" and tag == "results":
                        results = element
                    elif event == "end" and tag == "result":
                        bound = {
                            binding.get("name"): _xml_term(binding)
                            for binding in element
                            if _xml_tag(binding) == "binding"
                        }
                        # The solutions read are dropped, so that memory does not
                        # grow with the size of the answer.
                        if results is not None:
                            results.clear()
                        yield (
                            bound.get(ENTITY_VARIABLE),
                            tuple(bound.get(variable) for variable in matched),
                        )
        except ElementTree.ParseError as exc:
            if exc.code in _XML_CHARACTER_ERRORS:
                # Read anew by pyoxigraph, from the start (see build_index).
                raise _StreamingError(exc) from exc
            raise _not_results(endpoint, answer, exc) from exc

    return variables, solutions()


# The characters of a literal that an answer in XML may hold, as it is written
# by stores such as Oxigraph and rdflib, but XML parsers cannot read as they
# are written: carriage returns, which XML reads as line feeds, and the
# characters XML 1.0 has no room for (control characters, U+FFFE and U+FFFF),
# written as they are or as character references.
_XML_UNREADABLE_CODES = (
    set(range(0x00, 0x09)) | {0x0B, 0x0C, 0x0D} | set(range(0x0E, 0x20)) | {0xFFFE, 0xFFFF}
)
# The errors of an XML parser rejecting a character, which pyoxigraph reads:
# not well-formed (invalid token), and reference to invalid character number.
# The answer is then read by pyoxigraph; any other error, such as an answer cut
# short, which pyoxigraph may read in part without saying so, fails.
_XML_CHARACTER_ERRORS = {4, 14}
# The private-use characters standing in for them while the answer is read, at
# the end of the last plane of Unicode: U+10FF00 plus their code (plus 0xFE and
# 0xFF for U+FFFE and U+FFFF). A literal holding one of them, or the character
# escaping them, U+10FF20, holds it escaped, preceded by U+10FF20, so that any
# literal is read as written.
_XML_PLACEHOLDER_BASE = 0x10FF00
_XML_ESCAPE = chr(0x10FF20)
_XML_FROM_PLACEHOLDERS = {
    _XML_PLACEHOLDER_BASE + (code & 0xFF): code for code in _XML_UNREADABLE_CODES
}
_XML_ESCAPED_CODES = set(_XML_FROM_PLACEHOLDERS) | {ord(_XML_ESCAPE)}
# What is rewritten in the content of a literal: control characters (carriage
# returns included), U+FFFE and U+FFFF, the placeholders and the escape (in
# UTF-8), and character references, of which only those to any of them are.
_XML_REWRITTEN = re.compile(
    rb"[\x00-\x08\x0b-\x1f]"
    rb"|\xef\xbf[\xbe\xbf]"
    rb"|\xf4\x8f\xbc[\x80-\xa0]|\xf4\x8f\xbf[\xbe\xbf]"
    rb"|&#(?:x0*([0-9a-fA-F]{1,6})|0*([0-9]{1,7}));"
)
# Every byte but control characters, to tell quickly whether content holds any.
_XML_NOT_CONTROL = bytes(
    byte for byte in range(256) if byte in (0x09, 0x0A) or byte >= 0x20
)
# The placeholders and the escape, in a literal as read, and how they are read.
_XML_MARKED = re.compile(f"[{chr(_XML_PLACEHOLDER_BASE)}-{_XML_ESCAPE}\U0010fffe\U0010ffff]")
_XML_UNMARKED = re.compile(
    f"{_XML_ESCAPE}(.)|[{chr(_XML_PLACEHOLDER_BASE)}-{chr(_XML_PLACEHOLDER_BASE + 0x1F)}"
    "\U0010fffe\U0010ffff]",
    re.DOTALL,
)
# The start of the start tag of a literal, whatever the prefix of its namespace.
_XML_LITERAL_START = re.compile(rb"""<(?:[^\s<>/:=!?"']+:)?literal(?=[\s/>])""")
# An XML declaration naming the encoding of the answer, which is only rewritten
# in UTF-8 (or ASCII, a part of it).
_XML_DECLARED_ENCODING = re.compile(
    rb"""^(?:\xef\xbb\xbf)?\s*<\?xml[^>]*?encoding\s*=\s*["']([A-Za-z0-9._-]+)["']"""
)
_XML_REWRITTEN_ENCODINGS = {b"utf-8", b"utf8", b"us-ascii", b"ascii"}


def _xml_rewritten(match) -> bytes:
    """
    What *match*, in the content of a literal, is rewritten as: the placeholder
    of a character XML cannot read as written, or a character escaped.
    """
    written = match.group(0)
    hexadecimal, decimal = match.group(1), match.group(2)
    if hexadecimal is not None or decimal is not None:
        code = int(hexadecimal, 16) if hexadecimal is not None else int(decimal)
        if code in _XML_ESCAPED_CODES:
            return (_XML_ESCAPE + chr(code)).encode("utf-8")
        if code not in _XML_UNREADABLE_CODES or code == 0x0D:
            # Read as it is, a carriage return written as a reference included.
            return written
    elif written.startswith(b"\xf4"):
        return _XML_ESCAPE.encode("utf-8") + written
    elif written.startswith(b"\xef"):
        code = 0xFFFE if written == b"\xef\xbf\xbe" else 0xFFFF
    else:
        code = written[0]
    return chr(_XML_PLACEHOLDER_BASE + (code & 0xFF)).encode("utf-8")


def _xml_literal_text(text: str) -> str:
    """The text of a literal as written, its placeholders and escapes read."""
    if text.isascii() or not _XML_MARKED.search(text):
        return text
    return _XML_UNMARKED.sub(
        lambda match: match.group(1)
        if match.group(1) is not None
        else chr(_XML_FROM_PLACEHOLDERS[ord(match.group(0))]),
        text,
    )


class _XmlLiteralFile(io.RawIOBase):
    """
    The file of an answer in XML, encoded in UTF-8, read with the characters of
    its literals XML parsers cannot read as they are written (see
    _XML_UNREADABLE_CODES) replaced with private-use placeholders, which
    :func:`_xml_literal_text` turns back: a literal holding them is then read
    as it is written, as from an answer in JSON, rather than failing the whole
    answer or turning its carriage returns into line feeds. Nothing outside
    the content of literals is changed, and an answer in another encoding is
    read as it is. It is read as a stream, as the answer is.
    """

    def __init__(self, path: str, chunk_size: int = 1 << 16) -> None:
        super().__init__()
        self._file = open(path, "rb")
        self._chunk_size = chunk_size
        self._pending = b""
        self._output = bytearray()
        self._in_literal = False
        self._end = False
        # The start of the answer, read to tell its encoding, then as the rest.
        self._start = self._file.read(1024)
        declared = _XML_DECLARED_ENCODING.match(self._start)
        self._rewritten = not (
            # UTF-16 or UTF-32, with or without a byte order mark.
            self._start[:2] in (b"\xff\xfe", b"\xfe\xff")
            or b"\x00" in self._start[:4]
            or (declared is not None and declared.group(1).lower() not in _XML_REWRITTEN_ENCODINGS)
        )

    def readable(self) -> bool:
        return True

    def readinto(self, buffer) -> int:
        while not self._output and not (self._end and not self._pending):
            self._fill()
        size = min(len(buffer), len(self._output))
        buffer[:size] = self._output[:size]
        del self._output[:size]
        return size

    def close(self) -> None:
        if not self.closed:
            self._file.close()
        super().close()

    def _fill(self) -> None:
        if self._start:
            chunk, self._start = self._start[:self._chunk_size], self._start[self._chunk_size:]
        else:
            chunk = self._file.read(self._chunk_size)
        self._end = not chunk
        data = self._pending + chunk
        if not self._rewritten:
            self._output += data
            self._pending = b""
            return
        position = 0
        while position < len(data):
            if not self._in_literal:
                match = _XML_LITERAL_START.search(data, position)
                end = data.find(b">", match.end()) if match else -1
                if match is None or end == -1:
                    if self._end:
                        keep = len(data)
                    elif match is not None:
                        keep = match.start()
                    else:
                        # A tag left unfinished may be the start tag of a literal.
                        last = data.rfind(b"<", position)
                        keep = last if last != -1 and data.find(b">", last) == -1 else len(data)
                    self._output += data[position:keep]
                    position = keep
                    break
                self._output += data[position:end + 1]
                position = end + 1
                # An empty literal, written '<literal/>', has no content.
                self._in_literal = data[end - 1:end] != b"/"
            else:
                # The content of a literal holds no markup, but its end tag.
                close = data.find(b"</", position)
                stop = len(data) if close == -1 else close
                if close == -1 and not self._end:
                    # What is left may be the start of the end tag, of a
                    # character reference or of a character in UTF-8.
                    ampersand = data.rfind(b"&", position)
                    if ampersand != -1 and data.find(b";", ampersand) == -1:
                        stop = ampersand
                    if data.endswith(b"<"):
                        stop = min(stop, len(data) - 1)
                    for lead in (b"\xef", b"\xf4"):
                        start = data.rfind(lead, max(position, len(data) - 3))
                        if start != -1:
                            stop = min(stop, start)
                content = data[position:stop]
                if (
                    b"&#" in content or b"\xef\xbf" in content or b"\xf4\x8f" in content
                    or content.translate(None, _XML_NOT_CONTROL)
                ):
                    content = _XML_REWRITTEN.sub(_xml_rewritten, content)
                self._output += content
                position = stop
                if close == -1:
                    break
                self._in_literal = False
        self._pending = data[position:]


def _xml_tag(element) -> str:
    """The name of an element of SPARQL results in XML, without its namespace."""
    return element.tag.removeprefix(XML_RESULTS_NAMESPACE)


def _xml_term(binding) -> tuple[str, str | None] | None:
    """The kind and value of the term of a binding of SPARQL results in XML."""
    for term in binding:
        kind = _xml_tag(term)
        if kind == "literal":
            # Exactly as written, whitespace included (see _XmlLiteralFile).
            return "literal", _xml_literal_text(term.text or "")
        if kind in ("uri", "bnode"):
            return kind, (term.text or "").strip()
        # A triple term has no value of its own to match.
        return kind, None
    return None


def _read_json_solutions(endpoint: str, answer: Download, reason, arity: int = 1):
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

    matched = matching_variables(arity)

    if variables is None:
        # Without a head, the bindings speak for the projection: a value the
        # query matches entities by, but the mapping does not reconcile any
        # value with, would otherwise be left out unnoticed, matching rows by
        # some of the values of entities only.
        unmatched = sorted(
            variable
            for variable in {key for binding in bindings if isinstance(binding, dict) for key in binding}
            if MATCHING_VARIABLES.fullmatch(variable) and variable not in matched
        )
        if unmatched:
            raise ValueError(
                f"The answer of the SPARQL endpoint '{endpoint}' binds "
                f"{' '.join(f'?{variable}' for variable in unmatched)}, so its query "
                f"matches entities by values the mapping does not reconcile: it "
                f"reconciles {arity} value(s) with it, "
                f"{' '.join(f'?{variable}' for variable in matched)}. {_BIND_VALUES}"
            )

    def solutions():
        for binding in bindings:
            yield (
                term(binding.get(ENTITY_VARIABLE)),
                tuple(term(binding.get(variable)) for variable in matched),
            )

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


def check_variables(endpoint: str, variables, arity: int = 1) -> None:
    """
    Make sure the query projects the variables reconciliation reads, rather than
    letting a misspelled one silently reconcile every value against nothing:
    ?entity_iri, and one ?matching_value_<i> for each of the *arity* values the
    mapping matches entities by, numbered from 1.
    """
    projected = " ".join(f"?{variable}" for variable in variables) or "nothing"
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
            f"projects: {projected}."
        )

    numbered = [
        variable for variable in variables if MATCHING_VARIABLES.fullmatch(variable)
    ]
    misnumbered = [
        variable for variable in numbered if not POSITIONED_VARIABLES.fullmatch(variable)
    ]
    if misnumbered:
        raise ValueError(
            f"The SPARQL query sent to '{endpoint}' projects "
            f"{' '.join(f'?{variable}' for variable in misnumbered)}, which is not "
            "numbered as the values an entity is matched by are: "
            f"{', '.join(f'?{variable}' for variable in matching_variables(3))}... "
            f"from 1, without leading zeros. It projects: {projected}."
        )

    positions = sorted(
        int(POSITIONED_VARIABLES.fullmatch(variable).group(1)) for variable in numbered
    )
    gap = next(
        (number for number, position in enumerate(positions, 1) if number != position), None,
    )
    if gap is not None:
        raise ValueError(
            f"The SPARQL query sent to '{endpoint}' projects "
            f"?{MATCHING_VARIABLE_PREFIX}{positions[-1]} but not "
            f"?{MATCHING_VARIABLE_PREFIX}{gap}: the values an entity is matched by "
            f"are numbered from 1, without gaps. It projects: {projected}."
        )

    if len(numbered) != arity:
        alternatives = ""
        if arity == 1:
            alternatives = (
                " To match a value against any of several properties instead, bind "
                f"them all to ?{MATCHING_VARIABLE}, e.g. with a UNION."
            )
        raise ValueError(
            f"The SPARQL query sent to '{endpoint}' matches entities by "
            f"{len(numbered)} value(s), "
            f"{' '.join(f'?{variable}' for variable in matching_variables(len(numbered)))}, "
            f"but the mapping reconciles {arity} value(s) with it. "
            f"{_BIND_VALUES}{alternatives}"
        )


# How the values to reconcile are bound to the variables of the query.
_BIND_VALUES = (
    "Bind one value to reconcile per variable, matched by position: "
    f"'{GREL_VALUE_PARAM}1' (or '{GREL_VALUE_PARAM}') to ?{MATCHING_VARIABLE}, "
    f"'{GREL_VALUE_PARAM}2' to ?{MATCHING_VARIABLE_PREFIX}2, and so on."
)


def _index_solutions(
    solutions, state_dir: str, memory_limit: str, arity: int = 1, matchings=(EXACT_MATCHING,),
):
    """
    The index of *solutions* for each of *matchings*, by matching, and how many
    bound ?entity_iri to no IRI.
    """
    not_iris = 0
    with ConceptIndexBuilder(state_dir, memory_limit, *matchings, arity=arity) as builder:
        for entity, values in solutions:
            # Unbound in this solution, e.g. by an OPTIONAL pattern, or bound
            # to no IRI or literal: an entity is only matched by all its values.
            if entity is None or any(
                value is None or value[0] not in VALUE_KINDS or value[1] is None
                for value in values
            ):
                continue
            # A blank node or a literal identifies no entity outside the answer.
            if entity[0] != "uri":
                not_iris += 1
                continue
            # The answer says nothing of attributes: the values are indexed
            # under a single, unnamed one.
            matched = values[0][1] if arity == 1 else tuple(value[1] for value in values)
            for matching in matchings:
                builder.add("", matched, entity[1], matching)
        return {matching: builder.build(matching=matching) for matching in matchings}, not_iris


def build_index(
    resource,
    query: str,
    *,
    state_dir: str,
    memory_limit: str,
    arity: int = 1,
    matchings=(EXACT_MATCHING,),
) -> dict[str, ConceptIndex]:
    """
    Build the index of *query* sent to the endpoint of *resource*, in
    *state_dir*, for each of *matchings*, by matching, matching entities by the
    *arity* values the mapping binds. The query is sent once for all of them.
    """
    endpoint = resource.get_url()
    answer = run_query(resource, query, state_dir)
    solutions = None
    try:
        variables, solutions = read_solutions(endpoint, answer, arity)
        if variables is not None:
            check_variables(endpoint, variables, arity)

        try:
            indexes, not_iris = _index_solutions(
                solutions, state_dir, memory_limit, arity, matchings,
            )
        except _StreamingError as exc:
            # Indexed anew from the start, by a new builder.
            _, solutions = _read_anew(endpoint, answer, exc, arity)
            indexes, not_iris = _index_solutions(
                solutions, state_dir, memory_limit, arity, matchings,
            )
    finally:
        # A reader of the answer left halfway through closes its file, which
        # Windows cannot remove while it is open.
        if hasattr(solutions, "close"):
            solutions.close()
        answer.remove()

    if not_iris:
        LOGGER.warning(
            f"The SPARQL query sent to '{endpoint}' bound ?{ENTITY_VARIABLE} to "
            f"something other than an IRI in {not_iris} solution(s), which were "
            "left out."
        )

    by = "value(s)" if arity == 1 else f"tuple(s) of {arity} values"
    if any(index.concept_count for index in indexes.values()):
        for matching, index in indexes.items():
            LOGGER.info(
                f"SPARQL endpoint '{endpoint}': indexed {index.concept_count} "
                f"entity(ies) by {len(index)} {by}, for {matching.lower()} matching."
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

    return indexes
