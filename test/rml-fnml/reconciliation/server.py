__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
A throwaway HTTP server used by the reconciliation tests: it serves the SKOS
vocabulary and answers SPARQL queries, both behind HTTP Basic Authentication,
so that the tests exercise the remote code paths without reaching the network.

The SPARQL endpoints evaluate queries over the knowledge graph of
``clinical_trials.trig``, which keeps every entity in a named graph of its own:

- ``/sparql`` has a default graph that does not include the named graphs, as
  Fuseki, Oxigraph or Stardog by default.
- ``/sparql/union`` has a default graph that is the union of the named graphs,
  as GraphDB or Virtuoso.
- ``/public/sparql`` is ``/sparql`` without authentication.
- ``/public/sparql/slow`` is ``/public/sparql`` taking longer than a second
  to answer.
- ``/public/sparql/gzip``, ``/public/sparql/xml`` and ``/public/sparql/no-head``
  are ``/public/sparql`` answering gzipped JSON, XML, and JSON without the head
  some endpoints leave out.
- ``/public/sparql/results-first`` gives the results of its answer before the
  head, as rdflib writes them, and ``/public/sparql/bad-term`` adds a solution
  with a language tag that is not BCP 47 (``en_US``), as some stores write.

The other endpoints answer in JSON with the head first, as most stores do.

``/vocabulary.gz`` serves the vocabulary gzipped, ``/vocabulary.broken-gz``
says it does but does not, ``/vocabulary.multi-gz`` serves it gzipped in two
members, as concatenated gzip files are, and ``/vocabulary.cut`` closes the
connection halfway through it. ``/export|vocabulary`` has a URL that is no IRI.
"""

import gzip
import json
import os
import threading
import time
from base64 import b64encode
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit

from rdflib import Dataset

USERNAME = 'vocabulary_user'
PASSWORD = 's3cr3t'

TEST_DIR = os.path.dirname(os.path.realpath(__file__))

# Served as text/plain, as some servers do with files they do not know.
VOCABULARY_FILES = {
    '/vocabulary': ('disease_vocabulary.ttl', 'text/turtle'),
    '/vocabulary.nq': ('disease_vocabulary.nq', 'text/plain'),
    '/vocabulary.trig': ('disease_vocabulary.trig', 'application/trig'),
    '/vocabulary.gz': ('disease_vocabulary.ttl', 'text/turtle'),
    '/vocabulary.broken-gz': ('disease_vocabulary.ttl', 'text/turtle'),
    '/vocabulary.multi-gz': ('disease_vocabulary.ttl', 'text/turtle'),
    '/vocabulary.cut': ('disease_vocabulary.ttl', 'text/turtle'),
    '/export|vocabulary': ('disease_vocabulary.ttl', 'text/turtle'),
}


def _knowledge_graph(default_union):
    dataset = Dataset(default_union=default_union)
    dataset.parse(os.path.join(TEST_DIR, 'clinical_trials.trig'), format='trig')
    return dataset


# SPARQL endpoint path -> whether its default graph is the union of the others.
SPARQL_ENDPOINTS = {
    '/sparql': False,
    '/sparql/union': True,
    '/public/sparql': False,
    '/public/sparql/slow': False,
    '/public/sparql/gzip': False,
    '/public/sparql/xml': False,
    '/public/sparql/no-head': False,
    '/public/sparql/results-first': False,
    '/public/sparql/bad-term': False,
}

# Paths answered without authentication.
PUBLIC_PREFIX = '/public/'


def _expected_authorization():
    credentials = b64encode(f'{USERNAME}:{PASSWORD}'.encode()).decode()
    return f'Basic {credentials}'


class _Handler(BaseHTTPRequestHandler):
    """Serves the vocabulary and the SPARQL endpoints."""

    # Requests reaching the server, so that tests can assert the vocabulary is
    # fetched (and each query sent) exactly once, and the HTTP methods they used.
    requests = []
    methods = []

    def do_GET(self):
        path = urlsplit(self.path).path
        query = parse_qs(urlsplit(self.path).query).get('query', [''])[0]
        self._dispatch(path, query)

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length).decode()
        query = parse_qs(body).get('query', [''])[0]
        self._dispatch(urlsplit(self.path).path, query)

    def _dispatch(self, path, query):
        if (
            not path.startswith(PUBLIC_PREFIX)
            and self.headers.get('Authorization') != _expected_authorization()
        ):
            self._respond(401, b'unauthorized', 'text/plain')
            return

        # A mapping checked out with Windows line endings sends its query with
        # them; the tests compare queries whatever the platform.
        type(self).requests.append((path, query.replace('\r\n', '\n')))
        type(self).methods.append(self.command)

        if path in VOCABULARY_FILES:
            file_name, content_type = VOCABULARY_FILES[path]
            with open(os.path.join(TEST_DIR, file_name), 'rb') as f:
                body = f.read()
            if path.endswith('.gz'):
                self._respond(200, gzip.compress(body), content_type, gzip=True)
            elif path.endswith('.broken-gz'):
                self._respond(200, body, content_type, gzip=True)
            elif path.endswith('.multi-gz'):
                half = len(body) // 2
                members = gzip.compress(body[:half]) + gzip.compress(body[half:])
                self._respond(200, members, content_type, gzip=True)
            elif path.endswith('.cut'):
                self._respond(200, body, content_type, cut=True)
            else:
                self._respond(200, body, content_type)
        elif path in SPARQL_ENDPOINTS:
            if path.endswith('/slow'):
                time.sleep(1.5)
            try:
                results = _knowledge_graph(SPARQL_ENDPOINTS[path]).query(query)
            except Exception as exc:
                self._respond(400, str(exc).encode(), 'text/plain')
                return
            if path.endswith('/xml'):
                self._respond(
                    200, results.serialize(format='xml'), 'application/sparql-results+xml',
                )
                return
            # rdflib writes the results before the head.
            body = results.serialize(format='json')
            answer = json.loads(body)
            if path.endswith('/no-head'):
                del answer['head']
            elif not path.endswith('/results-first'):
                answer = {'head': answer['head'], **answer}
            if path.endswith('/bad-term'):
                answer['results']['bindings'].append({
                    'entity_iri': {'type': 'uri', 'value': 'https://data.com/id/unmatched'},
                    'matching_value_1': {'type': 'literal', 'value': 'Unmatched', 'xml:lang': 'en_US'},
                })
            body = json.dumps(answer).encode()
            self._respond(
                200,
                gzip.compress(body) if path.endswith('/gzip') else body,
                'application/sparql-results+json',
                gzip=path.endswith('/gzip'),
            )
        else:
            self._respond(404, b'not found', 'text/plain')

    def _respond(self, status, body, content_type, gzip=False, cut=False):
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        if gzip:
            self.send_header('Content-Encoding', 'gzip')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        if cut:
            # Half the body, then the connection is closed.
            self.wfile.write(body[:len(body) // 2])
            self.close_connection = True
            return
        self.wfile.write(body)

    def log_message(self, *args):
        """Keep the test output clean."""


class VocabularyServer:
    """Context manager starting the server on an ephemeral port."""

    def __init__(self):
        self._server = None
        self._thread = None

    def __enter__(self):
        _Handler.requests = []
        _Handler.methods = []
        self._server = HTTPServer(('127.0.0.1', 0), _Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc_info):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    @property
    def url(self):
        host, port = self._server.server_address
        return f'http://{host}:{port}'

    @property
    def requests(self):
        return list(_Handler.requests)

    @property
    def methods(self):
        return list(_Handler.methods)
