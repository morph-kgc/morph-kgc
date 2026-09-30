__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
A throwaway HTTP server used by the reconciliation tests: it serves the SKOS
vocabulary and answers SPARQL queries, both behind HTTP Basic Authentication,
so that the tests exercise the remote code paths without reaching the network.
"""

import json
import os
import threading
from base64 import b64encode
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit

USERNAME = 'vocabulary_user'
PASSWORD = 's3cr3t'

TEST_DIR = os.path.dirname(os.path.realpath(__file__))

# The bindings the fake endpoint answers every SELECT query with.
SPARQL_BINDINGS = [
    {
        'concept':   {'type': 'uri', 'value': 'https://data.boehringer.com/id/00036/10073037'},
        'attribute': {'type': 'uri', 'value': 'http://www.w3.org/2004/02/skos/core#altLabel'},
        'value':     {'type': 'literal', 'value': 'Early-onset spastic ataxia-myoclonic epilepsy-neuropathy syndrome'},
    },
    {
        'concept':   {'type': 'uri', 'value': 'https://data.boehringer.com/id/00036/10077339'},
        'attribute': {'type': 'uri', 'value': 'http://www.w3.org/2004/02/skos/core#prefLabel'},
        'value':     {'type': 'literal', 'value': 'Autosomal dominant cerebellar ataxia-deafness-narcolepsy syndrome'},
    },
]


def _expected_authorization():
    credentials = b64encode(f'{USERNAME}:{PASSWORD}'.encode()).decode()
    return f'Basic {credentials}'


class _Handler(BaseHTTPRequestHandler):
    """Serves /vocabulary and /sparql, both requiring Basic Authentication."""

    # Requests reaching the server, so that tests can assert the vocabulary is
    # fetched exactly once.
    requests = []

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
        if self.headers.get('Authorization') != _expected_authorization():
            self._respond(401, b'unauthorized', 'text/plain')
            return

        type(self).requests.append((path, query))

        if path == '/vocabulary':
            with open(os.path.join(TEST_DIR, 'disease_vocabulary.ttl'), 'rb') as f:
                self._respond(200, f.read(), 'text/turtle')
        elif path == '/sparql':
            results = {
                'head': {'vars': ['concept', 'attribute', 'value']},
                'results': {'bindings': SPARQL_BINDINGS},
            }
            self._respond(
                200,
                json.dumps(results).encode(),
                'application/sparql-results+json',
            )
        else:
            self._respond(404, b'not found', 'text/plain')

    def _respond(self, status, body, content_type):
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
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
