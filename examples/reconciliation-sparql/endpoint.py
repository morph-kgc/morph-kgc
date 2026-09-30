"""
A minimal SPARQL endpoint, so that this example runs without a triplestore.

It answers SELECT queries over `diseases.ttl` with SPARQL results in JSON, which
is what Morph-KGC asks a `SPARQL_ENDPOINT` resource for. `run.py` starts it on
its own; start it by hand to run the example from the command line instead:

    python endpoint.py
    morph-kgc config.ini

Every request it serves is logged, which shows that the endpoint is queried once
for the whole materialization. Nothing in this file is needed to reconcile
against a real endpoint: only the `url` of the resource has to point at it.
"""

import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit

import rdflib

HOST = '127.0.0.1'
PORT = 8890
PATH = '/sparql'
URL = f'http://{HOST}:{PORT}{PATH}'

SPARQL_RESULTS_JSON = 'application/sparql-results+json'

DATASET = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'diseases.ttl')
GRAPH = rdflib.Graph().parse(DATASET)


class _SPARQLHandler(BaseHTTPRequestHandler):
    """Evaluates the queries Morph-KGC sends, over GET and over POST."""

    def do_GET(self):
        url = urlsplit(self.path)
        self._answer(url.path, parse_qs(url.query).get('query', [''])[0])

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get('Content-Length', 0))).decode()
        self._answer(urlsplit(self.path).path, parse_qs(body).get('query', [''])[0])

    def _answer(self, path, query):
        if path != PATH:
            self._respond(404, b'not found', 'text/plain')
        elif not query:
            self._respond(400, b'no query given', 'text/plain')
        else:
            self._respond(
                200,
                GRAPH.query(query).serialize(format='json'),
                SPARQL_RESULTS_JSON,
            )

    def _respond(self, status, body, content_type):
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def create():
    """The HTTP server answering SPARQL queries over the dataset."""
    return HTTPServer((HOST, PORT), _SPARQLHandler)


def start():
    """Serve in the background and return the server, to be shut down later."""
    server = create()
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


if __name__ == '__main__':
    print(f"SPARQL endpoint answering queries over '{DATASET}' at {URL}")
    create().serve_forever()
