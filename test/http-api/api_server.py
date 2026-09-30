__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
A throwaway HTTP API used by the HTTP API source tests: it answers with JSON
and requires a token, so that the tests exercise the adapter without reaching
the network. Every request it serves is recorded, which is how the tests assert
which headers and query parameters the adapter sent.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit

TOKEN = 'Bearer s3cr3t'

PEOPLE = {
    'results': [
        {'id': '10', 'name': 'Venus', 'department': {'name': 'Physics'}},
        {'id': '11', 'name': 'Frank', 'department': {'name': 'Chemistry'}},
    ]
}


class _Handler(BaseHTTPRequestHandler):
    """Serves /people to requests carrying the expected token."""

    # Requests reaching the server, as (path, headers, query parameters).
    requests = []

    def do_GET(self):
        url = urlsplit(self.path)
        parameters = {name: values[0] for name, values in parse_qs(url.query).items()}
        type(self).requests.append((url.path, dict(self.headers), parameters))

        if self.headers.get('Authorization') != TOKEN:
            self._respond(401, b'unauthorized', 'text/plain')
        elif url.path != '/people':
            self._respond(404, b'not found', 'text/plain')
        else:
            self._respond(200, json.dumps(PEOPLE).encode(), 'application/json')

    def _respond(self, status, body, content_type):
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        """Keep the test output clean."""


class ApiServer:
    """Context manager starting the API on an ephemeral port."""

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
        return f'http://{host}:{port}/people'

    @property
    def requests(self):
        return list(_Handler.requests)
