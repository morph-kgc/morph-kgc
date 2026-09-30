"""
A minimal HTTP API, so that this example runs without an API of its own.

It answers GET /observations with JSON, and only to requests carrying the token
the API expects, which is what the `Authorization` header of the mapping and
`api_token.py` are for. `run.py` starts it; start it by hand to run the example
from the command line instead:

    python api.py
    morph-kgc config.ini

Every request it serves is logged, headers and query parameters included, which
shows what Morph-KGC sent. Nothing in this file is needed to read a real API:
only the `htv:absoluteURI` of the mapping has to point at it.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit

HOST = '127.0.0.1'
PORT = 8891
PATH = '/observations'
URL = f'http://{HOST}:{PORT}{PATH}'

# The token the API expects. `api_token.py` is what hands it to Morph-KGC.
TOKEN = 'Bearer s3cr3t'

OBSERVATIONS = {
    'observations': [
        {
            'id': 'obs-0001',
            'pollutant': 'NO2',
            'value': '34.5',
            'timestamp': '2026-01-27T08:00:00Z',
            'station': {'id': 'ST-0028', 'name': 'Plaza de España'},
        },
        {
            'id': 'obs-0002',
            'pollutant': 'PM10',
            'value': '18.2',
            'timestamp': '2026-01-27T08:00:00Z',
            'station': {'id': 'ST-0028', 'name': 'Plaza de España'},
        },
        {
            'id': 'obs-0003',
            'pollutant': 'NO2',
            'value': '52.1',
            'timestamp': '2026-01-27T09:00:00Z',
            'station': {'id': 'ST-0035', 'name': 'Escuelas Aguirre'},
        },
        {
            'id': 'obs-0004',
            'pollutant': 'O3',
            'value': '61.0',
            'timestamp': '2026-01-27T09:00:00Z',
            'station': {'id': 'ST-0035', 'name': 'Escuelas Aguirre'},
        },
    ]
}


class _Handler(BaseHTTPRequestHandler):
    """Answers the observations to requests carrying the expected token."""

    def do_GET(self):
        url = urlsplit(self.path)
        parameters = {name: values[0] for name, values in parse_qs(url.query).items()}
        print(
            f'{self.command} {url.path} '
            f'authorization={self.headers.get("Authorization")!r} '
            f'parameters={parameters}'
        )

        if self.headers.get('Authorization') != TOKEN:
            self._respond(401, b'unauthorized', 'text/plain')
        elif url.path != PATH:
            self._respond(404, b'not found', 'text/plain')
        else:
            self._respond(200, json.dumps(OBSERVATIONS).encode(), 'application/json')

    def _respond(self, status, body, content_type):
        self.send_response(status)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        """Requests are logged by the handler itself, in one line each."""


def create():
    """The HTTP server answering the observations."""
    return HTTPServer((HOST, PORT), _Handler)


def start():
    """Serve in the background and return the server, to be shut down later."""
    server = create()
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


if __name__ == '__main__':
    print(f'Serving the observations at {URL}')
    create().serve_forever()
