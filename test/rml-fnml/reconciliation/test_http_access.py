__author__ = "Julián Arenas-Guerrero"
__credits__ = ["Julián Arenas-Guerrero"]

__license__ = "Apache-2.0"
__maintainer__ = "Julián Arenas-Guerrero"
__email__ = "arenas.guerrero.julian@outlook.com"

"""
The HTTP client every remote resource is fetched with (SKOS vocabularies, SPARQL
endpoints and HTTP API sources): how it follows redirects with the credentials
and the body of a request, and how it rejects credentials written in a URL.

Each origin is a throwaway HTTP server on its own port of 127.0.0.1, which
records the requests it receives and answers them with the redirects a test
sets, or with 200 OK.
"""

import logging
import re
import socket
import threading
import traceback
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.request import Request

import pytest

from morph_kgc.constants import LOGGING_NAMESPACE
from morph_kgc.http import _RedirectHandler, basic_auth_header, fetch

USERNAME = 'vocabulary_user'
PASSWORD = 's3cr3t'
AUTHORIZATION = basic_auth_header(USERNAME, PASSWORD)['Authorization']

QUERY = b'query=SELECT+%3Fentity_iri+%3Fmatching_value_1+WHERE+%7B%7D'
FORM = 'application/x-www-form-urlencoded'

REDIRECT_CODES = [301, 302, 303, 307, 308]


@dataclass(frozen=True)
class Received:
    """A request as an origin received it."""
    method: str
    path: str
    authorization: str | None
    content_type: str | None
    body: bytes


class Origin:
    """Context manager starting a recording server on an ephemeral port."""

    def __init__(self):
        self.requests = []
        # The headers of each request, by lowercase name.
        self.received_headers = []
        self.redirects = {}
        self._server = None
        self._thread = None

    def reset(self):
        """Forget the requests received and the redirects set."""
        self.requests = []
        self.received_headers = []
        self.redirects = {}
        return self

    def redirect(self, path, code, location):
        """Answer requests for *path* with a *code* redirect to *location*."""
        self.redirects[path] = (code, location)

    def __enter__(self):
        self._server = HTTPServer(('127.0.0.1', 0), _handler(self))
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


def _handler(origin):
    """The request handler of *origin*, which records every request it answers."""
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            length = int(self.headers.get('Content-Length', 0))
            origin.requests.append(Received(
                method        = self.command,
                path          = self.path,
                authorization = self.headers.get('Authorization'),
                content_type  = self.headers.get('Content-Type'),
                body          = self.rfile.read(length),
            ))
            origin.received_headers.append(
                {name.lower(): value for name, value in self.headers.items()}
            )

            code, location = origin.redirects.get(self.path, (200, None))
            body = b'' if location else b'ok'
            self.send_response(code)
            if location:
                self.send_header('Location', location)
            self.send_header('Content-Type', 'text/plain')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_POST = do_GET

        def log_message(self, *args):
            """Keep the test output clean."""

    return Handler


@pytest.fixture(scope='module')
def servers():
    with Origin() as server, Origin() as other_server:
        yield server, other_server


@pytest.fixture
def origin(servers):
    return servers[0].reset()


@pytest.fixture
def other_origin(servers):
    return servers[1].reset()


# ── Credentials on redirects ──────────────────────────────────────────────────

@pytest.mark.parametrize('code', REDIRECT_CODES)
def test_credentials_are_kept_on_a_redirect_to_the_same_origin(origin, code):
    """A vocabulary moved within its server is still fetched with credentials."""
    origin.redirect('/id/vocabulary', code, '/files/vocabulary.ttl')

    response = fetch(f'{origin.url}/id/vocabulary', username=USERNAME, password=PASSWORD)

    assert response.body == b'ok'
    assert [(r.path, r.authorization) for r in origin.requests] == [
        ('/id/vocabulary', AUTHORIZATION),
        ('/files/vocabulary.ttl', AUTHORIZATION),
    ]


@pytest.mark.parametrize('code', REDIRECT_CODES)
def test_credentials_are_not_sent_to_another_origin(origin, other_origin, code):
    """The credentials of a resource never reach the server it redirects to."""
    origin.redirect('/vocabulary', code, f'{other_origin.url}/vocabulary')

    response = fetch(f'{origin.url}/vocabulary', username=USERNAME, password=PASSWORD)

    assert response.body == b'ok'
    assert [r.authorization for r in origin.requests] == [AUTHORIZATION]
    assert [(r.path, r.authorization) for r in other_origin.requests] == [
        ('/vocabulary', None),
    ]


def test_authorization_header_of_the_caller_is_not_sent_to_another_origin(
    origin, other_origin
):
    """An HTTP API source sets its own Authorization header, bound to its origin too."""
    origin.redirect('/people', 302, f'{other_origin.url}/people')

    fetch(f'{origin.url}/people', headers={'Authorization': 'Bearer an-api-key'})

    assert [r.authorization for r in origin.requests] == ['Bearer an-api-key']
    assert [r.authorization for r in other_origin.requests] == [None]


def test_api_key_of_the_caller_is_not_sent_to_another_origin(origin, other_origin):
    """
    The KeyId header of an HTTP API source is bound to its origin as well, while
    the headers that carry no credentials follow the redirect.
    """
    origin.redirect('/people', 302, f'{other_origin.url}/people')

    fetch(
        f'{origin.url}/people',
        accept  = 'application/json',
        headers = {'KeyId': 'an-api-key', 'User-Agent': 'morph-kgc'},
    )

    assert origin.received_headers[0]['keyid'] == 'an-api-key'
    assert 'keyid' not in other_origin.received_headers[0]
    assert other_origin.received_headers[0]['accept'] == 'application/json'
    assert other_origin.received_headers[0]['user-agent'] == 'morph-kgc'


def test_credentials_are_not_sent_back_after_leaving_the_origin(origin, other_origin):
    """Once dropped, the credentials stay dropped along a chain of redirects."""
    origin.redirect('/vocabulary', 302, f'{other_origin.url}/vocabulary')
    other_origin.redirect('/vocabulary', 302, f'{origin.url}/files/vocabulary.ttl')

    fetch(f'{origin.url}/vocabulary', username=USERNAME, password=PASSWORD)

    assert [(r.path, r.authorization) for r in origin.requests] == [
        ('/vocabulary', AUTHORIZATION),
        ('/files/vocabulary.ttl', None),
    ]


@pytest.mark.parametrize('url, redirect_url, kept', [
    ('https://example.org/sparql', 'https://example.org/sparql/', True),
    ('https://example.org/sparql', 'https://example.org:443/sparql/', True),
    ('https://example.org/sparql', 'HTTPS://EXAMPLE.ORG/sparql/', True),
    ('https://example.org/sparql', 'http://example.org/sparql', False),
    ('https://example.org/sparql', 'http://example.org:443/sparql', False),
    ('https://example.org/sparql', 'https://example.org:8443/sparql', False),
    ('https://example.org/sparql', 'https://www.example.org/sparql', False),
    ('https://example.org/sparql', 'https://example.org:99999/sparql', False),
    ('https://example.org/sparql', 'https://example.org:abc/sparql', False),
    ('http://example.org/sparql', 'https://example.org/sparql', True),
    ('http://example.org:80/sparql', 'https://example.org:443/sparql', True),
    ('http://example.org:8080/sparql', 'https://example.org/sparql', False),
    ('http://example.org/sparql', 'https://example.org:8443/sparql', False),
    ('http://example.org/sparql', 'https://www.example.org/sparql', False),
])
def test_authorization_is_only_sent_to_its_origin(url, redirect_url, kept):
    """
    Whether the Authorization header follows a redirect (from endpoints not
    served by these tests): only to the same scheme, host and port, never in
    clear text over http, and to https from http on the same host and default
    ports, where the credentials have already been sent.
    """
    request = Request(url, headers=basic_auth_header(USERNAME, PASSWORD))

    redirected = _RedirectHandler().redirect_request(
        request, None, 302, 'Found', {}, redirect_url
    )

    assert redirected.has_header('Authorization') is kept


@pytest.mark.parametrize('port', ['99999', 'abc'])
def test_redirect_to_an_invalid_port_is_reported(origin, port):
    """A Location with a port that is no port fails as any other unreachable URL."""
    origin.redirect('/vocabulary', 302, f'http://127.0.0.1:{port}/vocabulary')

    with pytest.raises(ValueError, match=(
        re.escape(f"Could not retrieve the vocabulary '{origin.url}/vocabulary': ")
    )):
        fetch(
            f'{origin.url}/vocabulary',
            username    = USERNAME,
            password    = PASSWORD,
            description = 'vocabulary',
        )


# ── POST requests on redirects ────────────────────────────────────────────────

@pytest.mark.parametrize('code', [301, 302, 307, 308])
def test_post_keeps_its_method_and_body_when_redirected(origin, code):
    """A SPARQL query sent by POST reaches the endpoint it is redirected to."""
    origin.redirect('/sparql', code, '/sparql/')

    response = fetch(
        f'{origin.url}/sparql',
        username     = USERNAME,
        password     = PASSWORD,
        data         = QUERY,
        content_type = FORM,
        method       = 'POST',
    )

    assert response.body == b'ok'
    assert origin.requests[1] == Received('POST', '/sparql/', AUTHORIZATION, FORM, QUERY)


@pytest.mark.parametrize('code', [301, 302, 307, 308])
def test_post_redirected_to_another_origin_keeps_its_body_but_not_credentials(
    origin, other_origin, code
):
    """The endpoint redirected to gets the query, but not the credentials."""
    origin.redirect('/sparql', code, f'{other_origin.url}/sparql')

    fetch(
        f'{origin.url}/sparql',
        username     = USERNAME,
        password     = PASSWORD,
        data         = QUERY,
        content_type = FORM,
        method       = 'POST',
    )

    assert other_origin.requests == [Received('POST', '/sparql', None, FORM, QUERY)]


def test_post_redirected_with_see_other_becomes_a_get(origin):
    """303 See Other points to another resource, which is retrieved with GET."""
    origin.redirect('/sparql', 303, '/results')

    fetch(
        f'{origin.url}/sparql',
        username     = USERNAME,
        password     = PASSWORD,
        data         = QUERY,
        content_type = FORM,
        method       = 'POST',
    )

    assert origin.requests[1] == Received('GET', '/results', AUTHORIZATION, None, b'')


# ── Credentials in URLs ───────────────────────────────────────────────────────

@pytest.mark.parametrize('url, shown_url', [
    ('http://kmax:p%40ss@127.0.0.1:8080/vocabulary',
     'http://kmax:***@127.0.0.1:8080/vocabulary'),
    ('http://kmax:p%40ss@127.0.0.1/vocabulary',
     'http://kmax:***@127.0.0.1/vocabulary'),
    ('http://kmax:p%3Ass@127.0.0.1/vocabulary',
     'http://kmax:***@127.0.0.1/vocabulary'),
    ('http://kmax@127.0.0.1/vocabulary',
     'http://kmax@127.0.0.1/vocabulary'),
])
def test_credentials_in_the_url_are_rejected(monkeypatch, caplog, url, shown_url):
    """
    A URL with userinfo is rejected before any request, since urllib would take
    it for part of the host name; its password is neither logged nor reported.
    """
    def no_network(host, *args, **kwargs):
        raise AssertionError(f'{host} was looked up')

    monkeypatch.setattr(socket, 'getaddrinfo', no_network)

    with caplog.at_level(logging.DEBUG, logger=LOGGING_NAMESPACE):
        with pytest.raises(ValueError, match='holds credentials') as error:
            fetch(url, description='vocabulary')

    assert f"'{shown_url}'" in str(error.value)
    reported = ''.join(traceback.format_exception(error.value)) + caplog.text
    for password in ('p%40ss', 'p@ss', 'p%3Ass', 'p:ss'):
        assert password not in reported
