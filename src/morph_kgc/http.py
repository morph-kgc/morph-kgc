from __future__ import annotations

__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
HTTP access
===========
The HTTP client of the engine: the resources declared in ``[RESOURCE:<name>]``
config sections and the stateful functions that access them, and the HTTP API
data sources. It is built on ``urllib`` so that fetching a vocabulary, querying
a SPARQL endpoint or reading an HTTP API needs no dependency beyond the
standard library.

Supports HTTP Basic Authentication (preemptive: the ``Authorization`` header is
sent with the first request, as required by endpoints that answer 401 without a
``WWW-Authenticate`` challenge), query parameters, caller-supplied headers,
transparent gzip decompression, and local paths, which are read from disk.

Public API
----------
fetch(url, ...)  -> Response(body: bytes, content_type: str)
"""

import gzip
import logging
import os
from base64 import b64encode
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from .constants import LOGGING_NAMESPACE

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

DEFAULT_TIMEOUT = 30

# Only gzip is advertised: it is what servers actually serve, and it is the one
# content encoding that the standard library can undo.
ACCEPTED_ENCODING = "gzip"


@dataclass(frozen=True)
class Response:
    """The body of a fetched resource and the content type it was served with."""
    body: bytes
    content_type: str = ""


def is_remote(url: str) -> bool:
    """True when *url* is an actual URL rather than a local file path."""
    scheme = urlsplit(url).scheme
    # A single-letter scheme is a Windows drive letter, not a URL scheme.
    return len(scheme) > 1


def basic_auth_header(username: str, password: str) -> dict[str, str]:
    """Build the preemptive HTTP Basic Authentication header."""
    if not username and not password:
        return {}
    credentials = b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    return {"Authorization": f"Basic {credentials}"}


def fetch(
    url: str,
    *,
    username: str = "",
    password: str = "",
    accept: str = "",
    params: dict[str, str] | None = None,
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
    content_type: str = "",
    method: str = "",
    timeout: int = DEFAULT_TIMEOUT,
    description: str = "resource",
) -> Response:
    """
    Retrieve *url* and return its body.

    Local paths are read from disk; everything else goes over the network with,
    when credentials are given, HTTP Basic Authentication. *params* are added to
    the query string, *headers* take precedence over the ones derived from the
    other arguments, and gzipped responses are decompressed.
    """
    if not is_remote(url):
        return _read_file(url, description)

    request_headers = basic_auth_header(username, password)
    request_headers["Accept-Encoding"] = ACCEPTED_ENCODING
    if accept:
        request_headers["Accept"] = accept
    if content_type:
        request_headers["Content-Type"] = content_type
    if headers:
        request_headers.update(headers)

    url = _with_params(url, params)
    request = Request(url, data=data, headers=request_headers, method=method or None)

    LOGGER.debug(f"Fetching {description} from '{url}'.")

    try:
        with urlopen(request, timeout=timeout) as response:      # noqa: S310
            content_encoding = response.headers.get("Content-Encoding", "")
            return Response(
                body=_decompress(response.read(), content_encoding, url, description),
                content_type=response.headers.get_content_type() or "",
            )
    except HTTPError as exc:
        detail = f"{exc.code} {exc.reason}"
        if exc.code in (401, 403) and not (username or password):
            detail += (
                ". Set 'username' and 'password' in the resource section to "
                "access it with HTTP Basic Authentication"
            )
        raise ValueError(f"Could not retrieve the {description} '{url}': {detail}.") from exc
    except URLError as exc:
        raise ValueError(
            f"Could not retrieve the {description} '{url}': {exc.reason}."
        ) from exc


def _with_params(url: str, params: dict[str, str] | None) -> str:
    """Add *params* to the query string of *url*, keeping the ones already there."""
    if not params:
        return url

    url_parts = urlsplit(url)
    query = urlencode(params)
    if url_parts.query:
        query = f"{url_parts.query}&{query}"

    return urlunsplit(url_parts._replace(query=query))


def _decompress(body: bytes, content_encoding: str, url: str, description: str) -> bytes:
    """Undo the content encoding of a response body."""
    if content_encoding.lower() != "gzip":
        return body

    try:
        return gzip.decompress(body)
    except (OSError, EOFError) as exc:
        raise ValueError(
            f"Could not decompress the {description} '{url}': {exc}."
        ) from exc


def _read_file(path: str, description: str) -> Response:
    """Read a resource stored as a local file."""
    if not os.path.isfile(path):
        raise ValueError(f"The {description} '{path}' is not a readable file.")

    with open(path, "rb") as resource_file:
        return Response(body=resource_file.read())
