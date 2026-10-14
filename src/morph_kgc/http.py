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

Redirects are followed, but the credentials of a request are only sent to the
origin (scheme, host and port) they were given for, or to its https version, and
a POST keeps its method and body unless redirected with 303 See Other.
Credentials written in a URL (``https://user:password@host``) are rejected.

Public API
----------
fetch(url, ...)               -> Response(body: bytes, content_type: str)
download(url, directory, ...) -> Download(path: str, content_type: str, temporary: bool)
"""

import gzip
import logging
import os
import shutil
import tempfile
import zlib
from base64 import b64encode
from contextlib import contextmanager
from dataclasses import dataclass
from http.client import InvalidURL
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .constants import LOGGING_NAMESPACE

LOGGER = logging.getLogger(LOGGING_NAMESPACE)

DEFAULT_TIMEOUT = 30

# Only gzip is advertised: it is what servers actually serve, and it is the one
# content encoding that the standard library can undo.
ACCEPTED_ENCODING = "gzip"

# The port of a URL that does not give one, to tell origins apart.
DEFAULT_PORTS = {"http": 80, "https": 443}

# Redirects that keep the method and body of a request (see _RedirectHandler).
METHOD_PRESERVING_REDIRECTS = (301, 302, 307, 308)

# Headers a request keeps when redirected to another origin (as urllib spells
# them). Any other may carry credentials meant for the origin they were given
# for: Authorization, the KeyId of an HTTP API, Proxy-Authorization, ...
CROSS_ORIGIN_HEADERS = {"Accept", "Accept-encoding", "Content-type", "User-agent"}

# Bytes read from the network at a time by download().
CHUNK_SIZE = 1 << 20


@dataclass(frozen=True)
class Response:
    """The body of a fetched resource and the content type it was served with."""
    body: bytes
    content_type: str = ""


@dataclass(frozen=True)
class Download:
    """
    A resource written to a local file, and the content type it was served
    with. A local path is not copied: *temporary* says whether the file was
    written by :func:`download`, for the caller to remove once read.
    """
    path: str
    content_type: str = ""
    temporary: bool = False

    def remove(self) -> None:
        """Remove the file, if :func:`download` wrote it."""
        if self.temporary and os.path.exists(self.path):
            os.remove(self.path)


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
    other arguments, and gzipped responses are decompressed. Redirects are
    followed as ``_RedirectHandler`` describes.
    """
    if not is_remote(url):
        return _read_file(url, description)

    with _open(
        url, username, password, accept, params, headers, data, content_type,
        method, timeout, description,
    ) as (response, url):
        content_encoding = response.headers.get("Content-Encoding", "")
        return Response(
            body=_decompress(response.read(), content_encoding, url, description),
            content_type=response.headers.get_content_type() or "",
        )


def download(
    url: str,
    directory: str,
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
) -> Download:
    """
    Retrieve *url* as :func:`fetch` does, but write its body to a new file of
    *directory* as it arrives, instead of holding it in memory: a resource of
    any size is read with the same memory. A local path is returned as it is.
    """
    if not is_remote(url):
        if not os.path.isfile(url):
            raise ValueError(f"The {description} '{url}' is not a readable file.")
        return Download(path=url)

    with _open(
        url, username, password, accept, params, headers, data, content_type,
        method, timeout, description,
    ) as (response, url):
        content_encoding = response.headers.get("Content-Encoding", "")
        descriptor, path = tempfile.mkstemp(prefix="download-", dir=directory)
        try:
            with os.fdopen(descriptor, "wb") as body:
                if content_encoding.lower() == "gzip":
                    _copy_decompressed(response, body, url, description)
                else:
                    shutil.copyfileobj(response, body, CHUNK_SIZE)
            # Read in parts, a body cut short by the server is not an error:
            # what it said it would send and did not is left over.
            if getattr(response, "length", None):
                raise ValueError(
                    f"Could not retrieve the {description} '{url}': the connection "
                    f"closed {response.length} byte(s) before the end of the body."
                )
        except BaseException:
            os.remove(path)
            raise
        return Download(
            path=path,
            content_type=response.headers.get_content_type() or "",
            temporary=True,
        )


@contextmanager
def _open(
    url, username, password, accept, params, headers, data, content_type,
    method, timeout, description,
):
    """
    Send the request *fetch* and *download* describe, and yield the response
    and the URL it was sent to, turning the errors of urllib into ValueError.
    """
    if urlsplit(url).username is not None:
        # urllib would take them for part of the host name, and send them in
        # clear text to the DNS resolver, a proxy and the logs.
        raise ValueError(
            f"The URL of the {description} '{_redact_password(url)}' holds "
            "credentials, which are not accepted in a URL. Declare them in the "
            "configuration file instead."
        )

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

    opener = build_opener(_RedirectHandler)
    try:
        with opener.open(request, timeout=timeout) as response:
            yield response, url
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
    except InvalidURL as exc:
        # E.g. a redirect to a URL whose port is not a number.
        raise ValueError(f"Could not retrieve the {description} '{url}': {exc}.") from exc


class _RedirectHandler(HTTPRedirectHandler):
    """
    Follows redirects as urllib does, with two differences:

    - When a redirect leaves the origin (scheme, host and port) of the
      request, every header that may carry credentials is dropped, whether
      derived from the username and password or given by the caller, so that
      credentials never reach another server, nor travel in clear text after a
      redirect from https to http. Moving from http to https on the same host
      and default ports is the exception: it sends them nowhere they have not
      already been.
    - A POST keeps its method and body on 307 and 308, as HTTP requires, and on
      301 and 302 too (as curl's ``--post301`` and ``--post302``), so that a
      SPARQL query sent in the body survives an endpoint that redirects http to
      https or adds a trailing slash. Only 303 See Other turns it into a GET.
    """

    # Not followed by urllib before Python 3.11.
    http_error_308 = HTTPRedirectHandler.http_error_302

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        method = req.get_method()
        if code in METHOD_PRESERVING_REDIRECTS and method in ("GET", "HEAD", "POST"):
            redirected = Request(
                newurl.replace(" ", "%20"),
                data            = req.data,
                headers         = req.headers,
                method          = method,
                origin_req_host = req.origin_req_host,
                unverifiable    = True,
            )
        else:
            redirected = super().redirect_request(req, fp, code, msg, headers, newurl)

        # Compared with the previous request rather than the first one: once
        # dropped, a header is not sent again, even back at its origin.
        if not (
            _same_origin(redirected.full_url, req.full_url)
            or _is_https_upgrade(req.full_url, redirected.full_url)
        ):
            for name in list(redirected.headers):
                if name not in CROSS_ORIGIN_HEADERS:
                    redirected.remove_header(name)

        return redirected


def _same_origin(url: str, other_url: str) -> bool:
    """True when *url* and *other_url* have the same scheme, host and port."""
    try:
        return _origin(url) == _origin(other_url)
    except ValueError:
        # A port that is no port (out of range or not a number) is no origin's:
        # the request to it fails afterwards, as for any unreachable URL.
        return False


def _is_https_upgrade(url: str, redirect_url: str) -> bool:
    """True when *redirect_url* is the https version of the http *url*."""
    try:
        url_parts, redirect_parts = urlsplit(url), urlsplit(redirect_url)
        return (
            url_parts.scheme.lower() == "http"
            and redirect_parts.scheme.lower() == "https"
            and url_parts.hostname == redirect_parts.hostname
            and url_parts.port in (None, 80)
            and redirect_parts.port in (None, 443)
        )
    except ValueError:
        return False


def _origin(url: str) -> tuple[str, str | None, int | None]:
    """The scheme, host and port of *url*."""
    url_parts = urlsplit(url)
    scheme = url_parts.scheme.lower()
    return scheme, url_parts.hostname, url_parts.port or DEFAULT_PORTS.get(scheme)


def _redact_password(url: str) -> str:
    """Mask the password in the userinfo of *url*, if it has one."""
    url_parts = urlsplit(url)
    if not url_parts.password:
        return url

    userinfo, _, host = url_parts.netloc.rpartition("@")
    username = userinfo.split(":", 1)[0]
    return urlunsplit(url_parts._replace(netloc=f"{username}:***@{host}"))


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


def _copy_decompressed(response, body, url: str, description: str) -> None:
    """Copy the gzipped *response* to the file *body*, decompressing it."""
    # Every member of the body is read (as gzip.decompress does), and at most
    # CHUNK_SIZE bytes of it are decompressed at a time.
    try:
        with gzip.GzipFile(fileobj=response, mode="rb") as decompressed:
            shutil.copyfileobj(decompressed, body, CHUNK_SIZE)
    except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
        raise ValueError(
            f"Could not decompress the {description} '{url}': {exc}."
        ) from exc


def _read_file(path: str, description: str) -> Response:
    """Read a resource stored as a local file."""
    if not os.path.isfile(path):
        raise ValueError(f"The {description} '{path}' is not a readable file.")

    with open(path, "rb") as resource_file:
        return Response(body=resource_file.read())
