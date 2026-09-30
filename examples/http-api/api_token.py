"""
The module the `api_token` configuration option points at.

Morph-KGC calls `get_api_token(token_name)` with the value a header of the
mapping is written with, and sends the header with whatever it returns. That is
what a mapping needs when the token is not a constant: it is asked for, it
expires, and it has to be refreshed.

The token is cached on disk, and not in memory, because Morph-KGC materializes
in several processes: a cache file is shared by all of them, so the token is
asked for once rather than once per worker. `refresh_token` is the only part to
replace with the secret manager, identity provider or login endpoint the token
really comes from; everything else stays as it is.
"""

import json
import os
import tempfile
from datetime import datetime

# Where the cached token is kept, next to this file.
CACHE = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'token_cache.json')

# The token names this module hands out. Morph-KGC sends a header whose value
# is not one of them as it is written, which is how the constant 'Accept'
# header of the mapping reaches the API untouched.
TOKEN_NAMES = ['OBSERVATIONS_API_TOKEN']


def refresh_token(token_name):
    """
    Ask for a new token and return it with the seconds it is valid for.

    Replace this with the real thing: the credentials of your secret manager
    are what `os.environ` is read for, and the token is what it answers with.

        credentials = os.environ['SECRET_MANAGER_CREDENTIALS']

    The API of this example expects a constant token, so that the example runs
    without a secret manager of its own.
    """
    return 'Bearer s3cr3t', 3600


def get_api_token(token_name):
    """The token named *token_name*, refreshed when the cached one expired."""
    if token_name not in TOKEN_NAMES:
        # Not a token name: Morph-KGC sends the header value as it is written.
        return None

    cache = _read_cache()
    expires_at = cache.get(f'{token_name}_EXPIRES_AT', 0)
    if token_name in cache and expires_at > datetime.now().timestamp():
        return cache[token_name]

    token, expires_in = refresh_token(token_name)

    cache[token_name] = token
    cache[f'{token_name}_EXPIRES_AT'] = datetime.now().timestamp() + expires_in
    _write_cache(cache)

    return token


def _read_cache():
    """The cached tokens, or no token at all when the cache cannot be read."""
    try:
        with open(CACHE, 'r', encoding='utf-8') as cache_file:
            return json.load(cache_file)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _write_cache(cache):
    """
    Write the cache in one go.

    The worker processes materializing the knowledge graph share this file, so
    it is written to a temporary file that replaces it once complete: renaming
    is atomic, and no worker ever reads a half-written cache.
    """
    directory = os.path.dirname(os.path.abspath(CACHE)) or '.'
    descriptor, temporary_path = tempfile.mkstemp(dir=directory, prefix='.token_')
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as temporary_file:
            json.dump(cache, temporary_file, ensure_ascii=False, indent=2)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        os.replace(temporary_path, CACHE)
    finally:
        if os.path.exists(temporary_path):
            os.remove(temporary_path)
