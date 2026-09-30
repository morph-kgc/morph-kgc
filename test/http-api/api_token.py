__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
The module the `api_token` configuration option points at in the tests: it
hands out the token a header value names. A real one would ask a secret manager
for it, and cache it until it expires.
"""

TOKENS = {'PEOPLE_API_TOKEN': 'Bearer s3cr3t'}


def get_api_token(token_name):
    # A value naming no token is not one: the header is sent as it is written.
    return TOKENS.get(token_name)
