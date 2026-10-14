__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
Vectorized UDFs, called once with the values of every row: a stateful one, and
one returning something other than a result per row.
"""

import json


def load_country_names(initialization):
    resource = initialization.resource(
        initialization.constants('urn:morph:function:resource')[0]
    )

    with open(resource.get_url()) as names_file:
        return json.load(names_file)


# The rows each call received, to tell a vectorized call from per-row calls.
CALLS = []


@stateful_udf(
    fun_id='http://example.com/countryName',
    initializer=load_country_names,
    vectorized=True,
    code='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    resource='urn:morph:function:resource')
def country_name(code, context, resource=None):
    CALLS.append((list(code), list(resource)))
    return [context.get(c) for c in code]


@udf(
    fun_id='http://example.com/wrongCountryName',
    vectorized=True,
    code='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def wrong_country_name(code):
    return {c: c for c in code}
