__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
A stateful UDF: the country names are read from disk once, before any triple is
materialized, and the resulting dict is shared by every function invocation.
"""

import json


def load_country_names(initialization):
    # The accessed resource is declared in the configuration file, so the
    # mapping only carries the name of the resource to use.
    resource = initialization.resource(
        initialization.constants('urn:morph:function:resource')[0]
    )

    with open(resource.get_url()) as names_file:
        return json.load(names_file)


@stateful_udf(
    fun_id='http://example.com/countryName',
    initializer=load_country_names,
    code='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    resource='urn:morph:function:resource')
def country_name(code, context, resource=None):
    return context.get(code)
