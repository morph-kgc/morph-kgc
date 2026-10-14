__author__ = "Julián Arenas-Guerrero"
__license__ = "Apache-2.0"

"""
The stateful UDF of udf.py, keeping the country names on disk: the initializer
writes them to a Parquet table of the run, and the vectorized function looks the
codes of all the rows of a mapping rule up in a single query.
"""

import json

import pandas as pd


def load_country_names(initialization):
    resource = initialization.resource(
        initialization.constants('urn:morph:function:resource')[0]
    )

    with open(resource.get_url()) as names_file:
        names = json.load(names_file)

    return initialization.write_table(
        names.items(), {'code': 'VARCHAR', 'name': 'VARCHAR'}, order_by=('code',),
    )


@stateful_udf(
    fun_id='http://example.com/countryName',
    initializer=load_country_names,
    vectorized=True,
    code='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    resource='urn:morph:function:resource')
def country_name(code, context, resource=None):
    names = dict(context.select(
        'SELECT code, name FROM {table} WHERE code IN (SELECT code FROM codes)',
        codes=pd.DataFrame({'code': [str(c) for c in code]}, dtype=object),
    ))
    return [names.get(str(c)) for c in code]
