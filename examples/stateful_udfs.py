"""
Stateful user-defined functions.

A stateful UDF declares an *initializer*: a callable run exactly once, before
any triple is materialized, whose return value becomes a shared, immutable
context reused by every invocation of the function. It is the way to avoid
paying for an external lookup (an HTTP call, a database query, reading a large
file) once per row.

The context is persisted to disk by the engine, so the worker processes read it
back instead of rebuilding it. Whatever an initializer returns must therefore be
picklable. Each worker process loads it whole, the first time it executes the
function: a context too large for that keeps its data in an on-disk table
instead (see the second function below), which the function queries.

Declare the file holding these functions in the configuration:

    [CONFIGURATION]
    udfs=/path/to/stateful_udfs.py

and the resources they access, so that URLs and credentials stay out of the
mapping:

    [RESOURCE:country_codes]
    resource_type=CSV_FILE
    url=https://example.org/country-codes.csv
    username={COUNTRY_CODES_USER}
    password={COUNTRY_CODES_PASSWORD}
"""

import csv
import io

import pandas as pd

from morph_kgc.http import download, fetch


# ── The initializer ───────────────────────────────────────────────────────────

def load_country_codes(initialization):
    """
    Build the shared context.

    An initializer takes either no argument, or the InitializationContext the
    engine builds for it, which exposes:

      initialization.function_iri   the function being initialized
      initialization.config         the configuration of the run
      initialization.constants(...) the constant values the mapping binds to
                                    the given parameter IRIs
      initialization.resource(...)  a [RESOURCE:<name>] section, by name or by
                                    the IRI identifying it
      initialization.resources(...) every resource the mapping references
                                    through the given parameter IRIs
      initialization.executions     the constant values of each execution of
                                    the function, by parameter IRI, to pair
                                    values that go together
    """
    # Only the resources the mapping actually names are accessed.
    resources = initialization.resources('urn:morph:function:resource')

    codes = {}
    for resource in resources.values():
        # fetch() reads local paths and URLs alike, with HTTP Basic
        # Authentication when the resource declares credentials.
        response = fetch(
            resource.get_url(),
            username=resource.get_username(),
            password=resource.get_password(),
        )

        rows = csv.DictReader(io.StringIO(response.body.decode('utf-8')))
        for row in rows:
            codes[row['code']] = row['name']

    return codes


# ── The transformation function ───────────────────────────────────────────────

@stateful_udf(                                                    # noqa: F821
    fun_id='http://example.com/function/countryName',
    initializer=load_country_codes,
    # As for a stateless @udf, every keyword argument is bound to the parameter
    # IRI the mapping passes it through.
    code='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    resource='urn:morph:function:resource')
def country_name(code, context, resource=None):
    """
    The shared context arrives as the 'context' keyword argument. It is the very
    object the initializer returned, built once for the whole materialization.

    Returning None generates no triple for the row.
    """
    return context.get(code)


# ── A shared context kept on disk ─────────────────────────────────────────────

def write_country_codes(initialization):
    """
    Build a shared context of any size with the same memory.

    The rows go to a Parquet file of the state directory of the run as they are
    read, and the context is a handle to it (a ParquetTable), which costs
    nothing to load. The file is removed when the run ends.
    """
    resources = initialization.resources('urn:morph:function:resource')

    def rows():
        for resource in resources.values():
            # download() writes the resource to a file of the state directory
            # instead of holding it in memory (a local path is used as it is).
            codes = download(
                resource.get_url(),
                initialization.state_dir,
                username=resource.get_username(),
                password=resource.get_password(),
            )
            try:
                with open(codes.path, newline='', encoding='utf-8') as codes_file:
                    for row in csv.DictReader(codes_file):
                        yield row['code'], row['name']
            finally:
                codes.remove()

    # Sorted by the column the function looks values up by, so that DuckDB
    # skips the parts of the file that cannot match.
    return initialization.write_table(
        rows(), {'code': 'VARCHAR', 'name': 'VARCHAR'}, order_by=('code',),
    )


@stateful_udf(                                                    # noqa: F821
    fun_id='http://example.com/function/countryNameOnDisk',
    initializer=write_country_codes,
    # Called once for all the rows of a mapping rule: every argument the mapping
    # binds is a list with a value per row, and the function returns a list
    # with a result per row. One query looks all of them up.
    vectorized=True,
    code='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam',
    resource='urn:morph:function:resource')
def country_name_on_disk(code, context, resource=None):
    """
    The table is queried through DuckDB, written {table} in the query; the
    keyword arguments of select() are DataFrames the query may read. Each
    worker process reads the parts of the file it needs, under the
    'state_memory_limit' of the configuration.
    """
    names = dict(context.select(
        'SELECT code, name FROM {table} WHERE code IN (SELECT code FROM codes)',
        codes=pd.DataFrame({'code': code}, dtype=object),
    ))
    return [names.get(value) for value in code]


# ── A stateless UDF, for comparison ───────────────────────────────────────────

@udf(                                                             # noqa: F821
    fun_id='http://example.com/function/toUpperCase',
    text='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def to_upper_case(text):
    return text.upper()
