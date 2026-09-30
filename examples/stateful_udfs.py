"""
Stateful user-defined functions.

A stateful UDF declares an *initializer*: a callable run exactly once, before
any triple is materialized, whose return value becomes a shared, immutable
context reused by every invocation of the function. It is the way to avoid
paying for an external lookup (an HTTP call, a database query, reading a large
file) once per row.

The context is persisted to disk by the engine, so the worker processes read it
back instead of rebuilding it. Whatever an initializer returns must therefore be
picklable.

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

from morph_kgc.http import fetch


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


# ── A stateless UDF, for comparison ───────────────────────────────────────────

@udf(                                                             # noqa: F821
    fun_id='http://example.com/function/toUpperCase',
    text='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def to_upper_case(text):
    return text.upper()
