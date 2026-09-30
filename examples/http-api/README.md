# HTTP API sources

A logical source can be an HTTP API: the request is described in the mapping
with the [HTTP Vocabulary in RDF](https://www.w3.org/TR/HTTP-in-RDF10/)
(`htv:`), and the JSON the API answers with is read the way any other JSON
source is, with a reference formulation and an iterator.

Run this example with:

```bash
python run.py
```

[`api.py`](api.py) is a small API answering the observations of
[`mapping.ttl`](mapping.ttl), so that the example needs no API of its own;
`run.py` starts it. Its request log shows the headers and query parameters
Morph-KGC sent. To run the example from the command line instead, start the API
yourself:

```bash
python api.py &
morph-kgc config.ini
```

## The request in the mapping

`rml:source` points at the request rather than at a file, and the request says
where the API is and what it is sent:

```turtle
<#ObservationsAPI>
    a htv:Request ;
    htv:absoluteURI "http://127.0.0.1:8891/observations" ;
    htv:headers (
        [ htv:fieldName "Authorization" ; htv:fieldValue "OBSERVATIONS_API_TOKEN" ]
        [ htv:fieldName "Accept" ; htv:fieldValue "application/json" ]
        [ htv:fieldName "pollutant" ; htv:fieldValue "all" ]
    ) .

<#ObservationsMapping>
    a rml:TriplesMap ;
    rml:logicalSource [
        rml:source <#ObservationsAPI> ;
        rml:referenceFormulation rml:JSONPath ;
        rml:iterator "$.observations[*]"
    ] ;
    ...
```

`Authorization`, `Accept`, `KeyId` and `User-Agent` are sent as request headers.
Every other field is sent as a query parameter, which is how `pollutant=all`
ends up in the query string.

Several triples maps can name the same request, as
`<#ObservationsMapping>` and `<#StationsMapping>` do here.

## Where the token comes from

`htv:fieldValue` is resolved before the request is sent, so that a token needs
not be written in the mapping:

| The value is | Sent as |
| --- | --- |
| a `{ENV_VAR}` template, e.g. `Bearer {OBSERVATIONS_TOKEN}` | the template, with the environment variables replaced |
| the name of an environment variable, e.g. `OBSERVATIONS_TOKEN` | the value of that variable |
| a name the `api_token` module hands out a token for | that token |
| anything else | as it is written, which is what `Accept` needs |

A `{ENV_VAR}` template that names a variable which is not set is an error, and
no request is sent.

The `api_token` option of the configuration file points at a Python module with
a `get_api_token(token_name)` function, which is called with the value written
in the mapping:

```ini
[CONFIGURATION]
api_token=api_token.py
```

[`api_token.py`](api_token.py) is such a module. It is what a token that is
asked for, expires and has to be refreshed needs, and the only part of it to
replace is `refresh_token`: where the token really comes from, be it a secret
manager, an identity provider or a login endpoint. It hands out no token for a
value that names none, which is how the constant `Accept` header of the mapping
reaches the API untouched.

The token is cached on disk rather than in memory because Morph-KGC
materializes in several processes: the cache file is shared by all of them, so
the token is asked for once instead of once per worker. Running the example
leaves that cache in `token_cache.json`.

## What this example generates

The four observations the API answers with are materialized with the
[SOSA](https://www.w3.org/TR/vocab-ssn/) vocabulary, each with the pollutant it
observes, its result, the time of that result and the station that made it. The
stations are materialized from the same request by a second triples map, over
the `station` object nested in every observation: `station.id` and
`station.name` reference it the way any nested JSON value is referenced.

## The API is requested once per mapping group

Morph-KGC partitions the mapping rules into groups it materializes
independently, and the data of a rule is loaded when its group is
materialized. An HTTP API is no exception: it is requested once per group
rather than once per run, as the request log of this example shows. Keep it in
mind for an API that is rate limited or slow to answer.
