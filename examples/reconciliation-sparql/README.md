# Reconciliation against a SPARQL endpoint

Reconciliation maps a value of the input data to the concept it identifies in a
controlled vocabulary. `urn:morph:function:reconciliation:reconcileSPARQLConcept`
reconciles against the concepts held by a SPARQL endpoint, while
[`../reconciliation`](../reconciliation) reconciles against a SKOS vocabulary
fetched from a URL.

It is a **stateful** function: the endpoint is queried **once**, before any
triple is materialized, and the resulting index of concepts is shared by every
mapping rule and every worker process. No query is sent while generating
triples.

Run this example with:

```bash
python run.py
```

[`endpoint.py`](endpoint.py) is a small SPARQL endpoint answering queries over
[`diseases.ttl`](diseases.ttl), so that the example needs no triplestore of its
own; `run.py` starts it. Its request log shows the single query sent for the
whole materialization. To run the example from the command line instead, start
the endpoint yourself:

```bash
python endpoint.py &
morph-kgc config.ini
```

## The accessed endpoint lives in the configuration file

The mapping only names the resource it reconciles against. Where that endpoint
is, how it is authenticated and which of its concepts are indexed are declared
in the configuration file, so the same mapping runs unchanged against a local
triplestore, a staging endpoint or production:

```ini
[RESOURCE:disease_endpoint]
resource_type=SPARQL_ENDPOINT
url=https://example.org/sparql
username={ENDPOINT_USER}
password={ENDPOINT_PASSWORD}
method=POST
matching=CASE-INSENSITIVE
```

```turtle
<#DiseaseReconciliationExecution>
    rml:function morph-fr:reconcileSPARQLConcept ;
    rml:input [
        rml:parameter morph-fn:resource ;
        rml:inputValue "disease_endpoint"
    ] ;
    rml:input [
        rml:parameter grel:valueParam ;
        rml:inputValueMap [ rml:reference "disease_label" ]
    ] ;
    rml:input [
        rml:parameter morph-fn:attributeIRI ;
        rml:inputValue skos:prefLabel , skos:altLabel
    ] .
```

`{ENV_VAR}` placeholders in `url`, `username` and `password` are replaced with
environment variables, so credentials need not be written to the file at all.

## The query the index is built from

The `query` option is the SELECT query the concepts are read from. It projects
the concept, the value it is reconciled by and the attribute that value comes
from:

```sparql
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
SELECT ?concept ?attribute ?value WHERE {
    ?concept skos:inScheme <https://example.org/vocabulary/disease> ;
             ?attribute ?value .
    VALUES ?attribute { skos:prefLabel skos:altLabel }
    FILTER (lang(?value) = 'en')
}
```

The query is what an endpoint offers over a vocabulary file: it decides which
part of the endpoint the index is built from. The endpoint of this example also
holds a drug vocabulary, and labels in languages other than English, none of
which the patients are reconciled against.

Without the option, everything the endpoint describes with the SKOS labelling
properties (`skos:prefLabel`, `skos:altLabel`, `skos:hiddenLabel` and
`skos:notation`) is indexed. The mapping matches against `skos:prefLabel` and
`skos:altLabel` here, so those are the two properties that are queried for.

A query projecting no `?attribute` builds an index that does not tell attributes
apart: values are then matched whatever property they come from, and the mapping
needs not name any attribute either.

## Resource options

| Option | Meaning |
| --- | --- |
| `resource_type` | `SPARQL_ENDPOINT` |
| `url` | where the endpoint is queried |
| `iri` | the IRI identifying the endpoint, when it differs from `url`. A mapping may name the resource by it |
| `username`, `password` | HTTP Basic Authentication credentials |
| `query` | the SELECT query the index is built from. Defaults to a query over the SKOS labelling properties |
| `method` | `GET` (default) or `POST` |
| `concept_variable`, `attribute_variable`, `value_variable` | projected variable names (default `concept`, `attribute`, `value`) |
| `matching` | `EXACT` (default) or `CASE-INSENSITIVE`, which also collapses whitespace |
| `attributes` | comma-separated attributes to index when the mapping names none |
| `timeout` | seconds to wait for the endpoint (default `30`) |

## Function parameters

| Parameter | Meaning |
| --- | --- |
| `grel:valueParam` | the value to reconcile |
| `morph-fn:resource` | name of the `[RESOURCE:<name>]` section to reconcile against. May be omitted when a single SPARQL endpoint is declared. `morph-fn:endpointIRI` is an accepted spelling, and also accepts the IRI identifying the endpoint |
| `morph-fn:attributeIRI` | the property (or properties) the value is matched against, e.g. `skos:prefLabel`. Bind it several times to match against several: they are matched as a union, since RDF puts no order on the values of a property. `grel:attributeIRI` is accepted as well |

A value matching no concept yields no triple. A value matching several concepts
yields one triple per matched concept.

## What this example generates

The four patients of [`patients.csv`](patients.csv) show what reconciling does
with a value:

| Patient | Disease label | Reconciled against |
| --- | --- | --- |
| `pid_00001` | `early-onset spastic ataxia-myoclonic epilepsy-neuropathy syndrome` | a `skos:altLabel`, in spite of the case, since the resource matches `CASE-INSENSITIVE` |
| `pid_00002` | `Autosomal dominant cerebellar ataxia-deafness-narcolepsy syndrome` | the `skos:prefLabel` of a concept |
| `pid_00003` | `ADCA-DN` | a `skos:altLabel` of that same concept |
| `pid_00004` | `Common cold` | no concept, so no disease triple is generated for the patient |

## YARRRML

The same reconciliation in YARRRML:

```yaml
prefixes:
    grel: http://users.ugent.be/~bjdmeest/function/grel.ttl#
    morph-fr: "urn:morph:function:reconciliation:"
    morph-fn: "urn:morph:function:"
    skos: http://www.w3.org/2004/02/skos/core#
mappings:
    patients:
        sources:
            - access: patients.csv
              referenceFormulation: csv
        subjects: https://example.org/kg/patient/$(pid)
        predicateobjects:
            - p: sio:SIO_000255
              o:
                function: morph-fr:reconcileSPARQLConcept
                type: iri
                parameters:
                    - parameter: morph-fn:resource
                      value: disease_endpoint
                    - parameter: grel:valueParam
                      value: $(disease_label)
                    - parameter: morph-fn:attributeIRI
                      value: skos:prefLabel
                    - parameter: morph-fn:attributeIRI
                      value: skos:altLabel
```

## Writing your own stateful function

Any user-defined function can be given a shared context in the same way; see
[`../stateful_udfs.py`](../stateful_udfs.py).
