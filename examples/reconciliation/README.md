# Reconciliation against a SKOS vocabulary or a SPARQL endpoint

Reconciliation maps a value of the input data to the concept it identifies in a
controlled vocabulary. Morph-KGC ships two **stateful** functions for it:

| Function | Reconciles against |
| --- | --- |
| `urn:morph:function:reconciliation:reconcileVocabularyConcept` | a SKOS vocabulary fetched from a URL |
| `urn:morph:function:reconciliation:reconcileSPARQLConcept` | the concepts held by a SPARQL endpoint |

Both are initialized **once**, before any triple is materialized: the vocabulary
is downloaded (or the endpoint queried) a single time and the resulting index is
shared by every mapping rule and every worker process. No repeated API call is
made while generating triples.

This example reconciles against a SKOS vocabulary. Run it with:

```bash
python run.py
```

[`../reconciliation-sparql`](../reconciliation-sparql) is the same example
reconciling against a SPARQL endpoint.

## The accessed resource lives in the configuration file

The mapping only names the resource it reconciles against. Where that resource
is, how it is authenticated and how values are matched are declared in the
configuration file, so the same mapping runs unchanged against a local copy of a
vocabulary, a staging server or production:

```ini
[RESOURCE:disease_vocabulary]
resource_type=SKOS_VOCABULARY
url=https://example.org/vocabulary/disease
username={VOCABULARY_USER}
password={VOCABULARY_PASSWORD}
matching=CASE-INSENSITIVE
```

```turtle
<#DiseaseReconciliationExecution>
    rml:function morph-fr:reconcileVocabularyConcept ;
    rml:input [
        rml:parameter morph-fn:resource ;
        rml:inputValue "disease_vocabulary"
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

## Resource options

Common to both resource types:

| Option | Meaning |
| --- | --- |
| `resource_type` | `SKOS_VOCABULARY` or `SPARQL_ENDPOINT` |
| `url` | where the vocabulary is downloaded from, or the endpoint queried. A local path is read from disk |
| `iri` | the IRI identifying the resource, when it differs from `url`. A mapping may name the resource by it |
| `username`, `password` | HTTP Basic Authentication credentials |
| `matching` | `EXACT` (default) or `CASE-INSENSITIVE`, which also collapses whitespace |
| `attributes` | comma-separated attributes to index when the mapping names none |
| `timeout` | seconds to wait for the resource (default `30`) |

Only for `SKOS_VOCABULARY`:

| Option | Meaning |
| --- | --- |
| `format` | RDF serialization of the vocabulary (`turtle`, `xml`, `nt`, `json-ld`, ...). Guessed from the response and the URL when omitted |

Only for `SPARQL_ENDPOINT`:

| Option | Meaning |
| --- | --- |
| `query` | the SELECT query the index is built from. Defaults to a query over the SKOS labelling properties |
| `method` | `GET` (default) or `POST` |
| `concept_variable`, `attribute_variable`, `value_variable` | projected variable names (default `concept`, `attribute`, `value`) |

## Function parameters

| Parameter | Meaning |
| --- | --- |
| `grel:valueParam` | the value to reconcile |
| `morph-fn:resource` | name of the `[RESOURCE:<name>]` section to reconcile against. May be omitted when a single resource of the right type is declared. `morph-fn:vocabularyIRI` and `morph-fn:endpointIRI` are accepted spellings, and also accept the IRI identifying the resource |
| `morph-fn:attributeIRI` | the vocabulary property (or properties) the value is matched against, e.g. `skos:prefLabel`. Bind it several times to match against several: they are matched as a union, since RDF puts no order on the values of a property. `grel:attributeIRI` is accepted as well |

A value matching no concept yields no triple. A value matching several concepts
yields one triple per matched concept.

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
                function: morph-fr:reconcileVocabularyConcept
                type: iri
                parameters:
                    - parameter: morph-fn:resource
                      value: disease_vocabulary
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
