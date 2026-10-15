# Reconciliation against a SKOS vocabulary

Reconciliation maps a value of the input data to the entity it identifies in a
controlled vocabulary or a knowledge graph. Morph-KGC ships two **stateful**
functions for it:

| Function | Reconciles against |
| --- | --- |
| `urn:morph:function:reconciliation:reconcileVocabularyConcept` | a SKOS vocabulary fetched from a URL |
| `urn:morph:function:reconcileEntityOverSPARQL` | the entities of any knowledge graph behind a SPARQL endpoint, selected by a query of the mapping |

Both are initialized **once**, before any triple is materialized: the vocabulary
is downloaded (or the query sent) a single time and the resulting index is
shared by every mapping rule and every worker process. No repeated API call is
made while generating triples.

The index is kept **on disk**, as a Parquet file of the state directory of the
run. Memory used does not grow with the size of
the vocabulary: the system uses at most the `state_memory_limit` of the
configuration file (`512MB` by default) and spills to the state directory.

This example reconciles against a SKOS vocabulary. Run it with:

```bash
python run.py
```

[`../reconciliation-sparql`](../reconciliation-sparql) reconciles against a
knowledge graph through a SPARQL endpoint.

## The accessed resource lives in the configuration file

The mapping only names the resource it reconciles against. Where that resource
is and how it is authenticated are declared in the configuration file, so the
same mapping runs unchanged against a local copy of a vocabulary, a staging
server or production. How values are matched against it changes the triples
generated, so the mapping says it:

```ini
[RESOURCE:disease_vocabulary]
resource_type=SKOS_VOCABULARY
url=https://example.org/vocabulary/disease
username={VOCABULARY_USER}
password={VOCABULARY_PASSWORD}
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
    ] ;
    rml:input [
        rml:parameter morph-fn:matching ;
        rml:inputValue "CASE-INSENSITIVE"
    ] .
```

An object map using the function needs `rml:termType rml:IRI`
(`rr:termType rr:IRI`, `type: iri` in YARRRML): one whose value is the result of
a function generates literals by default (RML-FNML, section 5.1), while a
subject map generates IRIs.

`{ENV_VAR}` placeholders in `iri`, `url`, `username` and `password` are replaced
with environment variables, so credentials need not be written to the file at
all.

## Resource options

| Option | Meaning |
| --- | --- |
| `resource_type` | `SKOS_VOCABULARY` |
| `url` | where the vocabulary is downloaded from. A local path is read from disk |
| `iri` | the IRI identifying the vocabulary, when it differs from `url`. A mapping may name the resource by it |
| `username`, `password` | HTTP Basic Authentication credentials, which are not accepted in `url` |
| `format` | RDF serialization of the vocabulary (`turtle`, `xml`, `nt`, `nquads`, `trig`, `json-ld`, ...). Guessed from the response and the URL when omitted. A vocabulary serialized as quads is indexed across all its named graphs |
| `attributes` | comma-separated attributes the value is matched against when the execution names none, as IRIs or as prefixed names (`skos:prefLabel`) with one of the prefixes `skos`, `rdf`, `rdfs`, `owl`, `xsd`, `dc`, `dcterms`/`dct`, `schema` (`https://schema.org/`) or `foaf`; an attribute in another namespace is written as a full IRI. Without it, every property whose value is a literal |
| `timeout` | seconds to wait for the vocabulary (default `30`) |

## Function parameters

| Parameter | Meaning |
| --- | --- |
| `grel:valueParam` | the value to reconcile |
| `morph-fn:resource` | name of the `[RESOURCE:<name>]` section to reconcile against. May be omitted when a single vocabulary is declared. `morph-fn:vocabularyIRI` is an accepted spelling, and also accepts the IRI identifying the vocabulary |
| `morph-fn:attributeIRI` | the vocabulary property (or properties) the value is matched against, e.g. `skos:prefLabel`. Bind it several times to match against several: they are matched as a union, since RDF puts no order on the values of a property. `grel:attributeIRI` is accepted as well, and both may be bound together. When omitted, the `attributes` option of the resource applies |
| `morph-fn:matching` | how the value is matched: `EXACT` (default) or `CASE-INSENSITIVE`, which also normalizes Unicode (NFKC) and collapses whitespace. A constant, since the vocabulary is indexed before any data is read. Executions matching the same vocabulary differently share a single download of it |

A value matching no concept yields no triple. A value matching several concepts
yields one triple per matched concept. Resources typed as concept schemes or
collections are never matched. The concepts are IRIs, so an object map
generating them needs `rml:termType rml:IRI`.

## YARRRML

The same reconciliation in YARRRML:

```yaml
prefixes:
    grel: http://users.ugent.be/~bjdmeest/function/grel.ttl#
    morph-fr: "urn:morph:function:reconciliation:"
    morph-fn: "urn:morph:function:"
    skos: http://www.w3.org/2004/02/skos/core#
    sio: http://semanticscience.org/resource/
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
                    - parameter: morph-fn:matching
                      value: CASE-INSENSITIVE
```

## Writing your own stateful function

Any user-defined function can be given a shared context in the same way, kept
in memory or, when large, in an on-disk table; see
[`../stateful_udfs.py`](../stateful_udfs.py).
