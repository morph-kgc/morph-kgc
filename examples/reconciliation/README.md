# Reconciliation against a SKOS vocabulary

Reconciliation maps a value of the input data to the entity it identifies in a
controlled vocabulary or a knowledge graph. Morph-KGC ships two **stateful**
functions for it:

| Function | Reconciles against |
| --- | --- |
| `urn:morph:function:reconciliation:reconcileVocabularyConcept` | a SKOS vocabulary fetched from a URL |
| `urn:morph:function:reconcileEntityOverSPARQL` | the entities of any knowledge graph behind a SPARQL endpoint, selected by a query of the mapping |

The vocabulary function is under `urn:morph:function:reconciliation:`
(`morph-fr:` below), while the SPARQL function and every parameter of both are
under `urn:morph:function:` (`morph-fn:`). [Ways of matching
values](#ways-of-matching-values), [Ambiguous values](#ambiguous-values) and the
[Reconciliation report](#reconciliation-report) apply to both functions.

Both are initialized **once**, before any triple is materialized: the vocabulary
is downloaded (or the query sent) a single time and the resulting index is
shared by every mapping rule and every worker process. No repeated API call is
made while generating triples.

The index is kept **on disk**, as a Parquet file of the state directory of the
run. It is built and read by DuckDB, which uses at most the `state_memory_limit`
of the configuration file (`512MB` by default) **in each process**, spilling to
the state directory beyond it, so that total memory grows with
`number_of_processes`, not with the size of the vocabulary. A vocabulary is read
as a stream, except in a few cases (such as JSON-LD whose top level is an
object), which are read whole into memory and say so in a warning.

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
generated, so the mapping says it. The execution below is a variant of the one
in [`mapping.ttl`](mapping.ttl), which matches values exactly:

```ini
[RESOURCE:disease_vocabulary]
resource_type=SKOS_VOCABULARY
url=https://example.org/vocabulary/disease
username={VOCABULARY_USER}
password={VOCABULARY_PASSWORD}
```

```turtle
@prefix rml: <http://w3id.org/rml/> .
@prefix grel: <http://users.ugent.be/~bjdmeest/function/grel.ttl#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix morph-fr: <urn:morph:function:reconciliation:> .
@prefix morph-fn: <urn:morph:function:> .

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

An object map using the function needs `rml:termType rml:IRI`: one whose value is the result of
a function generates literals by default.

`{ENV_VAR}` placeholders in `iri`, `url`, `username` and `password` are replaced
with environment variables, so credentials need not be written to the file at
all. A value written in the file doubles a literal `$` (`$$`), and, in these
four options, a literal `{` or `}` (`{{`, `}}`); a value read from an
environment variable needs no escaping. A misspelled option is ignored without
a warning.

## Resource options

| Option | Meaning |
| --- | --- |
| `resource_type` | `SKOS_VOCABULARY` (required). |
| `url` | where the vocabulary is downloaded from. A local path is read from disk, relative to the current working directory (not to the configuration file). Compressed files (`.gz`, `.zip`) are not read; a server's gzip `Content-Encoding` is decoded. |
| `iri` | the IRI identifying the vocabulary, when it differs from `url`. A mapping may name the resource by it. |
| `username`, `password` | HTTP Basic Authentication credentials. |
| `format` | RDF serialization of the vocabulary (`turtle`, `xml`, `nt`, `nquads`, `trig`, `json-ld`, ...). Guessed from the response and the URL when omitted. A vocabulary serialized as quads is indexed across all its named graphs. |
| `attributes` | comma-separated attributes the value is matched against when the execution names none, as IRIs or as prefixed names (`skos:prefLabel`) with one of the prefixes `skos`, `rdf`, `rdfs`, `owl`, `xsd`, `dc`, `dcterms`/`dct`, `schema` (`https://schema.org/`) or `foaf`; an attribute in another namespace is written as a full IRI. Without it, every property whose value is a literal. |
| `timeout` | seconds to wait for the vocabulary (default `30`). |
| `ambiguity` | the policy for the values matching several concepts of the vocabulary, when the execution sets none: `ALL` (default), `NONE` or `FAIL`. See [Ambiguous values](#ambiguous-values). |

The values matched to no concept, and to several, can be reported to a file;
see [Reconciliation report](#reconciliation-report).

## Function parameters

| Parameter | Meaning |
| --- | --- |
| `grel:valueParam` | the value to reconcile (`grel:valueParam1` is accepted as well). A concept is matched by a single value. |
| `morph-fn:resource` | the resource to reconcile against: the name of its `[RESOURCE:<name>]` section, or its `iri` or `url`. `morph-fn:vocabularyIRI` is an equivalent spelling. May be omitted when a single resource with `resource_type=SKOS_VOCABULARY` is declared. |
| `morph-fn:attributeIRI` | the vocabulary property (or properties) the value is matched against, e.g. `skos:prefLabel`. Bind it several times to match against several: they are matched as a union, since RDF puts no order on the values of a property. `grel:attributeIRI` is accepted as well, and both may be bound together. Bind it to IRIs (`rml:inputValue skos:prefLabel`), not strings: a string such as `"skos:prefLabel"` is not expanded and matches nothing. When omitted, the `attributes` option of the resource applies. |
| `morph-fn:matching` | how the value is matched: `EXACT` (default), `LOWERCASE` or `CASE-INSENSITIVE`. See [Ways of matching values](#ways-of-matching-values). A constant, since the vocabulary is indexed before any data is read. Executions matching the same vocabulary differently share a single download of it. |
| `morph-fn:ambiguity` | what a value matching several concepts is reconciled to: `ALL` (default), `NONE` or `FAIL`. See [Ambiguous values](#ambiguous-values). A constant, overriding the `ambiguity` option of the resource. |

A value matching no concept yields no triple. Every IRI of the vocabulary with
a value of a matched attribute is a candidate, whether or not it is typed
`skos:Concept`, except those typed `skos:ConceptScheme`, `skos:Collection` or
`skos:OrderedCollection`; blank nodes never are. A named attribute matches both
its literal and its IRI values (e.g. `skos:exactMatch`); without one, only
literal values are indexed. The concepts are IRIs, so an object map generating
them needs `rml:termType rml:IRI`.

The resource and the attributes may also be read from the data
(`rml:inputValueMap`). A resource read per row is the name, `iri` or `url` of a
declared resource, and every `SKOS_VOCABULARY` resource is then indexed.
Attributes read per row are full property IRIs, and only properties whose
values are literals can then be matched.

## Ways of matching values

`morph-fn:matching` says how the value and the labels of the vocabulary are
compared. In the examples below, the value `Café␠` ends with a space, which
`␠` stands for:

| Matching | Compares values | `Café␠` matches | `Straße` matches |
| --- | --- | --- | --- |
| `EXACT` | as they are. The default. | `Café␠` | `Straße` |
| `LOWERCASE` | lower-cased, and nothing else: no whitespace is trimmed or collapsed, and no Unicode normalization is applied. | `café␠`, `CAFÉ␠`; not `Café` (no space), `cafe`, `café␠␠` (two spaces) or `café␠` decomposed (NFD) | `straße`; not `strasse` |
| `CASE-INSENSITIVE` | normalized (NFKC), with their whitespace collapsed, and case-folded. | `café␠`, `Café` (no space), `café␠␠` (two spaces), `café␠` decomposed (NFD)...; not `cafe` | `strasse`, `STRASSE`, `straße` |

`LOWERCASE` lower-cases values with Python's `str.lower()`. `CASE-INSENSITIVE`
matches more, which may turn a value matching a single concept into an
ambiguous one.

Only the lexical form of a label is compared: language tags and datatypes are
ignored, so the labels of a concept in every language are matched, which may
also make a value ambiguous.

`urn:morph:function:reconcileEntityOverSPARQL` matches values in the same ways,
with the same results: the same data, mapping and way of matching values give
the same triples whether the vocabulary is read from a file or queried from a
SPARQL endpoint holding it (see
[`../reconciliation-sparql`](../reconciliation-sparql/README.md)).

## Ambiguous values

A value matching several concepts, such as a label that is the preferred label
of one concept and an alternative label of another, is ambiguous. What it is
reconciled to is a policy, which an execution chooses with `morph-fn:ambiguity`,
and the `ambiguity` option of the resource sets for the executions that do not:

| Policy | An ambiguous value yields |
| --- | --- |
| `ALL` | one triple per matched concept. The default. |
| `NONE` | no triple. |
| `FAIL` | no triple, as the run is aborted with an error. |

An aborted run may leave a partial output file, holding the triples of the
mapping groups already materialized: discard it when the run exits with an
error. Its report holds the values handled before the error.

```turtle
    rml:input [
        rml:parameter morph-fn:ambiguity ;
        rml:inputValue "NONE"
    ] .
```

```ini
[RESOURCE:disease_vocabulary]
resource_type=SKOS_VOCABULARY
url=https://example.org/vocabulary/disease
ambiguity=NONE
```

Ambiguous values are not logged: each is recorded, with the concepts it
matches, in the [reconciliation report](#reconciliation-report), whatever the
policy, so that the values left without a triple can be reviewed. With `FAIL`,
the first ambiguous value is also the message of the error aborting the run.

## Reconciliation report

The values matched to no concept, and to several, are written to a report, in
[JSON Lines](https://jsonlines.org) (one JSON object per line), when the
configuration file names it:

```ini
[CONFIGURATION]
reconciliation_report=reports/reconciliation.jsonl
```

A relative path is relative to the current working directory. It holds one line per distinct (execution, value, outcome): each value an
execution matched to no entity (`NO_MATCH`) or to several (`MULTIPLE_MATCH`),
whatever the number of rows holding it, of mapping rules reconciling it and of
processes (`number_of_processes`). It is written when the run ends, also when it
is aborted (ambiguity `FAIL`). It is not written when the run fails before any
triple is materialized (an invalid option, a vocabulary or endpoint that cannot
be read), and a report of an earlier run is then left in place. Its directory is
created as the configuration is read, and a path it cannot be written to is an
error then; an existing file, or a device such as `/dev/stdout`, needs only to
be writable itself. With `url=../../test/rml-fnml/reconciliation/disease_vocabulary_ambiguous.ttl`
and `ambiguity=NONE` in `[RESOURCE:disease_vocabulary]` of
[`config.ini`](config.ini), and `reconciliation_report=reports/reconciliation.jsonl`
in its `[CONFIGURATION]`, this example writes:

```json
{"outcome": "MULTIPLE_MATCH", "value": "Autosomal dominant cerebellar ataxia-deafness-narcolepsy syndrome", "normalized_value": "Autosomal dominant cerebellar ataxia-deafness-narcolepsy syndrome", "function_iri": "urn:morph:function:reconciliation:reconcileVocabularyConcept", "execution_id": "http://example.com/base/#DiseaseReconciliationExecution", "triples_map": "http://example.com/base/#DiseaseResourceMapping, http://example.com/base/#PatientsMapping", "resource": "disease_vocabulary", "candidates": ["https://data.boehringer.com/id/00036/10077339", "https://example.org/vocabulary/disease/adca-dn"], "source": "patients.csv", "mapping": "mapping.ttl"}
```

| Field | Meaning |
| --- | --- |
| `outcome` | `NO_MATCH` or `MULTIPLE_MATCH`. |
| `value`, `normalized_value` | the value as read (a string, whatever a nested function returned), and as looked up (see [Ways of matching values](#ways-of-matching-values)); arrays for an entity matched by several values. |
| `function_iri`, `execution_id` | the function and its execution in the mapping. An execution without an IRI (always the case in YARRRML) gets a generated id, which changes on every run. |
| `triples_map` | the triples map reconciling the value. |
| `resource` | the resource reconciled against; `endpoint` and `query_hash` (SHA-256 of the query) for a SPARQL endpoint. |
| `candidates` | for `MULTIPLE_MATCH`: the entities the value matches. |
| `source`, `mapping` | the logical source of the triples map (its file path, or its query or table), and the mapping file declaring it. |

An execution used by several triples maps, as the one of this example, records
a value once: `triples_map` names each of them, sorted and joined by `, `, and
so do `source` and `mapping` when they differ, and `resource` when the
execution reads it from the data. `candidates` gathers the entities the value
matches in every row (attributes read from the data may match it to different
ones).

Only the values of the rows that reach the term they are reconciled for are
reported: not those of a row missing a column the mapping rule references (see
`na_values`), nor those of a row an earlier term of the rule yields no value
for. When the subject of a row is reconciled to no entity, for instance, its
object is neither reconciled nor reported; reconcile them in separate triples
maps for the report to hold both.

The values themselves are never logged. A summary of each execution is logged
(`INFO`), counting the rows each mapping rule reconciled, without its
duplicates, matched, unmatched and ambiguous, and the distinct values reported:

```
Execution 'http://example.com/base/#DiseaseReconciliationExecution' of function 'urn:morph:function:reconciliation:reconcileVocabularyConcept' handled 6 value(s): 3 matched, 0 unmatched, 3 ambiguous (1 distinct).
```

A mapping rule is one predicate-object map (or `rml:class`) of a triples map:
here the 2 rows of `patients.csv` are counted once for each of the 3 rules
reconciling them, 6 values. The `INFO` line `Resource '...': indexed N value(s)
of M attribute(s)` says what was indexed: 0 attributes means that none of the
attributes named occurs in the vocabulary.

## YARRRML

The patient-to-disease reconciliation in YARRRML:

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
