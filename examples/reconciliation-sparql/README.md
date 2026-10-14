# Reconciliation against a knowledge graph through SPARQL

Reconciliation maps a value of the input data to the entity it identifies in an
existing knowledge graph, so that the generated graph reuses its IRIs instead of
minting new ones. `urn:morph:function:reconcileEntityOverSPARQL`, requested in
[discussion #339](https://github.com/morph-kgc/morph-kgc/discussions/339),
reconciles against the entities of any knowledge graph behind a SPARQL
endpoint, selected by a SPARQL query written in the mapping.
[`../reconciliation`](../reconciliation) reconciles against a SKOS vocabulary
fetched from a URL instead.

It is a **stateful** function: the query is sent **once**, before any triple is
materialized, and the resulting index of entities is shared by every mapping
rule and every worker process. No query is sent while generating triples. As
for a SKOS vocabulary, the answer is streamed into an index kept on disk, so
the memory an answer of millions of entities takes is set by the
`state_memory_limit` of the configuration file, not by the answer (see
[`../reconciliation`](../reconciliation)). An answer in JSON is streamed when it
gives its head first, as most stores write it; one giving its results first, or
holding a term the streaming parser rejects, is read whole into memory, with a
warning.

Run this example with:

```bash
python run.py
```

[`endpoint.py`](endpoint.py) is a small SPARQL endpoint answering queries over
[`clinical_trials.trig`](clinical_trials.trig), so that the example needs no
triplestore of its own; `run.py` starts it. Its request log shows the single
query sent for the whole materialization. To run the example from the command
line instead, start the endpoint yourself:

```bash
python endpoint.py &
morph-kgc config.ini
```

## The endpoint and the query are in the mapping

The mapping says which endpoint to query and which entities to reconcile
against:

```turtle
<#ClinicalTrialReconciliation>
    rml:function morph-fn:reconcileEntityOverSPARQL ;
    rml:input [
        rml:parameter morph-fn:sparqlEndpointUrl ;
        rml:inputValue "http://127.0.0.1:8890/sparql"
    ] ;
    rml:input [
        rml:parameter morph-fn:sparqlQuery ;
        rml:inputValue """
            PREFIX schema: <https://schema.org/>
            SELECT ?entity_iri ?matching_value_1 WHERE {
                GRAPH ?g {
                    ?entity_iri a schema:MedicalStudy ;
                                schema:identifier ?matching_value_1 .
                }
            }
        """
    ] ;
    rml:input [
        rml:parameter grel:valueParam ;
        rml:inputValueMap [ rml:reference "clinical_trial_id" ]
    ] .
```

The query is any SELECT query over the knowledge graph. It projects two
variables:

| Variable | Meaning |
| --- | --- |
| `?entity_iri` | the entity a value is reconciled with |
| `?matching_value_1` | the value the entity is matched by |

A value is matched against `?matching_value_1` only. To match it against
several properties, bind all of them to that variable, e.g. with a `UNION` or a
property path (`schema:identifier|schema:alternateName`). Other projected
variables, such as `?g`, are ignored, but `?matching_value_2` and the like are
rejected, since no value is matched against them. A query that does not project
both `?entity_iri` and `?matching_value_1` fails instead of reconciling nothing.

Values are matched exactly. To match them whatever their case, normalize both
sides in the mapping: `BIND(LCASE(STR(?identifier)) AS ?matching_value_1)` in
the query, and a nested `grel:toLowerCase` execution as `grel:valueParam`.

Declare the prefixes the query uses: an endpoint is not bound to know `schema:`
or any other prefix.

Since each reconciliation brings its own query, a mapping reconciles as many
kinds of entities, against as many endpoints, as it needs: one execution per
query, and the same query sent to the same endpoint by several executions is
sent once. When the reconciled entity is the object of a triple, give its object
map `rml:termType rml:IRI` (`type: iri` in YARRRML), since an object map whose
value is the result of a function generates literals by default.

### Named graphs

The query is sent as written. Stores differ on whether their default graph
includes the named graphs: GraphDB and Virtuoso include them, Fuseki, Oxigraph
and Stardog do not by default. On the latter, a knowledge graph that keeps its
entities in named graphs, as the one of this example does, is read with a
`GRAPH ?g { ... }` pattern. Without it, the query of this example finds no
entity: no triple is generated, and a warning says so. A query meant for any
store reads both the default graph and the named graphs:
`{ P } UNION { GRAPH ?g { P } }`.

### Writing the query in a mapping

In Turtle, write the query as a `"""..."""` literal and double every backslash
of the query: `regex(?id, "^NCT\\d+$")` is written `regex(?id, "^NCT\\\\d+$")`.
In YARRRML, write it as a `|` block scalar, which keeps it exactly as written. Some stores cap the
number of results they return (e.g. `ResultSetMaxRows` in Virtuoso); values of
entities left out of the answer are not reconciled.

## The configuration file is only for access control

An endpoint open to everyone needs nothing but its URL in the mapping, which is
why [`config.ini`](config.ini) declares no resource. An access-controlled
endpoint is declared with its credentials, in a `[RESOURCE:<name>]` section
whose `url` is the endpoint the mapping names:

```ini
[RESOURCE:clinical_trials]
resource_type=SPARQL_ENDPOINT
url=https://data.com/database/query
username={ENDPOINT_USER}
password={ENDPOINT_PASSWORD}
```

`{ENV_VAR}` placeholders in `url`, `iri`, `username` and `password` are replaced
with environment variables, so credentials need not be written to the file at
all.

| Option | Meaning |
| --- | --- |
| `resource_type` | `SPARQL_ENDPOINT` |
| `url` | where the endpoint is queried |
| `iri` | the endpoint the mapping names, when it is not `url`. A mapping naming a production endpoint then runs against a local or staging one, declared as `url` |
| `username`, `password` | HTTP Basic Authentication credentials |
| `method` | `GET` or `POST`. By default, `GET`, or `POST` for a query too long for a URL |
| `timeout` | seconds to wait for the answer to the query (default `300`) |

Credentials written in the endpoint URL of the mapping (`https://user:password@...`)
are rejected, so that they never end up in a mapping.

## Function parameters

| Parameter | Meaning |
| --- | --- |
| `grel:valueParam` | the value to reconcile |
| `morph-fn:sparqlEndpointUrl` | the URL of the SPARQL endpoint the query is sent to |
| `morph-fn:sparqlQuery` | the SELECT query returning `?entity_iri` and `?matching_value_1` |

The endpoint and the query are constants of the mapping, since they are sent
before any data is read. A value matching no entity yields no triple. A value
matching several entities yields one triple per matched entity.

## What this example generates

The patients of [`patients.csv`](patients.csv) are linked to the clinical trials
they participate in, using the IRIs of the knowledge graph:

```
<urn:id:p1> <https://example.org/ontology/participatesIn> <https://data.com/id/1> .
<urn:id:p2> <https://example.org/ontology/participatesIn> <https://data.com/id/2> .
<urn:id:p2> <https://example.org/ontology/participatesIn> <https://data.com/id/3> .
```

## YARRRML

The same reconciliation in YARRRML, with the query as a block scalar:

```yaml
prefixes:
    grel: http://users.ugent.be/~bjdmeest/function/grel.ttl#
    morph-fn: "urn:morph:function:"
    bi: https://example.org/ontology/
mappings:
    patients:
        sources:
            - access: patients.csv
              referenceFormulation: csv
        subjects: urn:id:$(patient_id)
        predicateobjects:
            - p: bi:participatesIn
              o:
                function: morph-fn:reconcileEntityOverSPARQL
                type: iri
                parameters:
                    - parameter: morph-fn:sparqlEndpointUrl
                      value: http://127.0.0.1:8890/sparql
                    - parameter: morph-fn:sparqlQuery
                      value: |
                        PREFIX schema: <https://schema.org/>
                        SELECT ?entity_iri ?matching_value_1 WHERE {
                            GRAPH ?g {
                                ?entity_iri a schema:MedicalStudy ;
                                            schema:identifier ?matching_value_1 .
                            }
                        }
                    - parameter: grel:valueParam
                      value: $(clinical_trial_id)
```

## Writing your own stateful function

Any user-defined function can be given a shared context in the same way, kept
in memory or, when large, in an on-disk table; see
[`../stateful_udfs.py`](../stateful_udfs.py).
