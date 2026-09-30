# RML 1.2 test cases

Test cases for the constructs introduced in *RML 1.2: Aligning RML with RDF 1.2
Triple Terms and Reifying Triples* (DMKG'26): `rml:TripleTermMap` /
`rml:tripleTermMap`, `rml:NonAssertedTriplesMap`, `rml:reifyingMap`, and
`rml:DirectionMap` / `rml:directionMap` / `rml:direction`.

`RML12TC001a` through `RML12TC010a` are ported from the reference suite that
accompanies the paper (https://github.com/Vergaraaa1/rml12-demo), rewritten
against the `http://w3id.org/rml/` namespace and the logical source form this
engine expects. `RML12TC011a` through `RML12TC015a` cover rules the paper
states but the reference suite does not exercise.

| Case | Covers |
| --- | --- |
| RML12TC001a | Asserted base triple plus an `rdf:reifies` triple term |
| RML12TC002a | `rml:NonAssertedTriplesMap` feeding a triple term |
| RML12TC003a | The `rml:reifyingMap` shortcut |
| RML12TC004a | Directional literal via the `rml:direction` shortcut |
| RML12TC005a | Directional literal via `rml:languageMap` and `rml:directionMap` |
| RML12TC006a | A direction token other than `ltr` or `rtl` is invalid |
| RML12TC007a | A direction without a language is invalid |
| RML12TC008a | A cycle in the `rml:tripleTermMap` graph is invalid |
| RML12TC009a | A base triples map with no predicate-object maps yields no triple term |
| RML12TC010a | Nested triple terms |
| RML12TC011a | Every predicate-object map of the base triples map contributes |
| RML12TC012a | A triple-term map with an `rml:joinCondition` |
| RML12TC013a | A graph map on the base triples map does not reach the triple term |
| RML12TC014a | Nested triple-term maps, each with its own join condition |
| RML12TC015a | A nested triple-term map without a join under one that has one |

## Status

All fifteen pass. `RML12TC011a` runs in a subprocess with a timeout, so a
mapping the parser cannot normalize fails the case instead of hanging the
suite.

The nested join cases, `RML12TC014a` and `RML12TC015a`, are cross-linked on
purpose: each source row points at a different row of the next source, so a
join that pairs rows wrongly, or reads a column from the wrong level, changes
the expected output rather than producing it by luck.

## Why these compare text rather than graphs

The RML-Core cases parse the expected output with rdflib and compare graphs for
isomorphism. rdflib 7.x parses neither RDF 1.2 triple terms (`<<( s p o )>>`)
nor directional language-tagged strings (`"x"@en--ltr`), so these cases compare
sets of N-Triples/N-Quads statements instead. Every mapping generates
deterministic terms, blank node labels included, which keeps that comparison
exact. Helpers live in `conftest.py` and reach the tests through the `rml12`
fixture.
