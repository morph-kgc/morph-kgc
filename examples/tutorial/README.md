## Tutorial

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1ByFx_NOEfTZeaJ1Wtw3UwTH3H3-Sye2O?usp=sharing)

Learn quickly with the tutorial in **[Google Colaboratory](https://colab.research.google.com/drive/1ByFx_NOEfTZeaJ1Wtw3UwTH3H3-Sye2O?usp=sharing)**!

It starts from a three-row CSV file and builds up to a real dataset, one construct at a time:

1. From rows to triples — what a mapping is.
2. Your first knowledge graph — logical source, subject map, predicate-object map.
3. Term maps — `rml:constant`, `rml:reference`, `rml:template`, term types, datatypes and language tags.
4. Linking two sources with a join.
5. Configuration files, and the ways to get the output.
6. A real dataset, and SPARQL.
7. Saying something about a triple — [RDF 1.2](https://www.w3.org/TR/rdf12-concepts/) triple terms and reifiers with [RML 1.2](https://sferrada.com/publication/2026-dmkg-rml-12/2026-dmkg-rml-12.pdf).
8. Large data from the command line.

The notebook writes its small examples as it goes, and fetches the two larger ones from this directory:

| File | Used in | Contents |
| --- | --- | --- |
| [`Morph-KGC.ipynb`](Morph-KGC.ipynb) | — | The notebook itself |
| [`mapping.gtfs.ttl`](mapping.gtfs.ttl) | Parts 6 and 8 | RML mapping of the [GTFS-Madrid-Bench](https://github.com/oeg-upm/gtfs-bench), over the [CSV data](../csv/data) |
| [`mapping.somef.ttl`](mapping.somef.ttl) | Part 7 | RML 1.2 mapping producing RDF 1.2 reifiers, with the extraction confidence and technique of every fact |
| [`oeg-upm_morph-kgc.json`](oeg-upm_morph-kgc.json) | Part 7 | [SoMEF](https://github.com/KnowledgeCaptureAndDiscovery/somef) metadata of this repository, the input of the mapping above |
