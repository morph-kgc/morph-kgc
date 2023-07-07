# Why Morph-KGC

## Reasons to Use Morph-KGC

### Performance

Morph-KGC relies on the usage of **[mapping partitioning](https://content.iospress.com/download/semantic-web/sw223135?id=semantic-web%2Fsw223135)** to achieve efficient knowledge graph materialization. Morph-KGC can run mapping rules in **parallel** using the full power of the CPU. Additional optimizations are also implemented to increase efficiency: **[redundant self-join elimination](https://content.iospress.com/download/semantic-web/sw223135?id=semantic-web%2Fsw223135)**, **[vectorized operations](https://en.wikipedia.org/wiki/Array_programming)**, **[hash joins](https://en.wikipedia.org/wiki/Hash_join)** and more.

### W3C Compliance

Morph-KGC adopts the **[W3C](https://www.w3.org/)** Recommendation **[R2RML](https://www.w3.org/TR/r2rml/)** mapping language to map relational databases to **[RDF](https://www.w3.org/TR/rdf11-concepts/)**. In addition, it supports **[RML-Core](https://w3id.org/rml/core/spec)**, **[RML-star](https://w3id.org/rml/star/spec)** (for **[RDF-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html)** generation) and **[RML-FNML](https://w3id.org/rml/fnml/spec)** (for data transformations), which are being further developed by the **[Knowledge Graph Construction W3C Community Group](https://www.w3.org/community/kg-construct/)**.

### Usability

Morph-KGC supports **[YARRRML](https://rml.io/yarrrml/spec/)**, a user-friendly serialization of **[RML](https://w3id.org/rml/core/spec)** that eases mapping development. Morph-KGC natively processes **[YARRRML](https://rml.io/yarrrml/spec/)**, there is no need of previous mapping translation.

### Reliability

Morph-KGC is being used for all our knowledge graph construction projects at the **[Ontology Engineering Group](https://oeg.fi.upm.es/)**, and other organizations have adopted it as well for their **[RDF](https://www.w3.org/TR/rdf11-concepts/)** and **[RDF-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html)** materialization pipelines. This is why we put strong emphasis in keeping it **stable**, with **solid** releases. The engine is under **[continuous integration](https://github.com/morph-kgc/morph-kgc/actions)** using **[R2RML test cases](https://www.w3.org/2001/sw/rdb2rdf/test-cases/)**, **[RML test cases](https://github.com/kg-construct/rml-core/tree/main/test-cases)** and **[RML-star test cases](https://github.com/kg-construct/rml-star/tree/main/test-cases)**, in addition to more complex ones.

### Free & Open Source

Morph-KGC is available under the permissive **[Apache License 2.0](https://github.com/morph-kgc/morph-kgc/blob/main/LICENSE)**, which allows commercial use, modification, distribution, patent use and private use.

## Featured In
- [Practical guide for the publication of linked data from **datos.gob.es**](https://datos.gob.es/sites/default/files/doc/file/guia-publicacion-datos-enlazados.pdf).
- [Loading Data in **GraphDB**: Best Practices and Tools](https://www.ontotext.com/blog/loading-data-in-graphdb-best-practices-and-tools/).
- **[FAIR Cookbook](https://faircookbook.elixir-europe.org/content/recipes/interoperability/rdf-conversion.html)**.
- **[Data2Services](https://d2s.semanticscience.org/docs/convert-rml)**.
- [RDF, RML, YARRRML: A basic tutorial to create Linked Data from a relational database table](https://katharinabrunner.de/2022/03/rdf-rml-yarrrml-kglab-morph-kgc/).

## Integrated In
- [**kglab** - Graph Data Science](https://github.com/DerwenAI/kglab).

![OEG](assets/logo-oeg.png){ width="150" align=left } ![UPM](assets/logo-upm.png){ width="161" align=right }
