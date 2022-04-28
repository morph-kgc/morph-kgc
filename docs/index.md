<figure markdown>
  ![Morph-KGC](assets/logo.png){ width="300" }
</figure>

#

**Morph-KGC** is an engine that constructs **[RDF](https://www.w3.org/TR/rdf11-concepts/)** and **[RDF-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html)** knowledge graphs from heterogeneous data sources with the **[R2RML](https://www.w3.org/TR/r2rml/)**, **[RML](https://rml.io/specs/rml/)** and **[RML-star](https://kg-construct.github.io/rml-star-spec/)** mapping languages. Morph-KGC is built on top of [pandas](https://pandas.pydata.org/) and it leverages *mapping partitions* to significantly reduce execution times and memory consumption for large data sources.

## Main Features

- Supports **[R2RML](https://www.w3.org/TR/r2rml/)**, **[RML](https://rml.io/specs/rml/)** and **[RML-star](https://kg-construct.github.io/rml-star-spec/)** mapping languages.
- Input data formats:
    - **Relational databases**: **[MySQL](https://www.mysql.com/)**, **[PostgreSQL](https://www.postgresql.org/)**, **[Oracle](https://www.oracle.com/database/)**, **[Microsoft SQL Server](https://www.microsoft.com/sql-server)**, **[MariaDB](https://mariadb.org/)**, **[SQLite](https://www.sqlite.org)**.
    - **Tabular files**: **[CSV](https://en.wikipedia.org/wiki/Comma-separated_values)**, **[TSV](https://en.wikipedia.org/wiki/Tab-separated_values)**, **[Excel](https://www.microsoft.com/en-us/microsoft-365/excel)**, **[Parquet](https://parquet.apache.org/documentation/latest/)**, **[Feather](https://arrow.apache.org/docs/python/feather.html)**, **[ORC](https://orc.apache.org/)**, **[Stata](https://www.stata.com/)**, **[SAS](https://www.sas.com)**, **[SPSS](https://www.ibm.com/analytics/spss-statistics-software)**, **[ODS](https://en.wikipedia.org/wiki/OpenDocument)**.
    - **Hierarchical files**: **[JSON](https://www.json.org)**, **[XML](https://www.w3.org/TR/xml/)**.
- Output **[RDF](https://www.w3.org/TR/rdf11-concepts/)** and **[RDF-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html)** serializations: **[N-Triples](https://www.w3.org/TR/n-triples/)**, **[N-Triples-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html#n-triples-star)**, **[N-Quads](https://www.w3.org/TR/n-quads/)**, **[N-Quads-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html#n-quads-star)**.
- Integration with **[RDFlib](https://rdflib.readthedocs.io)** and **[Oxigraph](https://oxigraph.org/pyoxigraph)**.
- Runs on **Linux**, **Windows** and **macOS** systems.
- Compatible with **[Python](https://www.python.org/)** 3.7 or higher.
- **Optimized** to materialize large knowledge graphs.

## Licenses

**Morph-KGC** is available under the **[Apache License 2.0](https://github.com/oeg-upm/Morph-KGC/blob/main/LICENSE)**.

The **documentation** is licensed under **[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/)**.

## Author

- **[Julián Arenas-Guerrero](https://github.com/ArenasGuerreroJulian/) - [julian.arenas.guerrero@upm.es](mailto:julian.arenas.guerrero@upm.es)**

*[Ontology Engineering Group](https://oeg.fi.upm.es/)*, *[Universidad Politécnica de Madrid](https://www.upm.es/internacional)*.

## Contributors

See the full list of contributors in **[GitHub](https://github.com/oeg-upm/morph-kgc/graphs/contributors)**.

![OEG](assets/logo-oeg.png){ width="150" align=left } ![UPM](assets/logo-upm.png){ width="161" align=right }
