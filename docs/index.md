<figure markdown>
  ![Morph-KGC](assets/logo.png){ width="300" }
</figure>

#

**Morph-KGC** is an engine that constructs **[RDF](https://www.w3.org/TR/rdf11-concepts/)** knowledge graphs from heterogeneous data sources, using any of the following declarative mapping languages: **[R2RML](https://www.w3.org/TR/r2rml/)**, **[RML](https://rml.io/specs/rml/)** and **[RML-star](https://kg-construct.github.io/rml-star-spec/)**. Morph-KGC is built on top of [pandas](https://pandas.pydata.org/) and it leverages *mapping partitions* to significantly reduce execution times and memory consumption for large data sources.

## Features

- Supports several declarative mapping languages: **[R2RML](https://www.w3.org/TR/r2rml/)**, **[RML](https://rml.io/specs/rml/)** and **[RML-star](https://kg-construct.github.io/rml-star-spec/)**.
- Input data formats:
    - **Relational databases**: **[MySQL](https://www.mysql.com/)**, **[PostgreSQL](https://www.postgresql.org/)**, **[Oracle](https://www.oracle.com/database/)**, **[Microsoft SQL Server](https://www.microsoft.com/sql-server)**, **[MariaDB](https://mariadb.org/)**, **[SQLite](https://www.sqlite.org/index.html)**.
    - **Tabular files**: **[CSV](https://en.wikipedia.org/wiki/Comma-separated_values)**, **[TSV](https://en.wikipedia.org/wiki/Tab-separated_values)**, **[Excel](https://www.microsoft.com/en-us/microsoft-365/excel)**, **[Parquet](https://parquet.apache.org/documentation/latest/)**, **[Feather](https://arrow.apache.org/docs/python/feather.html)**, **[ORC](https://orc.apache.org/)**, **[Stata](https://www.stata.com/)**, **[SAS](https://www.sas.com)**, **[SPSS](https://www.ibm.com/analytics/spss-statistics-software)**, **[ODS](https://en.wikipedia.org/wiki/OpenDocument)**.
    - **Hierarchical files**: **[JSON](https://www.json.org/json-en.html)**, **[XML](https://www.w3.org/TR/xml/)**.
- Output **[RDF](https://www.w3.org/TR/rdf11-concepts/)** serializations: **[N-Triples](https://www.w3.org/TR/n-triples/)**, **[N-Quads](https://www.w3.org/TR/n-quads/)**.
- Runs on **Linux**, **Windows** and **macOS** operating systems.
- Compatible with **Python** 3.7 or higher.
- **Optimized** to materialize large knowledge graphs.

## Licenses

**Morph-KGC** is available under the **[Apache License 2.0](https://github.com/oeg-upm/Morph-KGC/blob/main/LICENSE)**.

The **documentation** is licensed under **[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/)**.

## Author

- **[Julián Arenas-Guerrero](https://github.com/ArenasGuerreroJulian/) - [julian.arenas.guerrero@upm.es](mailto:julian.arenas.guerrero@upm.es)**

*[Ontology Engineering Group](https://oeg.fi.upm.es/)*, *[Universidad Politécnica de Madrid](https://www.upm.es/internacional)*.

## Contributors

See the full list of contributors in **[GitHub](https://github.com/oeg-upm/morph-kgc/graphs/contributors)**.
