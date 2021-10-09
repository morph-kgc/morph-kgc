[![GitHub license](https://img.shields.io/badge/license-Apache%20License%202.0-blue.svg?style=flat)](https://github.com/oeg-upm/Morph-KGC/blob/main/LICENSE)
[![DOI](https://zenodo.org/badge/311956260.svg?style=flat)](https://zenodo.org/badge/latestdoi/311956260)
[![Latest PyPI version](https://img.shields.io/pypi/v/morph-kgc?style=flat)](https://pypi.python.org/pypi/morph-kgc)
[![version](https://img.shields.io/badge/python-3.7+-blue.svg?style=flat)](https://www.python.org/downloads/release/python-370/)
[![GitHub commit activity](https://img.shields.io/github/commit-activity/m/oeg-upm/Morph-KGC?style=flat)](https://github.com/oeg-upm/Morph-KGC/commits/main)

<p align="center">
<img src="https://github.com/oeg-upm/morph-website/blob/master/morph-group/src/assets/logo.png" height="100" alt="morph">
</p>

Morph-KGC is an engine that constructs [RDF](https://www.w3.org/TR/rdf11-concepts/) knowledge graphs from heterogeneous data sources with [R2RML](https://www.w3.org/TR/r2rml/) and [RML](https://rml.io/specs/rml/) mapping languages. Morph-KGC is built on top of [pandas](https://pandas.pydata.org/) and it leverages *mapping partitions* to significantly reduce execution times and memory consumption for large data sources.

## Main Features

- Supports [R2RML](https://www.w3.org/TR/r2rml/) and [RML](https://rml.io/specs/rml/) mapping languages.
- Input data formats:
  - Relational databases: [MySQL](https://www.mysql.com/), [PostgreSQL](https://www.postgresql.org/), [Oracle](https://www.oracle.com/database/), [Microsoft SQL Server](https://www.microsoft.com/sql-server), [MariaDB](https://mariadb.org/), [SQLite](https://www.sqlite.org/index.html).
  - Tabular files: [CSV](https://en.wikipedia.org/wiki/Comma-separated_values), [TSV](https://en.wikipedia.org/wiki/Tab-separated_values), [Excel](https://www.microsoft.com/en-us/microsoft-365/excel), [Parquet](https://parquet.apache.org/documentation/latest/), [Feather](https://arrow.apache.org/docs/python/feather.html), [ORC](https://orc.apache.org/), [Stata](https://www.stata.com/), [SAS](https://www.sas.com), [SPSS](https://www.ibm.com/analytics/spss-statistics-software).
  - Hierarchical files: [JSON](https://www.json.org/json-en.html), [XML](https://www.w3.org/TR/xml/).
- Output RDF serializations: [N-Triples](https://www.w3.org/TR/n-triples/), [N-Quads](https://www.w3.org/TR/n-quads/).
- Runs on Linux, Windows and macOS systems.
- Compatible with Python 3.7 or higher.
- Optimized to materialize large knowledge graphs.
- Multiple configuration options.
- Available under the [Apache License 2.0](https://github.com/oeg-upm/Morph-KGC/blob/main/LICENSE).

## Installation and Usage

[PyPi](https://pypi.org/project/morph-kgc/) is the fastest way to install Morph-KGC:
```
pip install morph-kgc
```

To run the engine you just need to execute the following:
```
python3 -m morph_kgc configuration.ini
```

[Here](https://github.com/oeg-upm/Morph-KGC/wiki/Configuration) you can see how to generate the configuration file. It is also possible to run Morph-KGC as a library with [RDFlib](https://rdflib.readthedocs.io/en/stable/):
```python
import morph_kgc

# generate the triples and load them to an RDFlib graph
graph = morph_kgc.materialize('/path/to/configuration.ini')

# work with the graph
graph.query(' SELECT DISTINCT ?classes WHERE { ?s a ?classes } ')
```

## Wiki

Check the **[wiki](https://github.com/oeg-upm/Morph-KGC/wiki)** with all the information.

**[Getting Started](https://github.com/oeg-upm/Morph-KGC/wiki/Getting-Started)**

**[Usage](https://github.com/oeg-upm/Morph-KGC/wiki/Usage)**

**[Configuration](https://github.com/oeg-upm/Morph-KGC/wiki/Configuration)**
- **[Engine](https://github.com/oeg-upm/Morph-KGC/wiki/Engine-Configuration)**
- **[Data Sources](https://github.com/oeg-upm/Morph-KGC/wiki/Data-Source-Configuration)**
  - [Relational Databases](https://github.com/oeg-upm/Morph-KGC/wiki/Relational-Databases)
  - [Data Files](https://github.com/oeg-upm/Morph-KGC/wiki/Data-Files)

**[Tutorial](https://github.com/oeg-upm/Morph-KGC/wiki/Tutorial)**

**[Features](https://github.com/oeg-upm/Morph-KGC/wiki/Features)**

**[Academic Publications](https://github.com/oeg-upm/Morph-KGC/wiki/Academic-Publications)**

**[License](https://github.com/oeg-upm/Morph-KGC/wiki/License)**

**[FAQ](https://github.com/oeg-upm/Morph-KGC/wiki/FAQ)**

## Authors

- **Julián Arenas-Guerrero (julian.arenas.guerrero@upm.es)**
- David Chaves-Fraga
- Jhon Toledo
- Oscar Corcho

Ontology Engineering Group, Universidad Politécnica de Madrid | 2020 - Present
