<p align="center">
<img src="https://github.com/morph-kgc/morph-kgc/blob/main/logo/logo.png" height="100" alt="morph">
</p>

[![License](https://img.shields.io/pypi/l/morph-kgc.svg)](https://github.com/morph-kgc/morph-kgc/blob/main/LICENSE)
[![DOI](https://zenodo.org/badge/311956260.svg?style=flat)](https://zenodo.org/badge/latestdoi/311956260)
[![Latest PyPI version](https://img.shields.io/pypi/v/morph-kgc?style=flat)](https://pypi.python.org/pypi/morph-kgc)
[![Python Version](https://img.shields.io/pypi/pyversions/morph-kgc.svg)](https://pypi.python.org/pypi/morph-kgc)
[![PyPI status](https://img.shields.io:/pypi/status/morph-kgc?)](https://pypi.python.org/pypi/morph-kgc)
[![build](https://github.com/morph-kgc/morph-kgc/actions/workflows/ci.yml/badge.svg)](https://github.com/morph-kgc/morph-kgc/actions/workflows/ci.yml)
[![Documentation Status](https://readthedocs.org/projects/morph-kgc/badge/?version=latest)](https://morph-kgc.readthedocs.io/en/latest/?badge=latest)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1ByFx_NOEfTZeaJ1Wtw3UwTH3H3-Sye2O?usp=sharing)

**Morph-KGC** is an engine that constructs **[RDF](https://www.w3.org/TR/rdf11-concepts/)** knowledge graphs from heterogeneous data sources with the **[R2RML](https://www.w3.org/TR/r2rml/)** and **[RML](https://w3id.org/rml/core/spec)** mapping languages. Morph-KGC is built on top of [pandas](https://pandas.pydata.org/) and it leverages *mapping partitions* to significantly reduce execution times and memory consumption for large data sources.

## Features :sparkles:

- Supports the **[R2RML](https://www.w3.org/TR/r2rml/)** and **[RML](https://w3id.org/rml/core/spec)** mapping languages.
- User-friendly mappings with **[YARRRML](https://rml.io/yarrrml/spec/)**.
- Transformation functions with **[RML-FNML](https://w3id.org/rml/fnml/spec)**, including **Python user-defined functions**.
- [RDF-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html) generation with **[RML-star](https://w3id.org/rml/star/spec)**.
- **[RML views](https://oa.upm.es/73463/1/_2023___ESWC__RML_Tabular_Views.pdf)** over tabular data sources and [JSON](https://www.json.org) files.
- Integration with **[RDFLib](https://rdflib.readthedocs.io)**, **[Oxigraph](https://pyoxigraph.readthedocs.io/en/latest/)** and [Kafka](https://kafka-python.readthedocs.io).
- **Optimized** to materialize large knowledge graphs.
- **Remote** data and mapping files.
- Input data formats:
    - **Relational databases**: [MySQL](https://www.mysql.com/), [PostgreSQL](https://www.postgresql.org/), [Oracle](https://www.oracle.com/database/), [Microsoft SQL Server](https://www.microsoft.com/sql-server), [MariaDB](https://mariadb.org/), [SQLite](https://www.sqlite.org).
    - **Tabular files**: [CSV](https://en.wikipedia.org/wiki/Comma-separated_values), [TSV](https://en.wikipedia.org/wiki/Tab-separated_values), [Excel](https://www.microsoft.com/en-us/microsoft-365/excel), [Parquet](https://parquet.apache.org/documentation/latest/), [Feather](https://arrow.apache.org/docs/python/feather.html), [ORC](https://orc.apache.org/), [Stata](https://www.stata.com/), [SAS](https://www.sas.com), [SPSS](https://www.ibm.com/analytics/spss-statistics-software), [ODS](https://en.wikipedia.org/wiki/OpenDocument).
    - **Hierarchical files**: [JSON](https://www.json.org), [XML](https://www.w3.org/TR/xml/).
    - **In-memory data structures**: [Python Dictionaries](https://docs.python.org/3/tutorial/datastructures.html#dictionaries), [DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html).
	- **Cloud data lake solutions**: [Databricks](https://www.databricks.com/).

## Documentation :bookmark_tabs:

**[Read the documentation](https://morph-kgc.readthedocs.io/en/latest/documentation/)**.

## Tutorial :woman_teacher:

Learn quickly with the tutorial in **[Google Colaboratory](https://colab.research.google.com/drive/1ByFx_NOEfTZeaJ1Wtw3UwTH3H3-Sye2O?usp=sharing)**!

## Getting Started :rocket:

**[PyPi](https://pypi.org/project/morph-kgc/)** is the fastest way to install Morph-KGC:
```bash
pip install morph-kgc
```

We recommend to use [virtual environments](https://docs.python.org/3/library/venv.html#) to install Morph-KGC.

To run the engine via **command line** you just need to execute the following:
```bash
python3 -m morph_kgc config.ini
```

Check the **[documentation](https://morph-kgc.readthedocs.io/en/latest/documentation/#configuration)** to see how to generate the configuration **INI file**. **[Here](https://github.com/morph-kgc/morph-kgc/blob/main/examples/configuration-file/default_config.ini)** you can also see an example INI file.

It is also possible to run Morph-KGC as a **library** with **[RDFLib](https://rdflib.readthedocs.io)** and **[Oxigraph](https://pyoxigraph.readthedocs.io/en/latest/)**:
```python
import morph_kgc

# generate the triples and load them to an RDFLib graph
g_rdflib = morph_kgc.materialize('/path/to/config.ini')
# work with the RDFLib graph
q_res = g_rdflib.query('SELECT DISTINCT ?classes WHERE { ?s a ?classes }')

# generate the triples and load them to Oxigraph
g_oxigraph = morph_kgc.materialize_oxigraph('/path/to/config.ini')
# work with Oxigraph
q_res = g_oxigraph.query('SELECT DISTINCT ?classes WHERE { ?s a ?classes }')

# the methods above also accept the config as a string
config = """
            [DataSource1]
            mappings: /path/to/mapping/mapping_file.rml.ttl
            db_url: mysql+pymysql://user:password@localhost:3306/db_name
         """
g_rdflib = morph_kgc.materialize(config)
```

## License :unlock:

Morph-KGC is available under the **[Apache License 2.0](https://github.com/morph-kgc/morph-kgc/blob/main/LICENSE)**.

## Author & Contact :mailbox_with_mail:

- **[Julián Arenas-Guerrero](https://github.com/arenas-guerrero-julian/) - [julian.arenas.guerrero@upm.es](mailto:julian.arenas.guerrero@upm.es)**

*[Ontology Engineering Group](https://oeg.fi.upm.es)*, *[Universidad Politécnica de Madrid](https://www.upm.es/internacional)*.

## Citing :speech_balloon:

If you used Morph-KGC in your work, please cite the **[SWJ paper](https://www.doi.org/10.3233/SW-223135)**:

```bib
@article{arenas2024morph,
  title     = {{Morph-KGC: Scalable knowledge graph materialization with mapping partitions}},
  author    = {Arenas-Guerrero, Julián and Chaves-Fraga, David and Toledo, Jhon and Pérez, María S. and Corcho, Oscar},
  journal   = {Semantic Web},
  publisher = {IOS Press},
  issn      = {2210-4968},
  year      = {2024},
  doi       = {10.3233/SW-223135},
  volume    = {15},
  number    = {1},
  pages     = {1-20}
}
```

## Sponsor :shield:

<p align="center">
<img src="https://github.com/morph-kgc/morph-kgc-docs/blob/main/docs/assets/BASF.png" height="100" alt="BASF">
</p>
