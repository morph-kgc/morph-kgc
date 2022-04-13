# Guide

## Installation

In the following we provide different ways to install and run Morph-KGC. [Here](https://github.com/oeg-upm/Morph-KGC/wiki/Configuration) you can find out how to generate the configuration file. If you are using relational databases you may need to install additional libraries, check [here](https://github.com/oeg-upm/Morph-KGC/wiki/Relational-Databases).

### PyPi

**[PyPi](https://pypi.org/project/morph-kgc/)** is the fastest way to install Morph-KGC:
```
pip install morph-kgc
```

We recommend to use [virtual environments](https://docs.python.org/3/library/venv.html#) to install Morph-KGC.

### From Source

You can also grab the latest source code from the [GitHub repository](https://github.com/oeg-upm/morph-kgc). Clone the repository:
```
git clone https://github.com/oeg-upm/morph-kgc.git
```

Access the root directory of the repository:
```
cd morph-kgc
```

Install Morph-KGC:
```
pip3 install .
```

## Usage

### Command Line

To run the engine using the command line you just need to execute the following:

```
python3 -m morph_kgc path/to/config.ini
```

### Library

Morph-KGC provides different methods to materialize the RDF knowledge graph. It integrates with RDFlib and Oxigraph to easily create and work with knowledge graphs in Python.

The methods in the API accept the **config** as a **string** or as the **path** to a file.

```
import morph_kgc

config = """
            [DataSource1]
            mappings=/path/to/mapping/mapping_file.rml.ttl
            db_url=mysql+pymysql://user:password@localhost:3306/db_name
         """
```

#### [RDFlib](https://rdflib.readthedocs.io/en/stable/)

**`materialize`**

```
# generate the triples and load them to an RDFlib graph

graph = morph_kgc.materialize('config')
# or
graph = morph_kgc.materialize('/path/to/config.ini')

# work with the RDFlib graph
q_res = graph.query(' SELECT DISTINCT ?classes WHERE { ?s a ?classes } ')
```

{==

*__Note__: [RDFlib](https://rdflib.readthedocs.io/en/stable/) does not support [RDF-star](https://w3c.github.io/rdf-star/cg-spec/editors_draft.html), hence `materialize` does not support [RML-star](https://kg-construct.github.io/rml-star-spec/).*

==}

#### [Oxigraph](https://oxigraph.org/pyoxigraph/stable/index.html)

**`materialize_oxigraph`**

```
# generate the triples and load them to Oxigraph

graph = morph_kgc.materialize_oxigraph('config')
# or
graph = morph_kgc.materialize_oxigraph('/path/to/config.ini')

# work with Oxigraph
q_res = graph.query(' SELECT DISTINCT ?classes WHERE { ?s a ?classes } ')
```

#### Set of Triples

**`materialize_set`**

```
# create a Python Set with the triples

graph = morph_kgc.materialize_set('config')
# or
graph = morph_kgc.materialize_set('/path/to/config.ini')

# work with the Python set
print(len(graph))
```

## Advanced Setup

### Relational Databases

The supported DBMSs are **[MySQL](https://www.mysql.com/)**, **[PostgreSQL](https://www.postgresql.org/)**, **[Oracle](https://www.oracle.com/database/)**, **[Microsoft SQL Server](https://www.microsoft.com/sql-server)**, **[MariaDB](https://mariadb.org/)** and **[SQLite](https://www.sqlite.org/index.html)**. To use relational databases it is neccessary to first **install the DBAPI driver**. We recommend the following ones:

- MySQL: [PyMySQL](https://pypi.org/project/PyMySQL/).
- PostgreSQL: [psycopg2](https://pypi.org/project/psycopg2/).
- Oracle: [cx-Oracle](https://pypi.org/project/cx-Oracle/).
- Microsoft SQL Server: [pymssql](https://pypi.org/project/pymssql/).
- MariaDB: [PyMySQL](https://pypi.org/project/PyMySQL/).
- SQLite: does not need any additional DBAPI driver.

Morph-KGC relies on [SQLAlchemy](https://www.sqlalchemy.org/). Many DBAPI drivers are supported, you can check the full list [here](https://docs.sqlalchemy.org/en/14/dialects/index.html#included-dialects).

### Tabular Files

The supported tabular files formats are **[CSV](https://en.wikipedia.org/wiki/Comma-separated_values)**, **[TSV](https://en.wikipedia.org/wiki/Tab-separated_values)**, **[Excel](https://www.microsoft.com/en-us/microsoft-365/excel)**, **[Parquet](https://parquet.apache.org/documentation/latest/)**, **[Feather](https://arrow.apache.org/docs/python/feather.html)**, **[ORC](https://orc.apache.org/)**, **[Stata](https://www.stata.com/)**, **[SAS](https://www.sas.com)**, **[SPSS](https://www.ibm.com/analytics/spss-statistics-software)** and **[ODS](https://en.wikipedia.org/wiki/OpenDocument)**.

To use some tabular data formats it is neccessary to install some libraries:

- : [PyMySQL](https://pypi.org/project/PyMySQL/).
- PostgreSQL: [psycopg2](https://pypi.org/project/psycopg2/).
- Oracle: [cx-Oracle](https://pypi.org/project/cx-Oracle/).
- Microsoft SQL Server: [pymssql](https://pypi.org/project/pymssql/).
- MariaDB: [PyMySQL](https://pypi.org/project/PyMySQL/).
- SQLite: does not need any additional DBAPI driver.

#### ODS

### Hierarchical Files

#### JSON

#### XML
