# Documentation

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

**`morph_kgc.materialize(config)`**

```
# generate the triples and load them to an RDFlib graph

graph = morph_kgc.materialize('config')
# or
graph = morph_kgc.materialize('/path/to/config.ini')

# work with the RDFlib graph
q_res = graph.query(' SELECT DISTINCT ?classes WHERE { ?s a ?classes } ')
```

{==

*__Note:__ [RDFlib](https://rdflib.readthedocs.io/en/stable/) does not support [RDF-star](https://w3c.github.io/rdf-star/cg-spec/editors_draft.html), hence `materialize` does not support [RML-star](https://kg-construct.github.io/rml-star-spec/).*

==}

#### [Oxigraph](https://oxigraph.org/pyoxigraph/stable/index.html)

**`morph_kgc.materialize_oxigraph(config)`**

```
# generate the triples and load them to Oxigraph

graph = morph_kgc.materialize_oxigraph('config')
# or
graph = morph_kgc.materialize_oxigraph('/path/to/config.ini')

# work with Oxigraph
q_res = graph.query(' SELECT DISTINCT ?classes WHERE { ?s a ?classes } ')
```

#### Set of Triples

**`morph_kgc.materialize_set(config)`**

```
# create a Python Set with the triples

graph = morph_kgc.materialize_set('config')
# or
graph = morph_kgc.materialize_set('/path/to/config.ini')

# work with the Python set
print(len(graph))
```

## Configuration

The configuration of Morph-KGC is done via an **[INI file](https://en.wikipedia.org/wiki/INI_file)**. This configuration file can contain the following sections:

**`{++CONFIGURATION++}`**

- Contains the parameters that tune the execution of Morph-KGC. The full list of parameters can he found [here](https://github.com/oeg-upm/Morph-KGC/wiki/Engine-Configuration).

**One section for each `{++DATA SOURCE++}`**

- Each input data source has its own section. This section must provide at least the path to the mapping files. Depending on the data format different parameters will be required. A complete guide for each accepted data format can be found [here](https://github.com/oeg-upm/Morph-KGC/wiki/Data-Source-Configuration).

**`{++DEFAULT++}`**

- It is optional and it declares variables that can be used in all other sections for  convenience. For instance, you can set _mappings_dir: ../testing/mappings_ so that _mapping_dir_ can be used in the rest of sections.

Below is an example configuration file with one input relational source. In this case `DataSource1` is the only data source section, but additional data sources can be considered by including more sections. [Here](https://github.com/oeg-upm/morph-kgc/blob/main/examples/configuration-file-examples/default_config.ini) you can find a configuration file which is more complete.

```
[DEFAULT]
main_dir: ../testing

[CONFIGURATION]
output_file: my-knowledge-graph.nq

[DataSource1]
mappings: ${mappings_dir}/mapping_file.rml.ttl
db_url: mysql+pymysql://user:password@localhost:3306/db_name
```

The parameters of the sections in the INI file are explained below.

### Engine Configuration

|<div style="width:190px">Property</div>|Description|Values|
|-------|-------|-------|
|**`output_file`**|File to write the results to. If it is empty (no value) then an independent output file is generated for each group of the mapping partition.|**Default:** knowledge-graph|
|**`output_dir`**|Directory containing the output RDF files.|**Default:**|
|**`na_values`**|Set of values to be interpreted as _NULL_ when retrieving data from the input sources. The valid values are a list of values separated by commas.|**Default:** ,_#N/A_,_N/A_,_#N/A N/A_,_n/a_,_NA_,_\<NA\>_,_#NA_,_NULL_,_null_,_NaN_,_nan_,_None_|
|**`output_format`**|RDF serialization to use for output results.|**Valid:** _N-TRIPLES_, _N-QUADS_<br>**Default:** _N-QUADS_|
|**`only_printable_characters`**|Remove characters in the genarated RDF that are not printable.|**Valid:** _yes_, _no_, _true_, _false_, _on_, _off_, _1_, _0_<br>**Default:** _no_|
|**`safe_percent_encoding`**|Set of ASCII characters that should not be percent encoded. All characters are encoded by default.|**Example:** _:/_<br>**Default:**|
|**`mapping_partition`**|Mapping partitioning algorithm to use. Mapping partitioning can also be disabled.|**Valid:** _PARTIAL-AGGREGATIONS_, _MAXIMAL_, _no_, _false_, _off_, _0_<br>**Default:** _PARTIAL-AGGREGATIONS_|
|**`infer_sql_datatypes`**|Infer datatypes for relational databases. If a literal already has a datatype in the mapping, then the datatype will not be inferred.|**Valid:** _yes_, _no_, _true_, _false_, _on_, _off_, _1_, _0_<br>**Default:** _no_|
|**`number_of_processes`**|The number of processes to use. If _1_, multiprocessing is disabled.|**Default:** _2 * number of CPUs in the system_|
|**`logging_level`**|Sets the level of the log messages to show.|**Valid:** _DEBUG_, _INFO_, _WARNING_, _ERROR_, _CRITICAL_, _NOTSET_<br>**Default:** _INFO_|
|**`logging_file`**|If not provided, log messages will be redirected to stdout. If a file path is provided, log messages will be written to file.|**Default:**|
|**`oracle_client_lib_dir`**|*lib_dir* directory specified in a call to *[cx_Oracle.init_oracle_client()](https://cx-oracle.readthedocs.io/en/latest/api_manual/module.html#cx_Oracle.init_oracle_client)*.|**Default:**|
|**`oracle_client_config_dir`**|*config_dir* directory specified in a call to *[cx_Oracle.init_oracle_client()](https://cx-oracle.readthedocs.io/en/latest/api_manual/module.html#cx_Oracle.init_oracle_client)*.|**Default:**|

### Data Sources

#### Relational Databases

|<div style="width:70px">Property</div>|Description|Values|
|-------|-------|-------|
|**`mappings`**|To specify the mapping file(s) for the data source.|**Valid:**<br>- The path to one mapping file.<br>- The paths to multiple mapping files separated by commas.<br>- The path to a directory containing all the mapping files for that data source.|
|**`db_url`**|It is a URL that configures the database engine (username, password, hostname, database name). See [here](https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls) how to create the database URLs.|**Example:** _dialect+driver://username:password@host:port/database_|

{==

*__Note:__ to use __Oracle__ you may need to specify `oracle_client_lib_dir` or `oracle_client_config_dir` in the `[CONFIGURATION] section.`*

==}

#### Data Files

|<div style="width:70px">Property</div>|Description|Values|
|-------|-------|-------|
|**`mappings`**|To specify the mapping file(s) for the data source.|**Valid:**<br>- The path to one mapping file.<br>- The paths to multiple mapping files separated by commas.<br>- The path to a directory containing all the mapping files for that data source.|
|**`file_path`**|Specifies the local path or URL of the data file. It is optional since it can be provided within the mapping file with rml:source. If it is provided it will override the local path or URL provided in the mapping files.||

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

The supported tabular files formats are **[CSV](https://en.wikipedia.org/wiki/Comma-separated_values)**, **[TSV](https://en.wikipedia.org/wiki/Tab-separated_values)**, **[Excel](https://www.microsoft.com/en-us/microsoft-365/excel)**, **[Parquet](https://parquet.apache.org/documentation/latest/)**, **[Feather](https://arrow.apache.org/docs/python/feather.html)**, **[ORC](https://orc.apache.org/)**, **[Stata](https://www.stata.com/)**, **[SAS](https://www.sas.com)**, **[SPSS](https://www.ibm.com/analytics/spss-statistics-software)** and **[ODS](https://en.wikipedia.org/wiki/OpenDocument)**. To work with some of them it is neccessary to install some libraries:

- Excel: [OpenPyXL](https://pypi.org/project/openpyxl/).
- Parquet: [PyArrow](https://pypi.org/project/pyarrow/).
- ODS: [OdfPy](https://pypi.org/project/odfpy/).
- Feather: [PyArrow](https://pypi.org/project/pyarrow/).
- ORC: [PyArrow](https://pypi.org/project/pyarrow/).
- SPSS: [pyreadstat](https://pypi.org/project/pyreadstat/).

### Hierarchical Files

The supported hierarchical files formats are **[XML](https://www.w3.org/TR/xml/)** and **[JSON](https://www.json.org/json-en.html)**.

Morph-KGC uses **[XPath 2.0](https://www.w3.org/TR/xpath20/)** to query XML files and **[JSONPath](https://tools.ietf.org/id/draft-goessner-dispatch-jsonpath-00.html)** to query JSON files.

{==

*__Note:__ the specific JSONPath syntax supported by Morph-KGC can be consulted [here](https://github.com/zhangxianbing/jsonpath-python#jsonpath-syntax).*

==}
