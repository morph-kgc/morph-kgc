# Documentation

## Installation

In the following we describe different ways in which you can install and use Morph-KGC. Depending on the data sources that you need to work with, you may need to install additional libraries, see **[Advanced Setup](https://morph-kgc.readthedocs.io/en/latest/documentation/#advanced-setup)**.

### PyPi

**[PyPi](https://pypi.org/project/morph-kgc/)** is the fastest way to install Morph-KGC:
```
pip install morph-kgc
```

We recommend to use **[virtual environments](https://docs.python.org/3/library/venv.html#)** to install Morph-KGC.

### From Source

You can also grab the latest source code from the **[GitHub repository](https://github.com/oeg-upm/morph-kgc)**:
```
pip install git+https://github.com/oeg-upm/morph-kgc.git
```

## Usage

Morph-KGC uses an **[INI file](https://en.wikipedia.org/wiki/INI_file)** to configure the materialization process, see **[Configuration](https://morph-kgc.readthedocs.io/en/latest/documentation/#configuration)**.

### Command Line

To run the engine using the **command line** you just need to execute the following:

```
python3 -m morph_kgc path/to/config.ini
```

### Library

Morph-KGC can be used as a **library**, providing different methods to materialize the **[RDF](https://www.w3.org/TR/rdf11-concepts/)** or **[RDF-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html)** knowledge graph. It integrates with **[RDFLib](https://rdflib.readthedocs.io/en/stable/)** and **[Oxigraph](https://pyoxigraph.readthedocs.io/en/latest/)** to easily create and work with knowledge graphs in **[Python](https://www.python.org/)**.

The methods in the **API** accept the **config as a string or as the path to an INI file**.

```
import morph_kgc

config = """
            [DataSource1]
            mappings: /path/to/mapping/mapping_file.rml.ttl
            db_url: mysql+pymysql://user:password@localhost:3306/db_name
         """
```

{==

*__Note:__ Morph-KGC does not parallelize when running as a [library](https://morph-kgc.readthedocs.io/en/latest/documentation/#library).*

==}

#### [RDFLib](https://rdflib.readthedocs.io/en/stable/)

**`morph_kgc.materialize(config)`**

Materialize the knowledge graph to **[RDFLib](https://rdflib.readthedocs.io/en/stable/)**.

```
# generate the triples and load them to an RDFLib graph

graph = morph_kgc.materialize(config)
# or
graph = morph_kgc.materialize('/path/to/config.ini')

# work with the RDFLib graph
q_res = graph.query(' SELECT DISTINCT ?classes WHERE { ?s a ?classes } ')
```

{==

*__Note:__ [RDFLib](https://rdflib.readthedocs.io/en/stable/) does not support [RDF-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html), hence `materialize` does not support [RML-star](https://kg-construct.github.io/rml-star-spec/).*

==}

#### [Oxigraph](https://pyoxigraph.readthedocs.io/en/latest/)

**`morph_kgc.materialize_oxigraph(config)`**

Materialize the knowledge graph to **[Oxigraph](https://pyoxigraph.readthedocs.io/en/latest/)**.

```
# generate the triples and load them to Oxigraph

graph = morph_kgc.materialize_oxigraph(config)
# or
graph = morph_kgc.materialize_oxigraph('/path/to/config.ini')

# work with Oxigraph
q_res = graph.query(' SELECT DISTINCT ?classes WHERE { ?s a ?classes } ')
```

#### Set of Triples

**`morph_kgc.materialize_set(config)`**

Materialize the knowledge graph to a Python **Set of triples**.

```
# create a Python Set with the triples

graph = morph_kgc.materialize_set(config)
# or
graph = morph_kgc.materialize_set('/path/to/config.ini')

# work with the Python set
print(len(graph))
```

## Configuration

The configuration of Morph-KGC is done via an **[INI file](https://en.wikipedia.org/wiki/INI_file)**. This configuration file can contain the following sections:

**`{++CONFIGURATION++}`**

- Contains the parameters that **tune** the execution of Morph-KGC, see **[Engine Configuration](https://morph-kgc.readthedocs.io/en/latest/documentation/#engine-configuration)**.

**One section for each `{++DATA SOURCE++}`**

- Each input data source has its own section, see **[Data Sources](https://morph-kgc.readthedocs.io/en/latest/documentation/#data-sources)**.

**`{++DEFAULT++}`**

- It is **optional** and it declares variables that can be used in all other sections for  convenience. For instance, you can set _main_dir: ../testing_ so that _main_dir_ can be used in the rest of the sections.

Below is an example configuration file with one input relational source. In this case `DataSource1` is the only data source section, but other data sources can be considered by including additional sections. **[Here](https://github.com/oeg-upm/morph-kgc/blob/main/examples/configuration-file/default_config.ini)** you can find a configuration file which is more complete.

```
[DEFAULT]
main_dir: ../testing

[CONFIGURATION]
output_file: knowledge-graph.nt

[DataSource1]
mappings: ${mappings_dir}/mapping_file.rml.ttl
db_url: mysql+pymysql://user:password@localhost:3306/db_name
```

The parameters of the sections in the **[INI file](https://en.wikipedia.org/wiki/INI_file)** are explained below.

### Engine Configuration

The execution of Morph-KGC can be **tuned** via the **`CONFIGURATION`** section in the **[INI file](https://en.wikipedia.org/wiki/INI_file)**. This section can be empty, in which case Morph-KGC will use the **default** property values.

|<div style="width:190px">Property</div>|Description|Values|
|-------|-------|-------|
|**`output_file`**|File to write the resulting knowledge graph to.|**Default:** _knowledge-graph.nt_|
|**`output_dir`**|Directory to write the resulting knowledge graph to. If it is specified, `output_file` will be ignored and multiple output files will generated, one for each mapping partition.|**Default:**|
|**`na_values`**|Set of values to be interpreted as _NULL_ when retrieving data from the input sources. The set of values must be separated by commas.|**Default:** _#N/A_,_N/A_,_#N/A N/A_,_n/a_,_NA_,_<NA\>_,_#NA_,_NULL_,_null_,_NaN_,_nan_,,_None_|
|**`output_format`**|RDF serialization to use for the resulting knowledge graph.|**Valid:** _[N-TRIPLES](https://www.w3.org/TR/n-triples/)_, _[N-QUADS](https://www.w3.org/TR/n-quads/)_<br>**Default:** _[N-TRIPLES](https://www.w3.org/TR/n-triples/)_|
|**`only_printable_characters`**|Remove characters in the genarated RDF that are not printable.|**Valid:** _yes_, _no_, _true_, _false_, _on_, _off_, _1_, _0_<br>**Default:** _no_|
|**`safe_percent_encoding`**|Set of ASCII characters that should not be percent encoded. All characters are encoded by default.|**Example:** _:/_<br>**Default:**|
|**`mapping_partitioning`**|[Mapping partitioning](https://content.iospress.com/download/semantic-web/sw223135?id=semantic-web%2Fsw223135) algorithm to use. Mapping partitioning can also be disabled.|**Valid:** _PARTIAL-AGGREGATIONS_, _MAXIMAL_, _no_, _false_, _off_, _0_<br>**Default:** _PARTIAL-AGGREGATIONS_|
|**`infer_sql_datatypes`**|Infer datatypes for relational databases. If a [datatypeable term map](https://www.w3.org/TR/r2rml/#dfn-datatypeable-term-map) has a _[rr:datatype](https://www.w3.org/ns/r2rml#datatype)_ property, then the datatype will not be inferred.|**Valid:** _yes_, _no_, _true_, _false_, _on_, _off_, _1_, _0_<br>**Default:** _no_|
|**`number_of_processes`**|The number of processes to use. If _1_, Morph-KGC will use sequential processing (minimizing memory consumption), otherwise parallel processing is used (minimizing execution time).|**Default:** _2 * number of CPUs in the system_|
|**`logging_level`**|Sets the [level](https://docs.python.org/3/library/logging.html#logging-levels) of the log messages to show.|**Valid:** _DEBUG_, _INFO_, _WARNING_, _ERROR_, _CRITICAL_, _NOTSET_<br>**Default:** _INFO_|
|**`logging_file`**|If not provided, log messages will be redirected to _stdout_. If a file path is provided, log messages will be written to the file.|**Default:**|
|**`oracle_client_lib_dir`**|*lib_dir* directory specified in a call to *[cx_Oracle.init_oracle_client()](https://cx-oracle.readthedocs.io/en/latest/api_manual/module.html#cx_Oracle.init_oracle_client)*.|**Default:**|
|**`oracle_client_config_dir`**|*config_dir* directory specified in a call to *[cx_Oracle.init_oracle_client()](https://cx-oracle.readthedocs.io/en/latest/api_manual/module.html#cx_Oracle.init_oracle_client)*.|**Default:**|

{==

*__Note:__ there are some configuration properties that are ignored when using Morph-KGC as a [library](https://morph-kgc.readthedocs.io/en/latest/documentation/#library), such as `output_file`.*

==}

### Data Sources

One data source section should be included in the **[INI file](https://en.wikipedia.org/wiki/INI_file)** for each data source to be materialized. The properties in the data source section vary depending on the data source type (relational database or data file). **Remote** mapping files are supported.

{==

*__Note:__ Morph-KGC is case sensitive regarding identifiers. This means that table, column and reference names in the mappings must be the same as those in the data sources (no matter if the mapping uses [delimited identifiers](https://www.w3.org/TR/r2rml/#dfn-sql-identifier)).*

==}

#### Relational Databases

The properties to be specified for **relational databases** are listed below. All of the properties are **required**.

|<div style="width:70px">Property</div>|Description|<div style="width:475px">Values</div>|
|-------|-------|-------|
|**`mappings`**|Specifies the mapping file(s) or URL(s) for the relational database.|**[REQUIRED]**<br>**Valid:**<br>- The path to a mapping file or URL.<br>- The paths to multiple mapping files or URLs separated by commas.<br>- The path to a directory containing all the mapping files.|
|**`db_url`**|It is a URL that configures the database engine (username, password, hostname, database name). See **[here](https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls)** how to create the database URLs.|**[REQUIRED]**<br>**Example:** _dialect+driver://username:password@host:port/db_name_|

Example **`db_url`** values (see **[here](https://docs.sqlalchemy.org/en/14/core/engines.html#database-urls)** all the information) for the **DBAPI drivers** recommended in **[Advanced Setup](https://morph-kgc.readthedocs.io/en/latest/documentation/#advanced-setup)** are:

- **[MySQL](https://www.mysql.com/):** _mysql+pymysql://username:password@host:port/db_name_
- **[PostgreSQL](https://www.postgresql.org/):** _postgresql+psycopg2://username:password@host:port/db_name_
- **[Oracle](https://www.oracle.com/database/):** _oracle+cx_oracle://username:password@host:port/db_name_
- **[Microsoft SQL Server](https://www.microsoft.com/sql-server):** _mssql+pymssql://username:password@host:port/db_name_
- **[MariaDB](https://mariadb.org/):** _mariadb+pymysql://username:password@host:port/db_name_
- **[SQLite](https://www.sqlite.org):** _sqlite:///db_path/db_name.db_

#### Data Files

The properties to be specified for **data files** are listed below. **Remote** data files are supported. The `mappings` property is **required**.

|<div style="width:70px">Property</div>|Description|<div style="width:475px">Values</div>|
|-------|-------|-------|
|**`mappings`**|Specifies the mapping file(s) or URL(s) for the data file.|**[REQUIRED]**<br>**Valid:**<br>- The path to a mapping file or URL.<br>- The paths to multiple mapping files or URLs separated by commas.<br>- The path to a directory containing all the mapping files.|
|**`file_path`**|Specifies the local path or URL of the data file. It is **optional** since it can be provided within the mapping file with _[rml:source](http://semweb.mmlab.be/ns/rml#source)_. If it is provided it will **override** the local path or URL provided in the mapping files.|**Default:**|

{==

*__Note:__ [CSV](https://en.wikipedia.org/wiki/Comma-separated_values), [TSV](https://en.wikipedia.org/wiki/Tab-separated_values), [Stata](https://www.stata.com/) and [SAS](https://www.sas.com) support compressed files (gzip, bz2, zip, xz, tar). Files are decompressed _on-the-fly_ and compression format is automatically inferred.*

==}

## Advanced Setup

### Relational Databases

The supported DBMSs are **[MySQL](https://www.mysql.com/)**, **[PostgreSQL](https://www.postgresql.org/)**, **[Oracle](https://www.oracle.com/database/)**, **[Microsoft SQL Server](https://www.microsoft.com/sql-server)**, **[MariaDB](https://mariadb.org/)** and **[SQLite](https://www.sqlite.org)**. To use relational databases it is neccessary to first **install the DBAPI driver**. We recommend the following ones:

- **[MySQL](https://www.mysql.com/):** [PyMySQL](https://pypi.org/project/PyMySQL/).
- **[PostgreSQL](https://www.postgresql.org/):** [psycopg2](https://pypi.org/project/psycopg2-binary/).
- **[Oracle](https://www.oracle.com/database/):** [cx-Oracle](https://pypi.org/project/cx-Oracle/).
- **[Microsoft SQL Server](https://www.microsoft.com/sql-server):** [pymssql](https://pypi.org/project/pymssql/).
- **[MariaDB](https://mariadb.org/):** [PyMySQL](https://pypi.org/project/PyMySQL/).
- **[SQLite](https://www.sqlite.org):** does **not** need any additional DBAPI driver.

Morph-KGC relies on **[SQLAlchemy](https://www.sqlalchemy.org/)**. Additional DBAPI drivers are supported, you can check the full list **[here](https://docs.sqlalchemy.org/en/14/dialects/index.html#included-dialects)**. For **[MySQL](https://www.mysql.com/)** and **[MariaDB](https://mariadb.org/)** you may also need to install **[cryptography](https://pypi.org/project/cryptography/)**.

{==

*__Note:__ to run Morph-KGC with [Oracle](https://www.oracle.com/database/), the libraries of the [Oracle Client](https://www.oracle.com/database/technologies/instant-client/downloads.html) need to be loaded. See [cx_Oracle Installation](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html) to install these libraries. See [cx_Oracle Initialization](https://cx-oracle.readthedocs.io/en/latest/user_guide/initialization.html) to setup the initialization of [Oracle](https://www.oracle.com/database/). Depending on the selected option, provide the properties `oracle_client_lib_dir` and `oracle_client_config_dir` in the [`CONFIGURATION`](https://morph-kgc.readthedocs.io/en/latest/documentation/#engine-configuration) section accordingly.*

==}

### Tabular Files

The supported tabular files formats are **[CSV](https://en.wikipedia.org/wiki/Comma-separated_values)**, **[TSV](https://en.wikipedia.org/wiki/Tab-separated_values)**, **[Excel](https://www.microsoft.com/en-us/microsoft-365/excel)**, **[Parquet](https://parquet.apache.org/documentation/latest/)**, **[Feather](https://arrow.apache.org/docs/python/feather.html)**, **[ORC](https://orc.apache.org/)**, **[Stata](https://www.stata.com/)**, **[SAS](https://www.sas.com)**, **[SPSS](https://www.ibm.com/analytics/spss-statistics-software)** and **[ODS](https://en.wikipedia.org/wiki/OpenDocument)**. To work with some of them it is neccessary to install some libraries:

- **[Excel](https://www.microsoft.com/en-us/microsoft-365/excel):** [OpenPyXL](https://pypi.org/project/openpyxl/).
- **[Parquet](https://parquet.apache.org/documentation/latest/):** [PyArrow](https://pypi.org/project/pyarrow/).
- **[ODS](https://en.wikipedia.org/wiki/OpenDocument):** [OdfPy](https://pypi.org/project/odfpy/).
- **[Feather](https://arrow.apache.org/docs/python/feather.html):** [PyArrow](https://pypi.org/project/pyarrow/).
- **[ORC](https://orc.apache.org/):** [PyArrow](https://pypi.org/project/pyarrow/).
- **[SPSS](https://www.ibm.com/analytics/spss-statistics-software):** [pyreadstat](https://pypi.org/project/pyreadstat/).

### Hierarchical Files

The supported hierarchical files formats are **[XML](https://www.w3.org/TR/xml/)** and **[JSON](https://www.json.org)**.

Morph-KGC uses **[XPath 3.0](https://www.w3.org/TR/xpath30/)** to query XML files and **[JSONPath](https://goessner.net/articles/JsonPath/)** to query JSON files.

{==

*__Note:__ the specific JSONPath syntax supported by Morph-KGC can be consulted __[here](https://github.com/zhangxianbing/jsonpath-python#jsonpath-syntax)__.*

==}

![OEG](assets/logo-oeg.png){ width="150" align=left } ![UPM](assets/logo-upm.png){ width="161" align=right }
