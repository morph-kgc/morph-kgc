# Documentation

## Tutorial

You can get quickly started with the tutorial in **[`Google Colaboratory`](https://colab.research.google.com/drive/1ByFx_NOEfTZeaJ1Wtw3UwTH3H3-Sye2O?usp=sharing)**.

## Installation

In the following we describe different ways in which you can install and use Morph-KGC.

### PyPi

**[PyPi](https://pypi.org/project/morph-kgc/)** is the fastest way to install Morph-KGC:
```bash
pip install morph-kgc
```

Some data sources require additional dependencies. Check **[Advanced Setup](https://morph-kgc.readthedocs.io/en/latest/documentation/#advanced-setup)** for specific installation instructions or install all the dependencies:

```bash
pip install morph-kgc[all]
```

We recommend to use **[virtual environments](https://docs.python.org/3/library/venv.html#)** to install Morph-KGC.

### From Source

You can also grab the latest source code from the **[GitHub repository](https://github.com/morph-kgc/morph-kgc)**:
```bash
pip install git+https://github.com/morph-kgc/morph-kgc.git
```

## Usage

Morph-KGC uses an **[INI file](https://en.wikipedia.org/wiki/INI_file)** to configure the materialization process, see **[Configuration](https://morph-kgc.readthedocs.io/en/latest/documentation/#configuration)**.

### Command Line

To run the engine using the **command line** you just need to execute the following:

```bash
python3 -m morph_kgc path/to/config.ini
```

### Library

Morph-KGC can be used as a **library**, providing different methods to materialize the **[RDF](https://www.w3.org/TR/rdf11-concepts/)** or **[RDF-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html)** knowledge graph. It integrates with **[RDFLib](https://rdflib.readthedocs.io/en/stable/)** and **[Oxigraph](https://pyoxigraph.readthedocs.io/en/latest/)** to easily create and work with knowledge graphs in **[Python](https://www.python.org/)**.

The methods in the **API** accept the **config as a string or as the path to an INI file**.

```Python
import morph_kgc

config = """
            [DataSource1]
            mappings: /path/to/mapping/mapping_file.rml.ttl
            db_url: mysql+pymysql://user:password@localhost:3306/db_name
         """
```

#### [RDFLib](https://rdflib.readthedocs.io/en/stable/)

**`morph_kgc.materialize(config)`**

Materialize the knowledge graph to **[RDFLib](https://rdflib.readthedocs.io/en/stable/)**.

```Python
# generate the triples and load them to an RDFLib graph

graph = morph_kgc.materialize(config)
# or
graph = morph_kgc.materialize('/path/to/config.ini')

# work with the RDFLib graph
q_res = graph.query(' SELECT DISTINCT ?classes WHERE { ?s a ?classes } ')
```

{==

*__Note:__ [RDFLib](https://rdflib.readthedocs.io/en/stable/) does not support [RDF-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html), hence `materialize` does not support [RML-star](https://w3id.org/rml/star/spec).*

==}

#### [Oxigraph](https://pyoxigraph.readthedocs.io/en/latest/)

**`morph_kgc.materialize_oxigraph(config)`**

Materialize the knowledge graph to **[Oxigraph](https://pyoxigraph.readthedocs.io/en/latest/)**.

```Python
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

```Python
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

Below is an example configuration file with one input relational source. In this case `DataSource1` is the only data source section, but other data sources can be considered by including additional sections. **[Here](https://github.com/morph-kgc/morph-kgc/blob/main/examples/configuration-file/default_config.ini)** you can find a configuration file which is more complete.

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

| <div style="width:195px">Property</div> | Description                                                                                                                                                                                                                                  | Values                                                                                                                                                                  |
|-----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **`output_file`**                       | File to write the resulting knowledge graph to.                                                                                                                                                                                              | **Default:** _knowledge-graph.nt_                                                                                                                                       |
| **`output_dir`**                        | Directory to write the resulting knowledge graph to. If it is specified, `output_file` will be ignored and multiple output files will generated, one for each mapping partition.                                                             | **Default:**                                                                                                                                                            |
| **`na_values`**                         | Set of values to be interpreted as _NULL_ when retrieving data from the input sources. The set of values must be separated by commas.                                                                                                        | **Default:** ,_nan_                                                                                                                                                     |
| **`output_format`**                     | RDF serialization to use for the resulting knowledge graph.                                                                                                                                                                                  | **Valid:** _[N-TRIPLES](https://www.w3.org/TR/n-triples/)_, _[N-QUADS](https://www.w3.org/TR/n-quads/)_<br>**Default:** _[N-TRIPLES](https://www.w3.org/TR/n-triples/)_ |
| **`only_printable_chars`**              | Remove characters in the genarated RDF that are not printable.                                                                                                                                                                               | **Valid:** _yes_, _no_, _true_, _false_, _on_, _off_, _1_, _0_<br>**Default:** _no_                                                                                     |
| **`safe_percent_encoding`**             | Set of ASCII characters that should not be percent encoded. All characters are encoded by default.                                                                                                                                           | **Example:** _:/_<br>**Default:**                                                                                                                                       |
| **`udfs`**                              | File with Python user-defined functions to be called from _[RML-FNML](https://w3id.org/rml/fnml/spec)_.                                                                                                                            | **Default:**                                                                                                                                                            |
| **`mapping_partitioning`**              | [Mapping partitioning](https://content.iospress.com/download/semantic-web/sw223135?id=semantic-web%2Fsw223135) algorithm to use. Mapping partitioning can also be disabled.                                                                  | **Valid:** _PARTIAL-AGGREGATIONS_, _MAXIMAL_, _no_, _false_, _off_, _0_<br>**Default:** _PARTIAL-AGGREGATIONS_                                                          |
| **`infer_sql_datatypes`**               | Infer datatypes for relational databases. If a [datatypeable term map](https://www.w3.org/TR/r2rml/#dfn-datatypeable-term-map) has a _[rr:datatype](https://www.w3.org/ns/r2rml#datatype)_ property, then the datatype will not be inferred. | **Valid:** _yes_, _no_, _true_, _false_, _on_, _off_, _1_, _0_<br>**Default:** _no_                                                                                     |
| **`number_of_processes`**               | The number of processes to use. If _1_, Morph-KGC will use sequential processing (minimizing memory consumption), otherwise parallel processing is used (minimizing execution time).                                                         | **Default:** _2 * number of CPUs in the system_                                                                                                                         |
| **`logging_level`**                     | Sets the [level](https://docs.python.org/3/library/logging.html#logging-levels) of the log messages to show.                                                                                                                                 | **Valid:** _DEBUG_, _INFO_, _WARNING_, _ERROR_, _CRITICAL_, _NOTSET_<br>**Default:** _INFO_                                                                             |
| **`logging_file`**                      | If not provided, log messages will be redirected to _stdout_. If a file path is provided, log messages will be written to the file.                                                                                                          | **Default:**                                                                                                                                                            |
| **`oracle_client_lib_dir`**             | *lib_dir* directory specified in a call to *[cx_Oracle.init_oracle_client()](https://cx-oracle.readthedocs.io/en/latest/api_manual/module.html#cx_Oracle.init_oracle_client)*.                                                               | **Default:**                                                                                                                                                            |
| **`oracle_client_config_dir`**          | *config_dir* directory specified in a call to *[cx_Oracle.init_oracle_client()](https://cx-oracle.readthedocs.io/en/latest/api_manual/module.html#cx_Oracle.init_oracle_client)*.                                                            | **Default:**                                                                                                                                                            |

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

|<div style="width:100px">Property</div>|Description|<div style="width:475px">Values</div>|
|-------|-------|-------|
|**`mappings`**|Specifies the mapping file(s) or URL(s) for the relational database.|**[REQUIRED]**<br>**Valid:**<br>- The path to a mapping file or URL.<br>- The paths to multiple mapping files or URLs separated by commas.<br>- The path to a directory containing all the mapping files.|
|**`db_url`**|It is a URL that configures the database engine (username, password, hostname, database name). See **[here](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls)** how to create the database URLs.|**[REQUIRED]**<br>**Example:** _dialect+driver://username:password@host:port/db_name_|
|**`connect_args`**|A dictionary string of options for [SQLAlchemy](https://www.sqlalchemy.org/). See [here](https://docs.sqlalchemy.org/en/20/core/engines.html#sqlalchemy.create_engine.params.connect_args) the [SQLAlchemy](https://www.sqlalchemy.org/) documentation.|**Example:** _{"http_path": "<cluster_http_path>"}_|

Example **`db_url`** values (see **[here](https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls)** all the information):

- **[MySQL](https://www.mysql.com/)**: _mysql+pymysql://username:password@host:port/db_name_
- **[PostgreSQL](https://www.postgresql.org/)**: _postgresql+psycopg://username:password@host:port/db_name_
- **[Oracle](https://www.oracle.com/database/)**: _oracle+cx_oracle://username:password@host:port/db_name_
- **[Microsoft SQL Server](https://www.microsoft.com/sql-server)**: _mssql+pymssql://username:password@host:port/db_name_
- **[MariaDB](https://mariadb.org/)**: _mariadb+pymysql://username:password@host:port/db_name_
- **[SQLite](https://www.sqlite.org)**: _sqlite:///db_path/db_name.db_

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

The supported DBMSs are **[MySQL](https://www.mysql.com/)**, **[PostgreSQL](https://www.postgresql.org/)**, **[Oracle](https://www.oracle.com/database/)**, **[Microsoft SQL Server](https://www.microsoft.com/sql-server)**, **[MariaDB](https://mariadb.org/)** and **[SQLite](https://www.sqlite.org)**. To use relational databases it is necessary to additionally install **DBAPI drivers**. You can install them via:

- **[MySQL](https://www.mysql.com/)** and **[MariaDB](https://mariadb.org/)**: `pip install morph-kgc[mysql]`.
- **[PostgreSQL](https://www.postgresql.org/)**: `pip install morph-kgc[postgresql]`.
- **[Microsoft SQL Server](https://www.microsoft.com/sql-server)**: `pip install morph-kgc[mssql]`.
- **[Oracle](https://www.oracle.com/database/)**: `pip install morph-kgc[oracle]`.

To run Morph-KGC with [Oracle](https://www.oracle.com/database/), the libraries of the [Oracle Client](https://www.oracle.com/database/technologies/instant-client/downloads.html) need to be loaded. See [cx_Oracle Installation](https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html) to install these libraries. See [cx_Oracle Initialization](https://cx-oracle.readthedocs.io/en/latest/user_guide/initialization.html) to setup the initialization of [Oracle](https://www.oracle.com/database/). Depending on the selected option, provide the properties `oracle_client_lib_dir` and `oracle_client_config_dir` in the [`CONFIGURATION`](https://morph-kgc.readthedocs.io/en/latest/documentation/#engine-configuration) section accordingly.

### Tabular Files

The supported tabular files formats are **[CSV](https://en.wikipedia.org/wiki/Comma-separated_values)**, **[TSV](https://en.wikipedia.org/wiki/Tab-separated_values)**, **[Excel](https://www.microsoft.com/en-us/microsoft-365/excel)**, **[Parquet](https://parquet.apache.org/documentation/latest/)**, **[Feather](https://arrow.apache.org/docs/python/feather.html)**, **[ORC](https://orc.apache.org/)**, **[Stata](https://www.stata.com/)**, **[SAS](https://www.sas.com)**, **[SPSS](https://www.ibm.com/analytics/spss-statistics-software)** and **[ODS](https://en.wikipedia.org/wiki/OpenDocument)**. To work with some of them it is necessary to install additional dependencies. You can install them via:

- **[Excel](https://www.microsoft.com/en-us/microsoft-365/excel)** and **[ODS](https://en.wikipedia.org/wiki/OpenDocument)**: `pip install morph-kgc[excel]`.
- **[Parquet](https://parquet.apache.org/documentation/latest/)**, **[Feather](https://arrow.apache.org/docs/python/feather.html)** and **[ORC](https://orc.apache.org/)**: `pip install morph-kgc[tabular]`.
- **[SPSS](https://www.ibm.com/analytics/spss-statistics-software)**: `pip install morph-kgc[spss]`.

### Hierarchical Files

The supported hierarchical files formats are **[XML](https://www.w3.org/TR/xml/)** and **[JSON](https://www.json.org)**.

Morph-KGC uses **[XPath 3.0](https://www.w3.org/TR/xpath30/)** to query XML files and **[JSONPath](https://goessner.net/articles/JsonPath/)** to query JSON files. The specific JSONPath syntax supported by Morph-KGC can be consulted __[here](https://github.com/zhangxianbing/jsonpath-python#jsonpath-syntax)__.

## Mappings

Morph-KGC is compliant with the W3C Recommendation **[RDB to RDF Mapping Language (R2RML)](https://www.w3.org/TR/r2rml/)** and the **[RDF Mapping Language (RML)](https://w3id.org/rml/core/spec)**. You can refer to their associated specifications to consult the syntaxes.

### RML-FNML

Declarative **transformation functions** are supported via **[RML-FNML](https://w3id.org/rml/fnml/spec)**. Morph-KGC comes with a subset of the **[GREL functions](http://users.ugent.be/~bjdmeest/function/grel.ttl#)** as **built-in functions** that can be directly used from the mappings. Python **user-defined functions** are additionally supported. A Python script with **user-defined functions** is provided to Morph-KGC via the `udfs` parameter. Decorators for these functions must be defined to link the **Python** parameters to the **FNML** parameters. An example of a **user-defined function**:

```Python
@udf(
    fun_id='http://example.com/toUpperCase',
    text='http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam')
def to_upper_case(text):
    return text.upper()
```

An **[RML-FNML](https://w3id.org/rml/fnml/spec)** mapping calling this functions would be:

```ttl
<#TM1>
    rml:logicalSource [
        rml:source "test/rml-fnml/udf/student.csv";
        rml:referenceFormulation ql:CSV;
    ];
    rr:subjectMap [
        rr:template "http://example.com/{Name}";
    ];
    rr:predicateObjectMap [
        rr:predicate foaf:name;
        rr:objectMap [
            fnml:execution <#Execution>;
        ];
    ].

<#Execution>
    fnml:function ex:toUpperCase;
    fnml:input [
        fnml:parameter grel:valueParam;
        fnml:valueMap [
            rml:reference "Name";
        ];
    ].
```

The complete set of **built-in functions** can be consulted [here](https://github.com/morph-kgc/morph-kgc/blob/main/src/morph_kgc/fnml/built_in_functions.py).

### RML-star

Morph-KGC supports the new **[RML-star](https://w3id.org/rml/star/spec)** mapping language to generate **[RDF-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html)** knowledge graphs. **[RML-star](https://w3id.org/rml/star/spec)** introduces the **star map** class to generate **[RDF-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html)** triples. A star map can be either at the place of a subject map or an object map, generating **quoted triples** in either the subject or object positions. The _rml:embeddedTriplesMap_ property connects the star maps to the triples map that defines how the quoted triples will be generated. Triples map can be declared as _rml:NonAssertedTriplesMap_ if they are to be referenced from an embedded triples map, but are not supposed to generate asserted triples in the output **[RDF-star](https://w3c.github.io/rdf-star/cg-spec/2021-12-17.html)** graph. The following example from the **[RML-star specification](https://w3id.org/rml/star/spec)** uses a non-asserted triples map to generate quoted triples.

```ttl
<#TM1> a rml:NonAssertedTriplesMap;
    rml:logicalSource ex:ConfidenceSource;
    rml:subjectMap [
        rr:template "http://example.com/{entity}";
    ];
    rr:predicateObjectMap [
        rr:predicate rdf:type;
        rml:objectMap [
            rr:template "http://example.com/{class}";
        ];
    ].

<#TM2> a rr:TriplesMap;
    rml:logicalSource ex:ConfidenceSource;
    rml:subjectMap [
        rml:quotedTriplesMap <#TM1>;
    ];
    rr:predicateObjectMap [
        rr:predicate ex:confidence;
        rml:objectMap [
            rml:reference "confidence";
        ];
    ].
```

### YARRRML

**[YARRRML](https://rml.io/yarrrml/spec/)** is a human-friendly serialization of [RML](https://w3id.org/rml/core/spec) that uses [YAML](https://yaml.org/). Morph-KGC supports [YARRRML](https://rml.io/yarrrml/spec/), also for [RML-FNML](https://w3id.org/rml/fnml/spec) and [RML-star](https://w3id.org/rml/star). The mapping below shows a [YARRRML](https://rml.io/yarrrml/spec/) example.

```yml
prefixes:
  foaf: http://xmlns.com/foaf/0.1/
  ex: http://example.com/
  rdf: http://www.w3.org/1999/02/22-rdf-syntax-ns#
  xsd: http://www.w3.org/2001/XMLSchema#

mappings:
  TM1:
   sources:
     - ['student.csv~csv']
   s: http://example.com/$(Name)
   po:
     - [foaf:name, $(Name)]
```

### RML Views

In addition to **[R2RML views](https://www.w3.org/TR/r2rml/#r2rml-views)**, Morph-KGC also supports **RML views** over tabular data (**[CSV](https://en.wikipedia.org/wiki/Comma-separated_values)** and **[Parquet](https://parquet.apache.org/documentation/latest/)** formats) and **[JSON](https://www.json.org)** files. RML views enable transformation functions, complex joins or mixed content using the **[SQL](https://duckdb.org/docs/sql/introduction)** query language. For instance, the following triples map takes as input a **[CSV](https://en.wikipedia.org/wiki/Comma-separated_values)** file and filters the data based on the language of some codes.

```ttl
<#TM1>
    rml:logicalSource [
        rml:query """
            SELECT "Code", "Name", "Lan"
            FROM 'country.csv'
            WHERE "Lan" = 'EN';
        """
    ];
    rr:subjectMap [
        rr:template "http://example.com/{Code}";
    ];
    rr:predicateObjectMap [
        rr:predicate rdfs:label;
        rr:objectMap [
            rr:column "Name";
            rr:language "en";
        ];
    ].
```

Morph-KGC uses **[DuckDB](duckdb.org/)** to evaluate queries over tabular sources, the supported **[SQL](https://duckdb.org/docs/sql/introduction)** syntax can be consulted in its [documentation](https://duckdb.org/docs/sql/introduction). For views over **[JSON](https://www.json.org)** check the corresponding [JSON section in the DuckDB documentation](https://duckdb.org/docs/extensions/json.html) and [this blog post](https://duckdb.org/2023/03/03/json.html).

### RML In-Memory

Morph-KGC supports the definition of in-memory logical sources (**[Pandas DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html)** and **[Python Dictionaries](https://docs.python.org/3/tutorial/datastructures.html#dictionaries)**) within RML using the **[SD Ontology](https://knowledgecaptureanddiscovery.github.io/SoftwareDescriptionOntology/release/1.8.0/index-en.html)**. The following **[RML](https://w3id.org/rml/core/spec)** rules show the transformation of a **[Pandas Dataframe](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html)** to **[RDF](https://www.w3.org/TR/rdf11-concepts/)**.

```ttl
@prefix sd: <https://w3id.org/okn/o/sd#>.

<#TM1>
    rml:logicalSource [
        rml:source [
            a sd:DatasetSpecification;
            sd:name "variable1";
            sd:hasDataTransformation [
                sd:hasSoftwareRequirements "pandas>=1.1.0";
                sd:hasSourceCode [
                    sd:programmingLanguage "Python3.9";
                ];
            ];   
        ];
        rml:referenceFormulation ql:DataFrame;
    ];
    rr:subjectMap [
        rr:template "http://example.com/data/user{Id}";
    ];
    rr:predicateObjectMap [
        rr:predicate rdf:type;
        rr:objectMap [
            rr:constant ex:User;
        ];
    ].
```

The above mappings can be executed from Python as follows:
```Python
import morph_kgc
import pandas as pd

users_df = pd.DataFrame({'Id': [1,2,3,4],\
           'Username': ["@jude","@emily","@wayne","@jordan1"]})
data_dict = {"variable1": users_df}

config = """
    [DataSource]
    mappings = mapping_rml.ttl
"""

g_rdflib = morph_kgc.materialize(config, data_dict)
```

![OEG](assets/logo-oeg.png){ width="150" align=left } ![UPM](assets/logo-upm.png){ width="161" align=right }
