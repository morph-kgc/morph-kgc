## Examples

### Tutorial

[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/drive/1ByFx_NOEfTZeaJ1Wtw3UwTH3H3-Sye2O?usp=sharing)

The tutorial in **[Google Colaboratory](https://colab.research.google.com/drive/1ByFx_NOEfTZeaJ1Wtw3UwTH3H3-Sye2O?usp=sharing)** is the easiest way to learn how to use Morph-KGC. Some relevant files for the tutorial can be found in the [`tutorial`](https://github.com/morph-kgc/morph-kgc/tree/main/examples/tutorial) directory.


### Relational Databases
An example with _MySQL_ using [GTFS-Madrid-Bench](https://github.com/oeg-upm/gtfs-bench) data can be found in the [`rdb`](https://github.com/morph-kgc/morph-kgc/tree/main/examples/rdb) directory. The directory contains:
- The `mapping` file in [YARRRML](https://rml.io/yarrrml/spec/) format.
- The `configuration` file that has to be provided to Morph-KGC.

To start a docker container with the MySQL instance containing the data, run the following:
```bash
docker run --name mysql-gtfs1 -p 3306:3306 -d -e MYSQL_ROOT_PASSWORD=gtfs oegdataintegration/mysql-gtfs1:1.0
```

Note that the MySQL driver needs to be installed. You just need to install Morph-KGC in the following way:
```
pip install morph-kgc[mysql]
```
Also, **update the _paths_ parameters in the configuration file** accordingly.

### JSON and XML
Examples for _JSON_ and _XML_ can be found in [`json`](https://github.com/morph-kgc/morph-kgc/tree/main/examples/json) and [`xml`](https://github.com/morph-kgc/morph-kgc/tree/main/examples/xml) directories. The directories contain:
- The `data` file in [YARRRML](https://rml.io/yarrrml/spec/) format.
- The `mapping` file.
- The `configuration` file that has to be provided to Morph-KGC.

Note that the **_paths_ parameters in the configuration file need to be updated** accordingly.

### CSV
An example for _CSV_ can be found in the [`csv`](https://github.com/morph-kgc/morph-kgc/tree/main/examples/csv) directory. The directory contains:
- The `data` directory with several CSV files.
- The `mapping` file in [YARRRML](https://rml.io/yarrrml/spec/) format.
- The `configuration` file that has to be provided to Morph-KGC.

Note that the **_paths_ parameters in the configuration file need to be updated** accordingly. Given that this example involves multiple CSV files, the paths to these files are provided in the mapping file with the `rml:source` property. The values of this property have to be updated with the correct _paths_ to the CSV files in your system.

### In-memory Data Structures
Examples for [Python Dictionaries](https://docs.python.org/3/tutorial/datastructures.html#dictionaries) and [DataFrames](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) can be found in [`dict`](https://github.com/morph-kgc/morph-kgc/tree/main/examples/dict) and [`dataframe`](https://github.com/morph-kgc/morph-kgc/tree/main/examples/dataframe) directories.

### HTTP APIs
An example reading JSON from an HTTP API can be found in the [`http-api`](https://github.com/morph-kgc/morph-kgc/tree/main/examples/http-api) directory. The request is described in the mapping with the [HTTP Vocabulary in RDF](https://www.w3.org/TR/HTTP-in-RDF10/) (`htv:`): where the API is, the headers it is sent and the query parameters it takes. The directory contains:
- The `mapping` file.
- The `configuration` file, with the `api_token` module the tokens are asked for.
- An `api_token.py` module handing out the token of the mapping, and caching it until it expires.
- A `run.py` script, a small `api.py` answering the requests so that the example needs no API of its own, and a `README` describing every option.

Header values written as `{ENV_VAR}`, or as the name of an environment variable, are read from the environment, so that tokens need not be written in the mapping and the `api_token` module is only needed for tokens that have to be asked for and refreshed.

### Reconciliation and Stateful Functions
Reconciliation maps values of the input data to the concepts they identify in a controlled vocabulary. The directory [`reconciliation`](https://github.com/morph-kgc/morph-kgc/tree/main/examples/reconciliation) reconciles against a SKOS vocabulary fetched from a URL, and [`reconciliation-sparql`](https://github.com/morph-kgc/morph-kgc/tree/main/examples/reconciliation-sparql) against the concepts held by a SPARQL endpoint. Both are _stateful_ functions: the vocabulary is fetched (or the endpoint queried) once, before any triple is materialized, and the resulting index is shared by every mapping rule and worker process.

The accessed resource is declared in the configuration file with a `[RESOURCE:<name>]` section, so that URLs and credentials stay out of the mapping. Each directory contains:
- The `data` file and the `vocabulary` the values are reconciled against.
- The `mapping` file.
- The `configuration` file, with the resource to reconcile against.
- A `run.py` script and a `README` describing every option.

The SPARQL example also ships a small `endpoint.py` answering the queries, so that it runs without a triplestore of its own.

[`stateful_udfs.py`](https://github.com/morph-kgc/morph-kgc/blob/main/examples/stateful_udfs.py) shows how to give a user-defined function a shared context of its own with the `@stateful_udf` decorator.


### Configuration Files
The directory [`configuration-file`](https://github.com/morph-kgc/morph-kgc/tree/main/examples/configuration-file) contains some configuration files to run Morph-KGC with. [`default_config.ini`](https://github.com/morph-kgc/morph-kgc/blob/main/examples/configuration-file/default_config.ini) contains all possible configuration options along with their default values. Options that are not provided in the `CONFIGURATION` section will use the default values. You can see all the information about configuration files in the **[documentation](https://morph-kgc.readthedocs.io/en/latest/documentation/#configuration)**.
