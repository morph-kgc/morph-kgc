## Examples

### Relational Databases
An example with _MySQL_ using [GTFS-Madrid-Bench](https://github.com/oeg-upm/gtfs-bench) data can be found in the [`rdb-example`](https://github.com/oeg-upm/morph-kgc/tree/main/examples/rdb-example) directory. The directory contains:
- The `mapping` file (in _Turtle_ format).
- The `configuration` file that has to be provided to Morph-KGC.

To start a docker container with the MySQL instance containing the data run the following:
```
docker run --name mysql-gtfs1 -p 3306:3306 -d -e MYSQL_ROOT_PASSWORD=gtfs oegdataintegration/mysql-gtfs1:1.0
```

Note that the MySQL driver needs to be installed, as detailed in the [Wiki](https://github.com/oeg-upm/Morph-KGC/wiki/Relational-Databases). You just need to run the following:
```
pip install pymysql
pip install cryptography
```
Also, **update the _paths_ parameters in the configuration file** accordingly.

### JSON and XML
Examples for _JSON_ and _XML_ can be found in [`json-example`](https://github.com/oeg-upm/morph-kgc/tree/main/examples/json-example) and [`xml-example`](https://github.com/oeg-upm/morph-kgc/tree/main/examples/xml-example) directories. The directories contain:
- The `data` file.
- The `mapping` file (in _Turtle_ format).
- The `configuration` file that has to be provided to Morph-KGC.
- The mapping file in [YARRRML](https://rml.io/yarrrml/spec/) format. This is not necessary, but allows to inspect the mapping in a human-readable format.

Note that the **_paths_ parameters in the configuration file need to be updated** accordingly.

### CSV
An example for _CSV_ can be found in the [`csv-example`](https://github.com/oeg-upm/morph-kgc/tree/main/examples/csv-example) directory. The directory contains:
- The `data` directory with several CSV files.
- The `mapping` file (in _Turtle_ format).
- The `configuration` file that has to be provided to Morph-KGC.

Note that the **_paths_ parameters in the configuration file need to be updated** accordingly. Given that this example involves multiple CSV files, the paths to these files are provided in the mapping file with the `rml:source` property. The values of this property have to be updated with the correct _paths_ to the CSV files in your system.

### Configuration Files
The directory [`configuration-file-examples`](https://github.com/oeg-upm/morph-kgc/tree/main/examples/configuration-file-examples) contains some configuration files to run Morph-KGC with. [default_config.ini](https://github.com/oeg-upm/Morph-KGC/blob/main/examples/configuration-file-examples/default_config.ini) contains all possible configuration options along with their default values. Options that are not provided in the `CONFIGURATION` section will use their default values. You can see all the information about configuration files in the [Wiki](https://github.com/oeg-upm/Morph-KGC/wiki/Configuration).
