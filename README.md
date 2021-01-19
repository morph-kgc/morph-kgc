# morph-kgc

## USE
```
positional arguments:
  config                Path to the configuration file.

optional arguments:
  -h, --help            show this help message and exit
  -g DEFAULT_GRAPH, --default_graph DEFAULT_GRAPH
                        Default graph to add triples to.
  -d OUTPUT_DIR, --output_dir OUTPUT_DIR
                        Path to the directory storing the results.
  -o OUTPUT_FILE, --output_file OUTPUT_FILE
                        If a file name is specified, all the results will be stored in this file. If no file is specified the results will be stored in multiple files.
  -f {ntriples,nquads}, --output_format {ntriples,nquads}
                        Output serialization format.
  -r {yes,no,on,off,true,false,0,1}, --remove_duplicates {yes,no,on,off,true,false,0,1}
                        Whether to remove duplicated triples in the results.
  -p [{,s,p,g,sp,sg,pg,spg}], --mapping_partitions [{,s,p,g,sp,sg,pg,spg}]
                        Partitioning criteria for mappings. s for using subjects and p for using predicates. If this parameter is not provided, no mapping partitions will be considered.
  --push_down_distincts {yes,no,on,off,true,false,0,1}
                        Whether to retrieve distinct results from data sources.
  --number_of_processes NUMBER_OF_PROCESSES
                        Number of parallel processes. 0 to set it to the number of CPUs in the system.
  --chunksize CHUNKSIZE
                        Maximum number of rows of data processed at once by a process.
  -l [LOGS], --logs [LOGS]
                        File path to write logs to. If no path is provided logs are redirected to stdout.
  -v, --version         show program's version number and exit
```
