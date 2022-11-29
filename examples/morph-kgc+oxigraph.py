#!/usr/bin/env python

import os
import sys
import argparse
import morph_kgc
from time import time

EXIT_CODE_FILE_NOT_EXIST = -1
EXIT_CODE_NO_RESULTS = -2
VERSION = '0.0.1'


def execute(config_file, query_file, results_file):
    # run Morph-KGC
    start_time = time()
    g = morph_kgc.materialize_oxigraph(config_file)

    # execute query on materialized graph
    with open(query_file, 'r') as f:
        query = f.read()
    results = g.query(query)

    if not results:
        print('No results!', file=sys.stderr)
        sys.exit(EXIT_CODE_NO_RESULTS)

    # write result set
    with open(results_file, 'w') as f:
        for r in results:
            f.write(f'{r}\n')

    end_time = time()
    print(f'Finished in {round(end_time - start_time, 2)}s!')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog = 'Morph-KGC+Oxigraph',
                                     description = 'Query heterogeneous data  '
                                                   'with Morph-KGC and Oxigraph',
                                     epilog='Please cite our paper if you use '
                                            'this. Thanks!')
    parser.add_argument('config_file', help='Path to Morph-KGC\'s config file')
    parser.add_argument('query_file', help='Path to the query file to execute')
    parser.add_argument('results_file',
                        help='Path to the file to store results')
    parser.add_argument('--version', action='version',
                        version = f'Morph-KGC+Oxigraph v{VERSION}')
    args = parser.parse_args()

    config_file = os.path.abspath(args.config_file)
    query_file = os.path.abspath(args.query_file)
    results_file = os.path.abspath(args.results_file)

    print(f'Morph-KGC+Oxigraph v{VERSION}')
    print('Arguments:')
    print(f'Config file: {config_file}')
    print(f'Query file: {query_file}')
    print(f'Results file: {results_file}')

    if not os.path.exists(config_file):
        print(f'Config file does not exist: {config_file}', file=sys.stderr)
        sys.exit(EXIT_CODE_FILE_NOT_EXIST)

    if not os.path.exists(query_file):
        print(f'Query file does not exist: {query_file}', file=sys.stderr)
        sys.exit(EXIT_CODE_FILE_NOT_EXIST)

    execute(config_file, query_file, results_file)
