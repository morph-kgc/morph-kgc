"""
Reconcile the disease labels of patients.csv against a SPARQL endpoint.

Run it from this directory:

    python run.py

The small SPARQL endpoint of endpoint.py is started first, so that the example
needs no triplestore of its own.
"""

import os

import morph_kgc

import endpoint

os.chdir(os.path.dirname(os.path.realpath(__file__)))

server = endpoint.start()
try:
    graph = morph_kgc.materialize('config.ini')
finally:
    server.shutdown()
    server.server_close()

print(graph.serialize(format='nt'))
