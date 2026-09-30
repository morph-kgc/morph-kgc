"""
Materialize the observations an HTTP API answers with.

Run it from this directory:

    python run.py

The small API of api.py is started first, so that the example needs no API of
its own. Its request log shows the headers and query parameters Morph-KGC sent.
"""

import os

import morph_kgc

import api

os.chdir(os.path.dirname(os.path.realpath(__file__)))

server = api.start()
try:
    graph = morph_kgc.materialize('config.ini')
finally:
    server.shutdown()
    server.server_close()

print(graph.serialize(format='nt'))
