"""
Reconcile the disease labels of patients.csv against a SKOS vocabulary.

Run it from this directory:

    python run.py
"""

import os

import morph_kgc

os.chdir(os.path.dirname(os.path.realpath(__file__)))

graph = morph_kgc.materialize('config.ini')

print(graph.serialize(format='nt'))
