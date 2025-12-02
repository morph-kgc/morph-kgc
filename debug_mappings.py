#!/usr/bin/env python3
"""Debug script to inspect parsed mappings."""

import sys
sys.path.insert(0, '/Users/ozzy/Projects/morph-kgc/src')

from morph_kgc import materialize_oxigraph
import pandas as pd

# Temporarily patch to capture the dataframe
original_get_file_data = None
captured_source_types = []

def patch_get_file_data():
    from morph_kgc.data_source import data_file
    global original_get_file_data
    original_get_file_data = data_file.get_file_data
    
    def wrapped_get_file_data(rml_rule, references):
        source_type = rml_rule.get('source_type', 'UNKNOWN')
        captured_source_types.append({
            'source_type': source_type,
            'logical_source_value': rml_rule.get('logical_source_value', 'N/A'),
            'references': list(references)
        })
        return original_get_file_data(rml_rule, references)
    
    data_file.get_file_data = wrapped_get_file_data

config_path = 'test/geoparquet/RMLTC0001b/config_geoparquet.ini'

print("=" * 80)
print("Inspecting source types during materialization")
print("=" * 80)

patch_get_file_data()

try:
    g = materialize_oxigraph(config_path)
    
    print(f"\nCaptured {len(captured_source_types)} data source reads:")
    for i, info in enumerate(captured_source_types):
        print(f"\n  Read {i+1}:")
        print(f"    source_type: {info['source_type']}")
        print(f"    file: {info['logical_source_value']}")
        print(f"    columns: {info['references']}")
    
except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
