#!/usr/bin/env python3
"""Debug script to test GEOPARQUET source type detection with logging."""

import sys
sys.path.insert(0, '/Users/ozzy/Projects/morph-kgc/src')

import logging
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s | %(name)s | %(message)s')

from morph_kgc import materialize

# Test with RMLTC0001b (YARRRML with rml:sourceType and rml:queryFormulation)
config_path = 'test/geoparquet/RMLTC0001b/config_simple.ini'

print("=" * 80)
print("Testing GEOPARQUET source type detection with YARRRML (DEBUG MODE)")
print("=" * 80)

try:
    print(f"\nConfig: {config_path}")
    print("\nAttempting materialization...\n")
    
    g = materialize(config_path)
    
    print(f"\n\nSuccess! Generated {len(g)} triples")
    print("\nSample geometry triples:")
    for triple in g:
        if 'asWKT' in str(triple[1]):
            print(f"  {triple}")
        
except Exception as e:
    print(f"\nError: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
