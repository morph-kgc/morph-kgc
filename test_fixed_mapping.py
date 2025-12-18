#!/usr/bin/env python3
"""Test script with fixed YARRRML mapping."""

import sys
sys.path.insert(0, '/Users/ozzy/Projects/morph-kgc/src')

from morph_kgc import materialize

config_path = 'test/geoparquet/RMLTC0001b/config_fixed.ini'

print("=" * 80)
print("Testing GEOPARQUET with FIXED YARRRML mapping")
print("=" * 80)

try:
    print(f"\nConfig: {config_path}")
    print("\nAttempting materialization...\n")
    
    g = materialize(config_path)
    
    print(f"\nSuccess! Generated {len(g)} triples")
    print("\nGeometry triples:")
    for triple in g:
        if 'asWKT' in str(triple[1]):
            print(f"  Object: {triple[2]}")
        
except Exception as e:
    print(f"\nError: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
