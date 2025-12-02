#!/usr/bin/env python3
"""Debug script to test GEOPARQUET source type detection."""

import sys
sys.path.insert(0, '/Users/ozzy/Projects/morph-kgc/src')

from morph_kgc import materialize

# Test with RMLTC0001b (YARRRML with rml:sourceType and rml:queryFormulation)
config_path = 'test/geoparquet/RMLTC0001b/config_simple.ini'

print("=" * 80)
print("Testing GEOPARQUET source type detection with YARRRML")
print("=" * 80)

try:
    print(f"\nConfig: {config_path}")
    print("\nAttempting materialization...")
    
    g = materialize(config_path)
    
    print(f"\nSuccess! Generated {len(g)} triples")
    print("\nSample triples:")
    for i, triple in enumerate(g):
        if i >= 20:
            break
        print(f"  {triple}")
        
except Exception as e:
    print(f"\nError: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 80)
