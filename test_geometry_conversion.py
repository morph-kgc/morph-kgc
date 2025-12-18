#!/usr/bin/env python3
"""Debug script to check geometry conversion."""

import sys
sys.path.insert(0, '/Users/ozzy/Projects/morph-kgc/src')

import geopandas as gpd
import pandas as pd

file_path = 'test/geoparquet/RMLTC0001b/ENVIRO_AUDIT_POINT_sample.parquet'

print("=" * 80)
print("Testing geometry conversion")
print("=" * 80)

# Read the file
gdf = gpd.read_parquet(file_path, columns=['REFERENCE_', 'geometry'])

print(f"\nOriginal GeoDataFrame:")
print(f"Type: {type(gdf)}")
print(f"Geometry column name: {gdf.geometry.name}")
print(f"Geometry column type: {type(gdf.geometry)}")
print(f"First geometry value type: {type(gdf.geometry.iloc[0])}")
print(f"First geometry value: {gdf.geometry.iloc[0]}")

# Convert to WKT
print(f"\nConverting to WKT...")
gdf[gdf.geometry.name] = gdf.geometry.to_wkt()

print(f"\nAfter conversion:")
print(f"Geometry column type: {type(gdf[gdf.geometry.name])}")
print(f"First geometry value type: {type(gdf[gdf.geometry.name].iloc[0])}")
print(f"First geometry value: {gdf[gdf.geometry.name].iloc[0]}")

# Convert to DataFrame
df = pd.DataFrame(gdf)
print(f"\nAfter converting to DataFrame:")
print(f"Type: {type(df)}")
print(f"Geometry column type: {type(df['geometry'])}")
print(f"First geometry value type: {type(df['geometry'].iloc[0])}")
print(f"First geometry value: {df['geometry'].iloc[0]}")

print("\n" + "=" * 80)
