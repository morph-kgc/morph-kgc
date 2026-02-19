import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
import os

data = {
    'id': ['1', '2'],
    'name': ['Point A', 'Point B'],
    'geometry': [Point(1, 1), Point(2, 2)]
}

gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")
output_path = 'test/geoparquet/RMLTC0001a/data.parquet'
os.makedirs(os.path.dirname(output_path), exist_ok=True)
gdf.to_parquet(output_path)
print(f"Created {output_path}")
