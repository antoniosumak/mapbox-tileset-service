import geopandas as gpd
import pandas as pd
import sys
import os
import json
from datetime import datetime

def convert_plss_township(gdb_path, output_geojson):
    try:
        # Read the PLSSTownship table from the geodatabase
        gdf = gpd.read_file(gdb_path, layer='PLSSTownship')

        # Select only the required fields
        required_fields = ['TWNSHPNO', 'TWNSHPDIR', 'RANGENO', 'RANGEDIR', 'geometry']
        gdf = gdf[required_fields]

        # Ensure the data is in EPSG:4326 (WGS84)
        if gdf.crs is None:
            print("Warning: Input file has no CRS defined. Assuming EPSG:4326.")
            gdf.set_crs(epsg=4326, inplace=True)
        elif gdf.crs != "EPSG:4326":
            print(f"Converting from {gdf.crs} to EPSG:4326")
            gdf = gdf.to_crs(epsg=4326)

        # Build feature collection with polygons and centroids
        features = []
        for idx, row in gdf.iterrows():
            township_id = f"township-{idx + 1}"

            # Polygon feature
            polygon_feature = {
                "type": "Feature",
                "properties": {
                    "id": township_id,
                    "role": "polygon",
                    "TWNSHPNO": row['TWNSHPNO'],
                    "TWNSHPDIR": row['TWNSHPDIR'],
                    "RANGENO": row['RANGENO'],
                    "RANGEDIR": row['RANGEDIR']
                },
                "geometry": row.geometry.__geo_interface__
            }
            features.append(polygon_feature)

            # Centroid feature
            centroid = row.geometry.centroid
            centroid_feature = {
                "type": "Feature",
                "properties": {
                    "id": township_id,
                    "role": "centroid",
                    "TWNSHPNO": row['TWNSHPNO'],
                    "TWNSHPDIR": row['TWNSHPDIR'],
                    "RANGENO": row['RANGENO'],
                    "RANGEDIR": row['RANGEDIR']
                },
                "geometry": centroid.__geo_interface__
            }
            features.append(centroid_feature)

        # Create the FeatureCollection
        feature_collection = {
            "type": "FeatureCollection",
            "features": features
        }

        # Write to file
        with open(output_geojson, 'w') as f:
            json.dump(feature_collection, f)

        print(f"Successfully converted PLSSTownship to {output_geojson}")
        print(f"Number of polygons: {len(gdf)}")
        print(f"Total features (polygons + centroids): {len(features)}")
        
    except Exception as e:
        print(f"Error during conversion: {str(e)}")
        sys.exit(1)

def main():
    if len(sys.argv) != 2:
        print("Usage: python generate_township_geojson.py path_to_gdb_file")
        sys.exit(1)
    
    gdb_path = sys.argv[1]
    
    if not os.path.exists(gdb_path):
        print(f"Error: Input file {gdb_path} does not exist")
        sys.exit(1)
    
    # Generate output filename with timestamp
    output_geojson = "townships.geojson"
    
    convert_plss_township(gdb_path, output_geojson)

if __name__ == "__main__":
    main() 