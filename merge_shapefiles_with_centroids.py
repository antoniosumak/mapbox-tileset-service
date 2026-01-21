import geopandas as gpd
import argparse
import os
import sys
import math
import numpy as np
from pathlib import Path


def sanitize_value(val):
    """Convert numpy/pandas types to JSON-serializable Python types."""
    import pandas as pd
    from datetime import datetime, date

    if val is None:
        return None
    if isinstance(val, (pd.Timestamp,)):
        return val.isoformat()
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        if math.isnan(val) or math.isinf(val):
            return None
        return float(val)
    if isinstance(val, np.ndarray):
        return val.tolist()
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
    if pd.isna(val):
        return None
    return val


def sanitize_properties(props: dict) -> dict:
    """Sanitize all property values for JSON serialization."""
    return {k: sanitize_value(v) for k, v in props.items()}


def find_shapefiles(input_folder: str) -> list[Path]:
    """Find all shapefiles in the given folder."""
    folder = Path(input_folder)
    if not folder.exists():
        print(f"Error: Folder '{input_folder}' does not exist.")
        sys.exit(1)

    shapefiles = list(folder.glob("*.shp"))
    if not shapefiles:
        print(f"Error: No shapefiles found in '{input_folder}'.")
        sys.exit(1)

    print(f"Found {len(shapefiles)} shapefile(s) in '{input_folder}'")
    return shapefiles


def convert_and_merge_shapefiles(shapefiles: list[Path]) -> gpd.GeoDataFrame:
    """Convert all shapefiles to GeoDataFrames and merge them."""
    gdfs = []

    for shp_path in shapefiles:
        print(f"  Reading: {shp_path.name}")
        try:
            gdf = gpd.read_file(shp_path)

            # Ensure the data is in EPSG:4326 (WGS84)
            if gdf.crs is None:
                print(f"    Warning: {shp_path.name} has no CRS defined. Assuming EPSG:4326.")
                gdf.set_crs(epsg=4326, inplace=True)
            elif gdf.crs != "EPSG:4326":
                print(f"    Converting from {gdf.crs} to EPSG:4326")
                gdf = gdf.to_crs(epsg=4326)

            gdfs.append(gdf)
        except Exception as e:
            print(f"    Error reading {shp_path.name}: {str(e)}")
            continue

    if not gdfs:
        print("Error: No shapefiles could be read successfully.")
        sys.exit(1)

    # Merge all GeoDataFrames
    print(f"Merging {len(gdfs)} GeoDataFrame(s)...")
    import pandas as pd
    merged_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs="EPSG:4326")

    return merged_gdf


def create_features_with_centroids(merged_gdf: gpd.GeoDataFrame) -> dict:
    """
    Create a FeatureCollection with each polygon/multipolygon paired with its centroid.
    """
    features = []

    for idx, row in merged_gdf.iterrows():
        area_id = f"map-area-{idx + 1}"
        geometry = row.geometry

        # Get properties from the original feature (excluding geometry) and sanitize for JSON
        original_props = sanitize_properties({k: v for k, v in row.items() if k != 'geometry'})

        # Create polygon feature
        polygon_feature = {
            "type": "Feature",
            "properties": {
                "id": area_id,
                "role": "polygon",
                **original_props
            },
            "geometry": geometry.__geo_interface__
        }
        features.append(polygon_feature)

        # Create centroid feature
        centroid = geometry.centroid
        centroid_feature = {
            "type": "Feature",
            "properties": {
                "id": area_id,
                "role": "centroid",
                **original_props
            },
            "geometry": {
                "type": "Point",
                "coordinates": [centroid.x, centroid.y]
            }
        }
        features.append(centroid_feature)

    return {
        "type": "FeatureCollection",
        "features": features
    }


def main():
    parser = argparse.ArgumentParser(
        description="Merge shapefiles and add centroids for each polygon/multipolygon"
    )
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Input folder containing shapefiles"
    )
    parser.add_argument(
        "-o", "--output",
        default="merged_with_centroids.geojson",
        help="Output GeoJSON file path (default: merged_with_centroids.geojson)"
    )
    args = parser.parse_args()

    # Find all shapefiles
    shapefiles = find_shapefiles(args.input)

    # Convert and merge
    merged_gdf = convert_and_merge_shapefiles(shapefiles)
    print(f"Merged GeoDataFrame has {len(merged_gdf)} feature(s)")

    # Create features with centroids
    print("Creating polygon and centroid feature pairs...")
    geojson = create_features_with_centroids(merged_gdf)
    print(f"Created {len(geojson['features'])} features ({len(merged_gdf)} polygons + {len(merged_gdf)} centroids)")

    # Write output
    import json
    print(f"Writing to {args.output}...")
    with open(args.output, 'w') as f:
        json.dump(geojson, f)

    print("Done!")


if __name__ == "__main__":
    main()
