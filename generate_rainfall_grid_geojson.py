import json
import argparse

# Grid parameters matching the TypeScript calcLatLong function
LATITUDE_ORIGIN = 20.125
LONGITUDE_ORIGIN = -129.875
GRID_STEP = 0.25
MAX_LONGITUDE_STEPS = 300

# Calculate latitude rows needed to cover continental USA (up to ~50°N)
MAX_LATITUDE = 50.125
MAX_LATITUDE_STEPS = int((MAX_LATITUDE - LATITUDE_ORIGIN) / GRID_STEP)


def calc_lat_long(grid_id: int) -> tuple[float, float]:
    """
    Convert a grid number (1-indexed) into the lat/long of that grid's origin (bottom-left corner).
    This mirrors the TypeScript calcLatLong function.
    """
    zero_index_grid_id = grid_id - 1
    y_index = zero_index_grid_id // MAX_LONGITUDE_STEPS
    x_index = zero_index_grid_id % MAX_LONGITUDE_STEPS

    longitude = LONGITUDE_ORIGIN + x_index * GRID_STEP
    latitude = LATITUDE_ORIGIN + y_index * GRID_STEP

    return longitude, latitude


def create_outline_feature(grid_id: int, lon: float, lat: float) -> dict:
    """Create an outline (LineString) feature for a grid cell."""
    # Create line coordinates forming a closed rectangle
    coordinates = [
        [lon, lat],                           # bottom-left
        [lon + GRID_STEP, lat],               # bottom-right
        [lon + GRID_STEP, lat + GRID_STEP],   # top-right
        [lon, lat + GRID_STEP],               # top-left
        [lon, lat]                            # close the outline
    ]

    return {
        "type": "Feature",
        "properties": {
            "id": f"grid-{grid_id}",
            "role": "outline",
            "gridId": grid_id
        },
        "geometry": {
            "type": "LineString",
            "coordinates": coordinates
        }
    }


def create_centroid_feature(grid_id: int, lon: float, lat: float) -> dict:
    """Create a centroid point feature for a grid cell."""
    # Calculate centroid (center of the grid cell)
    centroid_lon = lon + GRID_STEP / 2
    centroid_lat = lat + GRID_STEP / 2

    return {
        "type": "Feature",
        "properties": {
            "id": f"grid-{grid_id}",
            "role": "centroid",
            "gridId": grid_id
        },
        "geometry": {
            "type": "Point",
            "coordinates": [centroid_lon, centroid_lat]
        }
    }


def generate_rainfall_grid_geojson(output_file: str = "rainfall_grid.geojson"):
    """Generate the complete rainfall grid GeoJSON covering the USA."""
    features = []

    total_grids = MAX_LONGITUDE_STEPS * MAX_LATITUDE_STEPS
    print(f"Generating {total_grids} grid cells ({MAX_LONGITUDE_STEPS} x {MAX_LATITUDE_STEPS})...")
    print(f"Longitude range: {LONGITUDE_ORIGIN} to {LONGITUDE_ORIGIN + (MAX_LONGITUDE_STEPS - 1) * GRID_STEP}")
    print(f"Latitude range: {LATITUDE_ORIGIN} to {LATITUDE_ORIGIN + (MAX_LATITUDE_STEPS - 1) * GRID_STEP}")

    for grid_id in range(1, total_grids + 1):
        lon, lat = calc_lat_long(grid_id)

        # Create outline and centroid features for this grid cell
        outline_feature = create_outline_feature(grid_id, lon, lat)
        centroid_feature = create_centroid_feature(grid_id, lon, lat)

        features.append(outline_feature)
        features.append(centroid_feature)

        if grid_id % 10000 == 0:
            print(f"  Processed {grid_id}/{total_grids} grid cells...")

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    print(f"Writing {len(features)} features to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(geojson, f)

    print("Done!")
    return geojson


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate rainfall grid GeoJSON for USA")
    parser.add_argument(
        "-o", "--output",
        default="rainfall_grid.geojson",
        help="Output file path (default: rainfall_grid.geojson)"
    )
    args = parser.parse_args()

    generate_rainfall_grid_geojson(args.output)
