#!/usr/bin/env python3
"""
Parser for converting pipe-delimited GDB export text files to GeoJSON
Processes multiple state files in parallel and creates output folders with FIPS codes
"""

import pandas as pd
import os
import json
import argparse
import traceback
import gzip
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from shapely import wkt
from shapely.geometry import mapping

STATE_FIPS_MAPPING = {
    'Alabama': '01', 'Alaska': '02', 'Arizona': '04', 'Arkansas': '05',
    'California': '06', 'Colorado': '08', 'Connecticut': '09', 'Delaware': '10',
    'Florida': '12', 'Georgia': '13', 'Hawaii': '15', 'Idaho': '16',
    'Illinois': '17', 'Indiana': '18', 'Iowa': '19', 'Kansas': '20',
    'Kentucky': '21', 'Louisiana': '22', 'Maine': '23', 'Maryland': '24',
    'Massachusetts': '25', 'Michigan': '26', 'Minnesota': '27', 'Mississippi': '28',
    'Missouri': '29', 'Montana': '30', 'Nebraska': '31', 'Nevada': '32',
    'NewHampshire': '33', 'NewJersey': '34', 'NewMexico': '35', 'NewYork': '36',
    'NorthCarolina': '37', 'NorthDakota': '38', 'Ohio': '39', 'Oklahoma': '40',
    'Oregon': '41', 'Pennsylvania': '42', 'RhodeIsland': '44', 'SouthCarolina': '45',
    'SouthDakota': '46', 'Tennessee': '47', 'Texas': '48', 'Utah': '49',
    'Vermont': '50', 'Virginia': '51', 'Washington': '53', 'WestVirginia': '54',
    'Wisconsin': '55', 'Wyoming': '56', 'DistrictofColumbia': '11',
    'PuertoRico': '72', 'VirginIslands': '78', 'Guam': '66', 'AmericanSamoa': '60',
    'NorthernMarianaIslands': '69'
}

GEOJSON_PROPERTIES = [
    'cluIdentifier',
    'area',
    'cluCalculatedAcreage',
    'tractNumber',
    'farmNumber',
    'fieldNumber',
    'legalDescription',
    'centroid'
]


def is_null_value(val):
    if pd.isna(val) or val == '' or val == 'NULL' or val == 'NUL' or val == 'null' or val == 'nul':
        return True
    return False


def convert_to_int(val):
    if is_null_value(val):
        return None
    try:
        float_val = float(val)
        return int(float_val)
    except (ValueError, TypeError):
        return None


def convert_to_float(val):
    if is_null_value(val):
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def construct_legal_description(section, township_num, township_dir, range_num, range_dir):
    """
    Construct legal description from Section-Township-Range components
    Format: ${SectionNumber}-${TownshipNumber}${TownshipDirection}-${RangeNumber}${RangeDirection}
    Example: 15-12N-8W
    """
    try:
        if any(is_null_value(v) for v in [section, township_num, township_dir, range_num, range_dir]):
            return None

        section = str(section).strip()
        township_num = str(township_num).strip()
        township_dir = str(township_dir).strip()
        range_num = str(range_num).strip()
        range_dir = str(range_dir).strip()

        return f"{section}-{township_num}{township_dir}-{range_num}{range_dir}"
    except Exception:
        return None


def wkt_to_geojson_geometry(wkt_string):
    try:
        if is_null_value(wkt_string):
            return None

        geom = wkt.loads(wkt_string)

        geojson_geom = mapping(geom)

        if geojson_geom['type'] == 'Polygon':
            geojson_geom = {
                'type': 'MultiPolygon',
                'coordinates': [geojson_geom['coordinates']]
            }

        return geojson_geom
    except Exception as e:
        print(f"  Warning: Failed to parse WKT geometry: {str(e)}")
        return None


def extract_state_name_from_filename(filename):
    """
    Extract state name from filename like 'clu26_WestVirginia_STR.txt'
    Returns state name or None if not found
    """
    try:
        name_without_ext = filename.replace('.txt', '')
        parts = name_without_ext.split('_')
        if len(parts) >= 2:
            return parts[1]
        return None
    except Exception:
        return None


def get_fips_code(state_name):
    return STATE_FIPS_MAPPING.get(state_name)


def read_input_file_chunked(file_path, chunk_size=10000):
    print(f"  Reading file: {file_path}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    with open(file_path, 'rb') as f:
        magic = f.read(2)
        is_gzipped = (magic == b'\x1f\x8b')

    if is_gzipped:
        print(f"  Detected gzip-compressed file")
        file_handle = gzip.open(file_path, 'rt', encoding='utf-8')
        chunk_iterator = pd.read_csv(
            file_handle,
            sep='|',
            header=None,
            on_bad_lines='warn',
            dtype=str,
            na_values=['NULL', 'NUL', 'null', 'nul', ''],
            chunksize=chunk_size
        )
    else:
        chunk_iterator = pd.read_csv(
            file_path,
            sep='|',
            header=None,
            encoding='utf-8',
            on_bad_lines='warn',
            dtype=str,
            na_values=['NULL', 'NUL', 'null', 'nul', ''],
            chunksize=chunk_size
        )

    return chunk_iterator

def calculate_centroid(wkt_string):
    try: 
        geom = wkt.loads(wkt_string)
        centroid = geom.centroid
        return [centroid.x, centroid.y]
    except Exception:
        return None

def process_chunk_to_features(df_chunk, columns):
    if len(df_chunk.columns) != len(columns):
        raise ValueError(f"Column count mismatch: expected {len(columns)}, got {len(df_chunk.columns)}")

    df_chunk.columns = columns

    for idx, row in df_chunk.iterrows():
        geometry = wkt_to_geojson_geometry(row['Shape'])
        if geometry is None:
            continue 

        centroid = calculate_centroid(row["Shape"])

        properties = {
            'cluIdentifier': row['clu_identifier'] if not is_null_value(row['clu_identifier']) else None,
            'area': convert_to_float(row['clu_calculated_acreage']),
            'cluCalculatedAcreage': convert_to_float(row['clu_calculated_acreage']),
            'tractNumber': convert_to_int(row['tract_number']),
            'farmNumber': convert_to_int(row['farm_number']),
            'fieldNumber': convert_to_int(row['clu_number']),
            'legalDescription': construct_legal_description(
                row['SectionNumber'],
                row['TownshipNumber'],
                row['TownshipDirection'],
                row['RangeNumber'],
                row['RangeDirection']
            ),
            'centroid': centroid
        }

        feature = {
            'type': 'Feature',
            'properties': properties,
            'geometry': geometry
        }

        yield feature


def write_geojson_streaming(output_path, state_name, feature_generator):
    """
    Write GeoJSON file incrementally to minimize memory usage
    """
    with open(output_path, 'w', encoding='utf-8') as f:
        # Write header
        f.write('{\n')
        f.write('  "type": "FeatureCollection",\n')
        f.write(f'  "name": "{state_name}",\n')
        f.write('  "crs": {\n')
        f.write('    "type": "name",\n')
        f.write('    "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}\n')
        f.write('  },\n')
        f.write('  "features": [\n')

        feature_count = 0
        for feature in feature_generator:
            if feature_count > 0:
                f.write(',\n')
            json.dump(feature, f, indent=4)
            feature_count += 1

        f.write('\n  ]\n')
        f.write('}\n')

    return feature_count


def process_single_file(file_path, output_base_dir, chunk_size=10000):
    try:
        filename = os.path.basename(file_path)
        print(f"\nProcessing: {filename}")

        state_name = extract_state_name_from_filename(filename)
        if not state_name:
            return (filename, None, False, "Could not extract state name from filename")

        fips_code = get_fips_code(state_name)
        if not fips_code:
            return (state_name, None, False, f"No FIPS code found for state: {state_name}")

        print(f"  State: {state_name}, FIPS: {fips_code}")

        columns = [
            'objectid', 'Shape', 'clu_identifier', 'clu_number', 'tract_number',
            'farm_number', 'clu_classification_code', 'clu_calculated_acreage',
            'highly_erodible_land_type_code', 'comments', 'state_code', 'county_code',
            'data_source_site_identifier', 'creation_date', 'last_change_date',
            'data_source', 'admin_state', 'admin_county', 'cropland_indicator_3cm',
            'sap_crp', 'clu_status', 'cdist_fips', 'edit_reason', 'clu_alt_id',
            'last_chg_user_nm', 'cims_as_of_date', 'cimsfile', 'cims_loc_state',
            'cims_loc_county', 'shape_length', 'shape_area', 'BoundingBox',
            'StateCode', 'PrincipalMeridian', 'TownshipNumber', 'TownshipDirection',
            'RangeNumber', 'RangeDirection', 'SectionNumber'
        ]

        output_dir = os.path.join(output_base_dir, fips_code)
        os.makedirs(output_dir, exist_ok=True)

        def feature_generator():
            chunk_iterator = read_input_file_chunked(file_path, chunk_size)
            for chunk in chunk_iterator:
                for feature in process_chunk_to_features(chunk, columns):
                    yield feature

        print(f"  Creating GeoJSON features (streaming)...")

        geojson_path = os.path.join(output_dir, f"{fips_code}.geojson")
        feature_count = write_geojson_streaming(geojson_path, state_name, feature_generator())

        print(f"  Created {feature_count} features")
        print(f"  Saved: {geojson_path}")

        recipe = {
            'version': 1,
            'layers': {
                fips_code: {
                    'source': f'mapbox://tileset-source/brisk-mapbox/{fips_code}',
                    'minzoom': 1,
                    'maxzoom': 13,
                    'features': {
                        'attributes': {
                            'allowed_output': GEOJSON_PROPERTIES
                        }
                    }
                }
            }
        }

        recipe_path = os.path.join(output_dir, 'recipe.json')
        with open(recipe_path, 'w', encoding='utf-8') as f:
            json.dump(recipe, f, indent=2)
        print(f"  Saved: {recipe_path}")

        return (state_name, fips_code, True, None)

    except Exception as e:
        error_msg = f"Error: {str(e)}\n{traceback.format_exc()}"
        print(f"  {error_msg}")
        return (filename, None, False, error_msg)


def process_directory(input_dir, output_dir, max_workers=4, chunk_size=10000):
    """
    Process all .txt files in input directory using multi-threading

    Args:
        input_dir: Directory containing input .txt files
        output_dir: Base directory for output folders
        max_workers: Number of parallel threads
        chunk_size: Number of rows to process at a time per file
    """
    print(f"\nStarting batch processing...")
    print(f"Input directory: {input_dir}")
    print(f"Output directory: {output_dir}")
    print(f"Max workers: {max_workers}")
    print(f"Chunk size: {chunk_size} rows")

    input_path = Path(input_dir)
    txt_files = list(input_path.glob("clu*_*_STR.txt"))

    if not txt_files:
        print("\nNo matching .txt files found!")
        print("Looking for files matching pattern: clu*_*_STR.txt")
        return

    print(f"\nFound {len(txt_files)} files to process")

    os.makedirs(output_dir, exist_ok=True)

    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(process_single_file, str(file_path), output_dir, chunk_size): file_path
            for file_path in txt_files
        }

        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"\nUnexpected error processing {file_path}: {str(e)}")
                results.append((str(file_path), None, False, str(e)))

    print("\n" + "="*80)
    print("PROCESSING SUMMARY")
    print("="*80)

    successful = [r for r in results if r[2]]
    failed = [r for r in results if not r[2]]

    print(f"\nTotal files: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    if successful:
        print("\nSuccessful conversions:")
        for state_name, fips_code, _, _ in successful:
            print(f"   {state_name} (FIPS: {fips_code})")

    if failed:
        print("\nFailed conversions:")
        for identifier, fips_code, _, error in failed:
            print(f"   {identifier}")
            print(f"    Error: {error}")

    print("\n" + "="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Convert pipe-delimited text files to GeoJSON with recipe.json (memory-optimized)'
    )
    parser.add_argument('input_dir', help='Directory containing input .txt files')
    parser.add_argument('--output', '-o', default='output',
                       help='Output directory for FIPS code folders (default: output)')
    parser.add_argument('--workers', '-w', type=int, default=2,
                       help='Number of parallel workers (default: 2, lowered for memory efficiency)')
    parser.add_argument('--chunk-size', '-c', type=int, default=10000,
                       help='Number of rows to process at a time per file (default: 10000)')

    args = parser.parse_args()

    process_directory(args.input_dir, args.output,
                     max_workers=args.workers, chunk_size=args.chunk_size)


if __name__ == "__main__":
    main()
