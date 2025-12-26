#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mapbox Tileset Automation Script
This script processes folders containing GeoJSON files and recipe.json files,
then executes Mapbox tileset commands to add sources, create tilesets, and publish them.
"""
from dotenv import load_dotenv
import os
import subprocess
import sys
from pathlib import Path
import time
import io

load_dotenv()

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

PROFILE_NAME = "brisk-mapbox"
TOKEN = os.getenv('MAPBOX_SECRET_KEY')
BASE_DIR = Path(__file__).parent

def find_tilesets_command():
    """Find the tilesets command, checking for installed executables."""
    import site
    import shutil

    tilesets_path = shutil.which('tilesets')
    if tilesets_path:
        return 'tilesets'

    if sys.platform == 'win32':
        user_scripts = Path(site.USER_BASE) / 'Python313' / 'Scripts'
        tilesets_exe = user_scripts / 'tilesets.exe'
        if tilesets_exe.exists():
            return str(tilesets_exe)

    return 'tilesets'

TILESETS_CMD = find_tilesets_command()

def run_command(command, description):
    """
    Execute a shell command and handle errors.

    Args:
        command (list): Command and arguments as list
        description (str): Description of what the command does

    Returns:
        bool: True if successful, False otherwise
    """
    print(f"\n{'='*60}")
    print(f"🔄 {description}")
    print(f"{'='*60}")

    quoted_command = []
    for arg in command:
        if ' ' in arg and not (arg.startswith('"') and arg.endswith('"')):
            quoted_command.append(f'"{arg}"')
        else:
            quoted_command.append(arg)

    command_str = ' '.join(quoted_command)
    print(f"Command: {command_str}")
    print(f"⏳ Executing... (this may take a few seconds)")
    sys.stdout.flush()

    try:
        result = subprocess.run(
            command_str,
            capture_output=True,
            text=True,
            check=True,
            shell=True
        )

        print(f"✅ Success!")
        if result.stdout:
            print(f"Output:\n{result.stdout}")

        return True

    except subprocess.CalledProcessError as e:
        print(f"❌ Error executing command!")
        print(f"Error code: {e.returncode}")
        if e.stdout:
            print(f"Output:\n{e.stdout}")
        if e.stderr:
            print(f"Error:\n{e.stderr}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

def find_geojson_file(folder_path):
    """
    Find the GeoJSON file in the folder.

    Args:
        folder_path (Path): Path to the folder

    Returns:
        Path or None: Path to the GeoJSON file if found
    """
    geojson_files = list(folder_path.glob("*.geojson"))
    if not geojson_files:
        return None
    return geojson_files[0]

def process_folder(folder_path, folder_name):
    """
    Process a single folder containing GeoJSON and recipe.json.

    Args:
        folder_path (Path): Path to the folder
        folder_name (str): Name of the folder (used as source name)

    Returns:
        bool: True if all steps successful, False otherwise
    """
    print(f"\n{'#'*60}")
    print(f"# Processing folder: {folder_name}")
    print(f"{'#'*60}")

    print(f"🔍 Checking for required files...")
    recipe_path = folder_path / "recipe.json"
    if not recipe_path.exists():
        print(f"⚠️  Warning: recipe.json not found in {folder_name}, skipping...")
        return False

    geojson_path = find_geojson_file(folder_path)
    if not geojson_path:
        print(f"⚠️  Warning: No GeoJSON file found in {folder_name}, skipping...")
        return False

    print(f"📁 Found GeoJSON: {geojson_path.name}")
    print(f"📄 Found recipe: {recipe_path.name}")

    source_name = folder_name
    tile_id = f"{PROFILE_NAME}.{source_name}"
    tileset_name = folder_name

    print(f"📋 Configuration:")
    print(f"   - Source name: {source_name}")
    print(f"   - Tileset ID: {tile_id}")
    print(f"   - Tileset name: {tileset_name}")
    sys.stdout.flush()

    add_source_cmd = [
        TILESETS_CMD, "add-source",
        PROFILE_NAME,
        source_name,
        str(geojson_path),
        "--token", TOKEN,
        "--no-validation"
    ]

    if not run_command(add_source_cmd, f"Step 1/3: Adding source '{source_name}'"):
        print(f"❌ Failed to add source for {folder_name}")
        return False

    print(f"⏸️  Waiting 1 second before next step...")
    sys.stdout.flush()
    time.sleep(1)

    create_tileset_cmd = [
        TILESETS_CMD, "create",
        tile_id,
        "--recipe", str(recipe_path),
        "--name", tileset_name,
        "--token", TOKEN
    ]

    if not run_command(create_tileset_cmd, f"Step 2/3: Creating tileset '{tile_id}'"):
        print(f"❌ Failed to create tileset for {folder_name}")
        return False

    print(f"⏸️  Waiting 1 second before next step...")
    sys.stdout.flush()
    time.sleep(1)

    publish_tileset_cmd = [
        TILESETS_CMD, "publish",
        tile_id,
        "--token", TOKEN
    ]

    if not run_command(publish_tileset_cmd, f"Step 3/3: Publishing tileset '{tile_id}'"):
        print(f"❌ Failed to publish tileset for {folder_name}")
        return False

    print(f"\n✅ Successfully processed {folder_name}!")
    return True

def main():
    """
    Main function to process all folders in the directory.
    """
    print("="*60)
    print("  Mapbox Tileset Automation Script")
    print("="*60)
    print(f"Profile: {PROFILE_NAME}")
    print(f"Base directory: {BASE_DIR}")
    print("="*60)

    print(f"🔍 Scanning directory for folders to process...")
    folders_to_process = []

    for item in BASE_DIR.iterdir():
        if item.is_dir() and not item.name.startswith('.'):
            recipe_exists = (item / "recipe.json").exists()
            geojson_exists = any(item.glob("*.geojson"))

            if recipe_exists and geojson_exists:
                folders_to_process.append(item)
                print(f"   ✓ {item.name} - has required files")
            else:
                print(f"   ⏭️  {item.name} - missing recipe.json or .geojson file")

    folders_to_process.sort(key=lambda x: x.name)

    if not folders_to_process:
        print("\n❌ No folders found with both recipe.json and .geojson files!")
        print("Make sure your folders contain:")
        print("  - <folder_name>.geojson")
        print("  - recipe.json")
        sys.exit(1)

    print(f"\n📦 Found {len(folders_to_process)} folder(s) to process:")
    for folder in folders_to_process:
        print(f"   - {folder.name}")

    print("\n" + "="*60)
    response = input("Do you want to proceed? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("❌ Operation cancelled by user.")
        sys.exit(0)

    print("\n" + "="*60)
    print("🚀 Starting processing...")
    print("="*60)

    successful = []
    failed = []
    total = len(folders_to_process)

    for idx, folder in enumerate(folders_to_process, 1):
        print(f"\n\n{'='*60}")
        print(f"📊 Progress: {idx}/{total} folders")
        print(f"{'='*60}")

        if process_folder(folder, folder.name):
            successful.append(folder.name)
            print(f"✅ Folder '{folder.name}' completed successfully")
        else:
            failed.append(folder.name)
            print(f"❌ Folder '{folder.name}' failed")

        sys.stdout.flush()

    print("\n" + "="*60)
    print("  PROCESSING SUMMARY")
    print("="*60)
    print(f"✅ Successful: {len(successful)}")
    for name in successful:
        print(f"   - {name}")

    if failed:
        print(f"\n❌ Failed: {len(failed)}")
        for name in failed:
            print(f"   - {name}")

    print("="*60)
    print("🏁 Script execution completed!")
    print("="*60)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Script interrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
