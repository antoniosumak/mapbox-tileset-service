# Mapbox Tileset Automation Script

## Overview

This script automates the process of creating and publishing Mapbox tilesets from GeoJSON files with recipes.

## Quick Setup

### 1. Install Python Dependencies

This project uses a `requirements.txt` file (similar to `package.json` in Node.js) to manage Python dependencies.

**Install all dependencies at once:**

```bash
pip install -r requirements.txt
```

This will install:

- `python-dotenv` - for environment variable management
- `pandas` - for data processing
- `shapely` - for geometry operations
- `mapbox-tilesets` - Mapbox CLI tools

**Alternative: Manual Installation**

```bash
pip install python-dotenv pandas shapely mapbox-tilesets
```

### 2. Set Up Environment Variables

Create a `.env` file in the project root:

```bash
MAPBOX_SECRET_KEY=your_mapbox_token_here
```

### 3. Verify Installation

Check that everything is installed correctly:

```bash
python -c "import pandas, shapely, dotenv; print('All dependencies installed successfully!')"
tilesets --version
```

## Getting Started

**Complete workflow example:**

1. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

2. **Configure your Mapbox token:**

   ```bash
   # Create .env file
   echo MAPBOX_SECRET_KEY=your_token_here > .env
   ```

3. **Process your data (if needed):**

   ```bash
   python watts_txt_to_geojson.py ./input_data --output ./output
   ```

4. **Upload to Mapbox:**
   ```bash
   python process_tilesets.py
   ```

Done! Your tilesets will be uploaded and published to Mapbox.

## Prerequisites

1. **Python**: Python 3.8 or higher recommended

2. **Folder structure**: Each folder should contain:
   - A `.geojson` file (e.g., `01.geojson`)
   - A `recipe.json` file

## How to Use

### Option 1: Using the Batch File (Windows)

1. Open **Anaconda Prompt** or **Anaconda PowerShell Prompt**
2. Navigate to the mappings directory:
   ```bash
   cd C:\mappings
   ```
3. Run the batch file:
   ```bash
   run_tilesets.bat
   ```

### Option 2: Run Python Script Directly

1. Open **Anaconda Prompt** or **Anaconda PowerShell Prompt**
2. Navigate to the mappings directory:
   ```bash
   cd C:\mappings
   ```
3. Run the script:
   ```bash
   python process_tilesets.py
   ```

## What the Script Does

For each folder (01, 02, 04, 05, etc.), the script will:

1. **Add Source**: Upload the GeoJSON file to Mapbox

   ```bash
   tilesets add-source brisk-mapbox <folder_name> <path_to_geojson> --token <token> --no-validation
   ```

2. **Create Tileset**: Create a tileset with the recipe

   ```bash
   tilesets create brisk-mapbox.<folder_name> --recipe <recipe_path> --name "<folder_name>" --token <token>
   ```

3. **Publish Tileset**: Publish the tileset to make it available
   ```bash
   tilesets publish brisk-mapbox.<folder_name> --token <token>
   ```

## Configuration

The script uses the following configuration:

- **Profile Name**: `brisk-mapbox` (hardcoded in `process_tilesets.py`)
- **Token**: Loaded from `.env` file as `MAPBOX_SECRET_KEY`

**To configure your Mapbox token:**

Create a `.env` file in the project root:

```bash
MAPBOX_SECRET_KEY=your_actual_mapbox_token_here
```

**To change the profile name:**

Edit `process_tilesets.py`:

```python
PROFILE_NAME = "brisk-mapbox"  # Change this if needed
```

## Output

The script will:

- Show progress for each folder being processed
- Display the commands being executed
- Show success/error messages for each step
- Provide a summary at the end showing which folders succeeded/failed

## Troubleshooting

### "tilesets: command not found"

Install the Mapbox Tilesets CLI:

```bash
npm install -g @mapbox/tilesets-cli
```

or

```bash
pip install mapbox-tilesets
```

### Permission or authentication errors

- Verify your token is correct and has the necessary permissions
- Check that you have write access to your Mapbox account

### GeoJSON file too large

The script uses `--no-validation` to skip validation for large files. If you still have issues:

- Consider splitting large GeoJSON files
- Check Mapbox's file size limits

## Folder Structure Example

```
C:\mappings\
├── 01\
│   ├── 01.geojson
│   └── recipe.json
├── 02\
│   ├── 02.geojson
│   └── recipe.json
├── 04\
│   ├── 04.geojson
│   └── recipe.json
├── process_tilesets.py
├── run_tilesets.bat
└── README_INSTRUCTIONS.md
```

## Additional Script: watts_txt_to_geojson.py

This script converts pipe-delimited text files (exported from GDB) to GeoJSON format with Mapbox recipe files.

**Usage:**

```bash
python watts_txt_to_geojson.py <input_directory> --output <output_directory> --workers 2
```

**Example:**

```bash
python watts_txt_to_geojson.py ./data --output ./output --workers 4
```

**Options:**

- `input_dir` - Directory containing input .txt files (pattern: `clu*_*_STR.txt`)
- `--output, -o` - Output directory for FIPS code folders (default: `output`)
- `--workers, -w` - Number of parallel workers (default: 2)
- `--chunk-size, -c` - Rows to process at a time (default: 10000)

**Output:**

- Creates folders named by state FIPS codes (e.g., `01`, `02`)
- Each folder contains a `.geojson` file and `recipe.json`
- These folders can then be processed by `process_tilesets.py`

## Notes

- The script will ask for confirmation before processing
- Processing may take time depending on file sizes
- Failed folders will be reported at the end
- You can interrupt the script with Ctrl+C if needed
- Make sure to create a `.env` file with your `MAPBOX_SECRET_KEY` before running
