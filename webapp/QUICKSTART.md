# Sewer Profile Analyzer — Quick Start

## Setup (one time)

```bash
# 1. Make sure you have Python 3.9+
python --version

# 2. Create a virtual environment (recommended)
python -m venv venv
source venv/bin/activate        # Mac/Linux
# venv\Scripts\activate         # Windows

# 3. Install dependencies
cd webapp
pip install -r requirements.txt
```

## Run the App

```bash
# From the 01_ArcGIS_Pro_AutoFix_Tool directory:
cd webapp
streamlit run app.py
```

This opens a browser window at http://localhost:8501 with the full app.

## How to Use

1. **Upload shapefiles** — Drag in your Pipes (.shp + .shx + .dbf + .prj) and Junctions files. Pumps and Storage are optional. You can also upload a single .zip per layer.

2. **Check field mapping** — The tool auto-detects common field names (INVERTUP, DIAMETER, RIMELEV, etc.). If your city uses different names, adjust the dropdowns.

3. **Adjust settings** — Use the sidebar sliders for snap tolerance, invert mismatch tolerance, and depth thresholds.

4. **Click "Run Analysis"** — The tool builds a connected network, checks every pipe and junction, and reports issues.

5. **Review the map** — Issues are color-coded: red = adverse slope, orange = invert mismatch, gold = diameter decrease, purple = missing data, blue = dead end, gray = orphan.

6. **Review fixes** — Each issue has a proposed fix with a confidence rating. Accept or reject each one.

7. **Export** — Download the issues report (CSV or Excel), the fix log, and corrected shapefiles.

## What It Detects

- **Adverse slopes** — Pipes where water would flow uphill
- **Invert mismatches** — Pipe inverts that don't match the junction they connect to
- **Diameter decreases** — Pipe gets smaller going downstream (without a pump)
- **Null/missing data** — Missing diameters or invert elevations
- **Dead ends** — Junctions with incoming pipes but nothing going out
- **Orphan nodes** — Junctions not connected to any pipe
- **Shallow/deep structures** — Manholes with unusual depth

## Test Without Real Data

```bash
# Run the pipeline test with built-in synthetic data:
python run_local_test.py
```

## File Structure

```
webapp/
  app.py                  # Streamlit web app (main entry point)
  ingest_gpd.py           # GeoPandas-based shapefile reader
  map_builder.py          # Folium interactive map builder
  auto_fix.py             # Fix proposal engine
  run_local_test.py       # Pipeline test (no streamlit needed)
  requirements.txt        # Python dependencies
  QUICKSTART.md           # This file

src/
  network_builder.py      # Directed graph construction
  profile_analyzer.py     # Profile issue detection
  validate.py             # Data validation checks
  field_mapper.py         # Field name auto-detection
  generate_test_data.py   # Synthetic test data generator

config/
  default_field_mapping.json  # Known field name variants
```
