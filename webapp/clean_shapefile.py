"""
clean_shapefile.py — Strip unnecessary columns from shapefiles.

Keeps only the fields needed for sewer profile analysis.
Saves cleaned copies alongside the originals with a _clean suffix.

Usage:
    python3 clean_shapefile.py
"""

import geopandas as gpd
import os

# ── Fields to keep per layer type ──
KEEP_FIELDS = {

    # Manholes / Junctions
    "manhole": [
        "FacilityID",   # Junction ID
        "AssetID",      # Backup/legacy ID
        "RimElevati",   # Rim elevation
        "invelevati",   # Invert elevation
        "HighPipeEl",   # High pipe elevation (alt invert)
        "Depth",        # Structure depth
        "SubType_TE",   # Structure type
        "SubBasin",     # Sub-basin grouping
        "LifeCycleS",   # Active/retired status
        "Condition",    # Condition rating
    ],

    # Pipes / Mains
    "main": [
        "FacilityID",   # Pipe ID
        "AssetID",      # Backup ID
        "Diameter",     # Pipe diameter
        "Material",     # Pipe material
        "UpstreamIn",   # Upstream invert
        "Downstrea",    # Downstream invert
        "Length",       # Pipe length
        "UpMH",         # Upstream manhole ID
        "DownMH",       # Downstream manhole ID
        "Slope",        # Slope
        "SubBasin",     # Sub-basin
        "LifeCycleS",   # Status
        "Condition",    # Condition
        "PipeType",     # Gravity / force main
    ],

    # Fittings
    "fitting": [
        "FacilityID",
        "AssetID",
        "SubType_TE",
        "LifeCycleS",
    ],
}


def clean_layer(input_path, layer_type):
    """
    Read a shapefile, keep only needed columns, save cleaned version.
    """
    print(f"\nReading: {input_path}")
    gdf = gpd.read_file(input_path)
    print(f"  Features: {len(gdf):,}  |  Columns: {len(gdf.columns) - 1} (excl. geometry)")

    keep = KEEP_FIELDS.get(layer_type, [])

    # Only keep columns that actually exist in this file
    available = [c for c in keep if c in gdf.columns]
    missing = [c for c in keep if c not in gdf.columns]

    if missing:
        print(f"  NOTE — these expected fields were not found and will be skipped: {missing}")

    # Always keep geometry
    gdf_clean = gdf[available + ["geometry"]].copy()

    # Build output path
    base, ext = os.path.splitext(input_path)
    out_path = base + "_clean" + ext

    gdf_clean.to_file(out_path)
    print(f"  Kept {len(available)} fields: {available}")
    print(f"  Dropped {len(gdf.columns) - 1 - len(available)} fields")
    print(f"  Saved to: {out_path}")
    return out_path


if __name__ == "__main__":

    # ── Update these paths to point to your shapefiles ──
    layers = [
        # (path to shapefile,  layer type key)
        ("../data/CityofNorthVancouver/SanMain_shp/SanMain.shp",       "main"),
        ("../data/CityofNorthVancouver/SanFacility_shp/SanFacility.shp", "manhole"),
        ("../data/CityofNorthVancouver/SanFitting_shp/SanFitting.shp",   "fitting"),
    ]

    # Also handle uploaded file if run directly on it
    import sys
    if len(sys.argv) == 3:
        # Usage: python3 clean_shapefile.py path/to/file.shp layer_type
        custom_path = sys.argv[1]
        custom_type = sys.argv[2]
        layers = [(custom_path, custom_type)]

    print("=" * 60)
    print("Shapefile Column Cleaner")
    print("=" * 60)

    for path, ltype in layers:
        abs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), path))
        if os.path.exists(abs_path):
            clean_layer(abs_path, ltype)
        else:
            print(f"\nSKIPPED (not found): {abs_path}")

    print("\nDone. Upload the _clean.shp files to the app.")
