"""
ingest_gpd.py — GeoPandas-based ingestion module (no arcpy dependency).

Reads shapefiles/GeoJSON into GeoDataFrames, applies field mapping,
validates geometry, and returns standardized data for the network builder.
"""

import geopandas as gpd
import pandas as pd
import json
import os
import tempfile
import zipfile

CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config")
DEFAULT_CONFIG = os.path.join(CONFIG_DIR, "default_field_mapping.json")


def load_field_config(config_path=None):
    path = config_path or DEFAULT_CONFIG
    with open(path, "r") as f:
        config = json.load(f)
    return {k: v for k, v in config.items() if not k.startswith("_")}


def auto_detect_fields(source_fields, feature_type, config=None):
    if config is None:
        config = load_field_config()
    if feature_type not in config:
        raise ValueError(f"Unknown feature type '{feature_type}'")
    variants = config[feature_type]
    source_upper = {f.upper(): f for f in source_fields}
    mapping = {}
    for internal_name, candidate_list in variants.items():
        matched = None
        for candidate in candidate_list:
            if candidate.upper() in source_upper:
                matched = source_upper[candidate.upper()]
                break
        mapping[internal_name] = matched
    return mapping


def get_required_fields(feature_type):
    required = {
        "pipes": ["pipe_id", "us_invert", "ds_invert", "diameter"],
        "junctions": ["junction_id", "invert_elev"],
        "pumps": ["station_id", "inlet_invert"],
        "storage": ["tank_id", "base_elev"],
    }
    return required.get(feature_type, [])


def read_shapefile_from_upload(uploaded_files):
    """
    Read a shapefile from Streamlit uploaded files.
    Expects multiple files (.shp, .shx, .dbf, .prj, etc.) or a single .zip.
    Returns a GeoDataFrame.
    """
    tmpdir = tempfile.mkdtemp()

    if len(uploaded_files) == 1 and uploaded_files[0].name.endswith('.zip'):
        zip_path = os.path.join(tmpdir, uploaded_files[0].name)
        with open(zip_path, 'wb') as f:
            f.write(uploaded_files[0].getbuffer())
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(tmpdir)
        shp_files = []
        for root, dirs, files in os.walk(tmpdir):
            for fn in files:
                if fn.endswith('.shp'):
                    shp_files.append(os.path.join(root, fn))
        if not shp_files:
            raise FileNotFoundError("No .shp file found in zip archive")
        return gpd.read_file(shp_files[0])
    else:
        for uf in uploaded_files:
            fpath = os.path.join(tmpdir, uf.name)
            with open(fpath, 'wb') as f:
                f.write(uf.getbuffer())
        shp_files = [f for f in os.listdir(tmpdir) if f.endswith('.shp')]
        if not shp_files:
            raise FileNotFoundError("No .shp file found in uploaded files")
        return gpd.read_file(os.path.join(tmpdir, shp_files[0]))


def read_shapefile_from_path(path):
    """Read a shapefile or GeoJSON from a file path."""
    return gpd.read_file(path)


def standardize_gdf(gdf, feature_type, mapping):
    """
    Rename columns in a GeoDataFrame according to the field mapping.
    Returns a new GeoDataFrame with internal field names.
    """
    rename_map = {}
    for internal, source in mapping.items():
        if source is not None and source in gdf.columns:
            rename_map[source] = internal
    gdf_std = gdf.rename(columns=rename_map).copy()
    return gdf_std


def gdf_to_records(gdf, feature_type, mapping):
    """
    Convert a GeoDataFrame to a list of dicts matching our internal schema.
    Geometry is preserved as Shapely objects.
    """
    records = []
    for idx, row in gdf.iterrows():
        record = {}
        for internal, source in mapping.items():
            if source is not None and source in gdf.columns:
                val = row[source]
                if pd.isna(val):
                    val = None
                record[internal] = val
            else:
                record[internal] = None
        record["geometry"] = row.geometry
        record["_gdf_index"] = idx
        records.append(record)
    return records


def ingest_gdf(gdf, feature_type, config=None, overrides=None):
    """
    Full ingestion pipeline for a GeoDataFrame.

    Returns dict with: records, mapping, gdf, crs, count, warnings, errors
    """
    warnings = []
    errors = []

    source_fields = [c for c in gdf.columns if c != "geometry"]
    mapping = auto_detect_fields(source_fields, feature_type, config)

    if overrides:
        for k, v in overrides.items():
            if k in mapping:
                mapping[k] = v

    required = get_required_fields(feature_type)
    unmapped = [f for f in required if mapping.get(f) is None]
    if unmapped:
        errors.append(f"Required fields not mapped for {feature_type}: {unmapped}")

    records = gdf_to_records(gdf, feature_type, mapping)

    null_geom = sum(1 for r in records if r["geometry"] is None or r["geometry"].is_empty)
    if null_geom > 0:
        warnings.append(f"{null_geom} features have null/empty geometry in {feature_type}")

    return {
        "records": records,
        "mapping": mapping,
        "gdf": gdf,
        "crs": gdf.crs,
        "count": len(records),
        "warnings": warnings,
        "errors": errors,
    }
