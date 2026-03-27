"""
ingest.py — Shapefile / feature class ingestion module.

Reads pipes, junctions, pumps, and storage feature classes using arcpy,
applies field mapping, validates geometry, and returns standardized dicts.

NOTE: arcpy is only available inside ArcGIS Pro's Python environment.
For development/testing without arcpy, use ingest_geopandas.py instead.
"""

import os
from .field_mapper import auto_detect_fields, validate_mapping, get_required_fields, summarize_mapping

try:
    import arcpy
    HAS_ARCPY = True
except ImportError:
    HAS_ARCPY = False


def _get_field_names(feature_class):
    """Get list of field names from a feature class."""
    return [f.name for f in arcpy.ListFields(feature_class)]


def _get_spatial_reference(feature_class):
    """Get the spatial reference of a feature class."""
    desc = arcpy.Describe(feature_class)
    return desc.spatialReference


def _read_features(feature_class, field_mapping, geometry_type="shape"):
    """
    Read features from a feature class and return standardized records.

    Parameters
    ----------
    feature_class : str
        Path to feature class or shapefile.
    field_mapping : dict
        {internal_name: source_field_name} from auto_detect_fields().
    geometry_type : str
        'shape' for full geometry, 'centroid' for point representation of polygons.

    Returns
    -------
    list[dict]
        Each dict has internal field names as keys + 'SHAPE@' for geometry.
    """
    # Build list of source fields to read (skip None / unmapped)
    source_fields = []
    internal_keys = []
    for internal, source in field_mapping.items():
        if source is not None:
            source_fields.append(source)
            internal_keys.append(internal)

    # Add geometry token
    geom_token = "SHAPE@"
    source_fields.append(geom_token)

    records = []
    with arcpy.da.SearchCursor(feature_class, source_fields) as cursor:
        for row in cursor:
            record = {}
            for i, key in enumerate(internal_keys):
                record[key] = row[i]
            record["geometry"] = row[-1]  # SHAPE@ object
            records.append(record)

    return records


def ingest_feature_class(feature_class, feature_type, config=None, overrides=None):
    """
    Full ingestion pipeline for a single feature class.

    Parameters
    ----------
    feature_class : str
        Path to shapefile or geodatabase feature class.
    feature_type : str
        One of: 'pipes', 'junctions', 'pumps', 'storage'.
    config : dict, optional
        Field mapping config override.
    overrides : dict, optional
        Manual field mapping overrides {internal_name: source_field_name}.

    Returns
    -------
    dict with keys:
        'records': list[dict] — standardized feature records
        'mapping': dict — field mapping used
        'spatial_ref': arcpy.SpatialReference
        'count': int
        'warnings': list[str]
        'errors': list[str]
    """
    if not HAS_ARCPY:
        raise RuntimeError("arcpy is not available. Run this inside ArcGIS Pro's Python environment.")

    if not arcpy.Exists(feature_class):
        raise FileNotFoundError(f"Feature class not found: {feature_class}")

    warnings = []
    errors = []

    # Get source fields and spatial reference
    source_fields = _get_field_names(feature_class)
    spatial_ref = _get_spatial_reference(feature_class)

    # Auto-detect field mapping
    mapping = auto_detect_fields(source_fields, feature_type, config)

    # Apply manual overrides if provided
    if overrides:
        from .field_mapper import apply_overrides
        mapping = apply_overrides(mapping, overrides)

    # Validate required fields are mapped
    required = get_required_fields(feature_type)
    is_valid, unmapped = validate_mapping(mapping, required)
    if not is_valid:
        errors.append(f"Required fields not mapped for {feature_type}: {unmapped}")
        return {
            "records": [],
            "mapping": mapping,
            "spatial_ref": spatial_ref,
            "count": 0,
            "warnings": warnings,
            "errors": errors,
        }

    # Log mapping summary
    summary = summarize_mapping(mapping, feature_type)
    warnings.append(summary)

    # Read features
    records = _read_features(feature_class, mapping)

    # Basic geometry validation
    null_geom_count = 0
    for rec in records:
        if rec["geometry"] is None:
            null_geom_count += 1
    if null_geom_count > 0:
        warnings.append(f"{null_geom_count} features have null geometry in {feature_type}")

    return {
        "records": records,
        "mapping": mapping,
        "spatial_ref": spatial_ref,
        "count": len(records),
        "warnings": warnings,
        "errors": errors,
    }


def ingest_all(pipes_fc, junctions_fc, pumps_fc=None, storage_fc=None,
               config=None, overrides=None):
    """
    Ingest all feature classes for the network.

    Parameters
    ----------
    pipes_fc : str
        Path to pipes feature class (required).
    junctions_fc : str
        Path to junctions feature class (required).
    pumps_fc : str, optional
        Path to pumps feature class.
    storage_fc : str, optional
        Path to storage feature class.
    config : dict, optional
        Field mapping config.
    overrides : dict, optional
        {feature_type: {internal_name: source_field}} nested overrides.

    Returns
    -------
    dict with keys for each feature type containing ingestion results.
    """
    results = {}
    all_warnings = []
    all_errors = []

    fc_map = {
        "pipes": pipes_fc,
        "junctions": junctions_fc,
        "pumps": pumps_fc,
        "storage": storage_fc,
    }

    for ftype, fc_path in fc_map.items():
        if fc_path is None:
            continue
        type_overrides = (overrides or {}).get(ftype, None)
        result = ingest_feature_class(fc_path, ftype, config, type_overrides)
        results[ftype] = result
        all_warnings.extend(result["warnings"])
        all_errors.extend(result["errors"])

    # Check CRS consistency
    spatial_refs = []
    for ftype, result in results.items():
        if result.get("spatial_ref"):
            spatial_refs.append((ftype, result["spatial_ref"]))

    if len(spatial_refs) > 1:
        base_wkid = spatial_refs[0][1].factoryCode
        for ftype, sr in spatial_refs[1:]:
            if sr.factoryCode != base_wkid:
                all_warnings.append(
                    f"CRS mismatch: {ftype} uses WKID {sr.factoryCode}, "
                    f"but {spatial_refs[0][0]} uses WKID {base_wkid}. "
                    f"Features will be reprojected to match."
                )

    results["_summary"] = {
        "total_warnings": len(all_warnings),
        "total_errors": len(all_errors),
        "warnings": all_warnings,
        "errors": all_errors,
    }

    return results
