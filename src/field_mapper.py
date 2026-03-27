"""
field_mapper.py — Auto-detect and map municipal GIS field names to internal schema.

Loads the default_field_mapping.json config, scans input feature class fields,
and returns a mapping dict. Supports auto-detection with manual override.
"""

import json
import os


CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config")
DEFAULT_CONFIG = os.path.join(CONFIG_DIR, "default_field_mapping.json")


def load_field_config(config_path=None):
    """Load field mapping configuration from JSON file."""
    path = config_path or DEFAULT_CONFIG
    with open(path, "r") as f:
        config = json.load(f)
    # Remove comment keys
    return {k: v for k, v in config.items() if not k.startswith("_")}


def auto_detect_fields(source_fields, feature_type, config=None):
    """
    Auto-detect field mapping by matching source field names against known variants.

    Parameters
    ----------
    source_fields : list[str]
        Field names from the input feature class (e.g., from arcpy.ListFields()).
    feature_type : str
        One of: 'pipes', 'junctions', 'pumps', 'storage'.
    config : dict, optional
        Field mapping config. If None, loads default.

    Returns
    -------
    dict
        {internal_field_name: matched_source_field_name or None}
        e.g. {'us_invert': 'INVERTUP', 'ds_invert': 'INVERTDN', ...}
    """
    if config is None:
        config = load_field_config()

    if feature_type not in config:
        raise ValueError(f"Unknown feature type '{feature_type}'. Expected one of: {list(config.keys())}")

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


def validate_mapping(mapping, required_fields):
    """
    Check that all required internal fields have a mapped source field.

    Parameters
    ----------
    mapping : dict
        Output from auto_detect_fields().
    required_fields : list[str]
        Internal field names that must be mapped.

    Returns
    -------
    tuple (bool, list[str])
        (is_valid, list of unmapped required field names)
    """
    unmapped = [f for f in required_fields if mapping.get(f) is None]
    return len(unmapped) == 0, unmapped


def apply_overrides(mapping, overrides):
    """
    Apply manual user overrides to an auto-detected mapping.

    Parameters
    ----------
    mapping : dict
        Output from auto_detect_fields().
    overrides : dict
        {internal_field_name: user_specified_source_field}

    Returns
    -------
    dict
        Updated mapping with overrides applied.
    """
    updated = dict(mapping)
    for key, value in overrides.items():
        if key in updated:
            updated[key] = value
    return updated


def get_required_fields(feature_type):
    """Return the list of required fields for each feature type."""
    required = {
        "pipes": ["pipe_id", "us_invert", "ds_invert", "diameter"],
        "junctions": ["junction_id", "invert_elev"],
        "pumps": ["station_id", "inlet_invert"],
        "storage": ["tank_id", "base_elev"],
    }
    return required.get(feature_type, [])


def summarize_mapping(mapping, feature_type):
    """
    Return a human-readable summary of the field mapping.

    Parameters
    ----------
    mapping : dict
        Output from auto_detect_fields().
    feature_type : str
        Feature type name for display.

    Returns
    -------
    str
        Formatted summary string.
    """
    required = get_required_fields(feature_type)
    lines = [f"Field Mapping for {feature_type.upper()}:", "-" * 50]
    for internal, source in mapping.items():
        status = ""
        if source is None:
            status = " ** UNMAPPED"
            if internal in required:
                status = " ** UNMAPPED (REQUIRED)"
        lines.append(f"  {internal:20s} -> {source or '---':30s}{status}")
    return "\n".join(lines)
