"""
auto_fix.py — Auto-fix engine for detected profile issues.

For each issue type, proposes a fix and can apply it to the data.
Fixes are non-destructive: original values are preserved in a log,
and a new "fixed" dataset is produced alongside the original.
"""

import copy


class FixProposal:
    """Represents a proposed fix for a detected issue."""

    APPLIED = "APPLIED"
    PROPOSED = "PROPOSED"
    MANUAL = "MANUAL_REVIEW"

    def __init__(self, issue_type, feature_id, field, old_value, new_value,
                 fix_method, confidence, description):
        self.issue_type = issue_type
        self.feature_id = feature_id
        self.field = field
        self.old_value = old_value
        self.new_value = new_value
        self.fix_method = fix_method
        self.confidence = confidence  # HIGH, MEDIUM, LOW
        self.description = description
        self.status = self.PROPOSED

    def to_dict(self):
        return {
            "issue_type": self.issue_type,
            "feature_id": self.feature_id,
            "field": self.field,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "fix_method": self.fix_method,
            "confidence": self.confidence,
            "description": self.description,
            "status": self.status,
        }


def _safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def fix_adverse_slopes(issues, pipe_records, junction_records):
    """
    Propose fixes for adverse slope issues.

    Strategy: For a pipe where ds_invert > us_invert, we can either:
    1. Swap the inverts (if flow direction is wrong)
    2. Interpolate the downstream invert based on neighboring pipes
    3. Flag for manual review if we can't determine the correct fix

    Simple approach: If the pipe's us_invert matches the upstream junction's
    invert, assume the ds_invert is the error and interpolate.
    """
    fixes = []
    pipe_lookup = {r.get("pipe_id"): r for r in pipe_records}
    junc_lookup = {r.get("junction_id"): r for r in junction_records}

    for issue in issues:
        if issue.issue_type != "ADVERSE_SLOPE":
            continue

        details = issue.details
        pid = issue.feature_id
        us_node = details.get("us_node")
        ds_node = details.get("ds_node")
        us_inv = _safe_float(details.get("us_invert"))
        ds_inv = _safe_float(details.get("ds_invert"))
        length = _safe_float(details.get("length"))

        if us_inv is None or ds_inv is None:
            fixes.append(FixProposal(
                "ADVERSE_SLOPE", pid, "us_invert/ds_invert",
                f"US={us_inv}, DS={ds_inv}", None,
                "MANUAL", "LOW",
                f"Cannot auto-fix: invert values are null. Manual review required."
            ))
            continue

        # Check if this might be a flow direction error (inverts are just swapped)
        if ds_inv > us_inv:
            # Simple fix: set ds_invert to be slightly lower than us_invert
            # using a minimal slope of 0.005 ft/ft (common minimum for gravity sewers)
            if length and length > 0:
                min_slope = 0.005
                proposed_ds = us_inv - (min_slope * length)

                # Don't let the fix create an unreasonably deep pipe
                if proposed_ds > 0:
                    fixes.append(FixProposal(
                        "ADVERSE_SLOPE", pid, "ds_invert",
                        ds_inv, round(proposed_ds, 2),
                        "INTERPOLATE_MIN_SLOPE", "MEDIUM",
                        f"Set DS invert to {proposed_ds:.2f} using minimum slope "
                        f"of {min_slope} ft/ft over {length} ft. "
                        f"Original: {ds_inv}, change: {ds_inv - proposed_ds:.2f} ft."
                    ))
                else:
                    fixes.append(FixProposal(
                        "ADVERSE_SLOPE", pid, "ds_invert",
                        ds_inv, None,
                        "MANUAL", "LOW",
                        f"Min slope interpolation produces negative elevation. Manual review needed."
                    ))
            else:
                # No length data — propose swapping inverts
                fixes.append(FixProposal(
                    "ADVERSE_SLOPE", pid, "us_invert/ds_invert",
                    f"US={us_inv}, DS={ds_inv}",
                    f"US={ds_inv}, DS={us_inv}",
                    "SWAP_INVERTS", "LOW",
                    f"Possible flow direction error. Consider swapping inverts. Manual review recommended."
                ))

    return fixes


def fix_invert_mismatches(issues, pipe_records, junction_records):
    """
    Propose fixes for invert mismatches at junctions.

    Strategy: Adjust the pipe invert to match the junction invert,
    since junction survey data is typically more reliable than pipe data.
    """
    fixes = []

    for issue in issues:
        if issue.issue_type != "INVERT_MISMATCH":
            continue

        details = issue.details
        jid = details.get("junction_id")
        pid = details.get("pipe_id")
        junc_inv = _safe_float(details.get("junction_invert"))
        diff = _safe_float(details.get("difference"))

        pipe_inv_field = None
        pipe_inv_val = None
        if "pipe_ds_invert" in details:
            pipe_inv_field = "ds_invert"
            pipe_inv_val = _safe_float(details["pipe_ds_invert"])
        elif "pipe_us_invert" in details:
            pipe_inv_field = "us_invert"
            pipe_inv_val = _safe_float(details["pipe_us_invert"])

        if junc_inv is not None and pipe_inv_val is not None and pipe_inv_field:
            confidence = "HIGH" if diff and diff < 1.0 else "MEDIUM"
            fixes.append(FixProposal(
                "INVERT_MISMATCH", pid, pipe_inv_field,
                pipe_inv_val, junc_inv,
                "MATCH_JUNCTION", confidence,
                f"Set pipe {pid} {pipe_inv_field} from {pipe_inv_val} to "
                f"{junc_inv} to match junction {jid} invert. Diff was {diff:.3f} ft."
            ))

    return fixes


def fix_diameter_decreases(issues, pipe_records):
    """
    Propose fixes for diameter decreases along flow path.

    Strategy: Flag for manual review — could be a data entry error
    or a legitimate design choice. Suggest matching upstream diameter.
    """
    fixes = []

    for issue in issues:
        if issue.issue_type != "DIAMETER_DECREASE":
            continue

        details = issue.details
        ds_pipe = details.get("downstream_pipe")
        ds_diam = _safe_float(details.get("downstream_diameter"))
        us_diam = _safe_float(details.get("upstream_diameter"))

        if ds_diam is not None and us_diam is not None:
            fixes.append(FixProposal(
                "DIAMETER_DECREASE", ds_pipe, "diameter",
                ds_diam, us_diam,
                "MATCH_UPSTREAM", "LOW",
                f"Suggest changing pipe {ds_pipe} diameter from {ds_diam}\" to "
                f"{us_diam}\" to match upstream. VERIFY — this could be intentional "
                f"(e.g., rehabilitation with liner). Manual review recommended."
            ))

    return fixes


def fix_null_diameters(issues, pipe_records):
    """
    Propose fixes for null diameter values.

    Strategy: Look at neighboring pipes and suggest the most common
    diameter in the immediate vicinity.
    """
    fixes = []
    all_diameters = [_safe_float(r.get("diameter")) for r in pipe_records
                     if _safe_float(r.get("diameter")) is not None]

    if all_diameters:
        from collections import Counter
        most_common = Counter(all_diameters).most_common(1)[0][0]
    else:
        most_common = None

    for issue in issues:
        if issue.issue_type != "NULL_DIAMETER":
            continue

        pid = issue.feature_id
        if most_common:
            fixes.append(FixProposal(
                "NULL_DIAMETER", pid, "diameter",
                None, most_common,
                "USE_NETWORK_MODE", "LOW",
                f"Set diameter to {most_common}\" (most common in network). "
                f"Manual verification strongly recommended."
            ))
        else:
            fixes.append(FixProposal(
                "NULL_DIAMETER", pid, "diameter",
                None, None,
                "MANUAL", "LOW",
                f"Cannot determine diameter — no reference data available."
            ))

    return fixes


def generate_all_fixes(analysis_result, pipe_records, junction_records):
    """
    Generate fix proposals for all detected issues.

    Returns list of FixProposal objects.
    """
    issues = analysis_result.get("issues", [])
    all_fixes = []

    all_fixes.extend(fix_adverse_slopes(issues, pipe_records, junction_records))
    all_fixes.extend(fix_invert_mismatches(issues, pipe_records, junction_records))
    all_fixes.extend(fix_diameter_decreases(issues, pipe_records))
    all_fixes.extend(fix_null_diameters(issues, pipe_records))

    # Connectivity issues (dead ends, orphans) don't have auto-fixes
    for issue in issues:
        if issue.issue_type in ("DEAD_END", "ORPHAN_NODE"):
            all_fixes.append(FixProposal(
                issue.issue_type, issue.feature_id, "connectivity",
                None, None,
                "MANUAL", "LOW",
                f"{issue.message} — requires manual review of network connectivity."
            ))

    return all_fixes


def apply_fixes_to_gdf(gdf, fixes, mapping, feature_type):
    """
    Apply accepted fixes to a GeoDataFrame, producing a corrected copy.

    Parameters
    ----------
    gdf : GeoDataFrame
        Original data.
    fixes : list[FixProposal]
        Fixes to apply (only those with status == APPLIED).
    mapping : dict
        Field mapping {internal_name: source_field_name}.
    feature_type : str
        'pipes', 'junctions', etc.

    Returns
    -------
    GeoDataFrame with fixes applied.
    """
    gdf_fixed = gdf.copy()

    # Reverse mapping: internal -> source
    reverse_map = {v: k for k, v in mapping.items() if v is not None}
    # Also keep internal -> source for lookup
    int_to_source = {k: v for k, v in mapping.items() if v is not None}

    # Build feature ID field name
    id_fields = {
        "pipes": "pipe_id",
        "junctions": "junction_id",
        "pumps": "station_id",
        "storage": "tank_id",
    }
    id_internal = id_fields.get(feature_type)
    id_source = int_to_source.get(id_internal) if id_internal else None

    for fix in fixes:
        if fix.status != FixProposal.APPLIED:
            continue
        if fix.new_value is None:
            continue

        # Find the source field name for the fix field
        source_field = int_to_source.get(fix.field)
        if source_field is None or source_field not in gdf_fixed.columns:
            continue

        # Find the row by feature ID
        if id_source and id_source in gdf_fixed.columns:
            mask = gdf_fixed[id_source] == fix.feature_id
            if mask.any():
                gdf_fixed.loc[mask, source_field] = fix.new_value
                fix.status = FixProposal.APPLIED

    return gdf_fixed
