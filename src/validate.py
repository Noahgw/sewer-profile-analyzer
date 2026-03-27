"""
validate.py — Data validation module for ingested features.

Runs QA checks on standardized records: elevation ranges, null values,
diameter sanity, geometry validity. Returns a validation report.
"""


# Configurable thresholds (can be overridden by user)
DEFAULT_THRESHOLDS = {
    "min_elevation_ft": -100,       # lowest reasonable invert (below sea level areas)
    "max_elevation_ft": 15000,      # highest reasonable invert
    "min_depth_ft": 1.5,            # shallowest reasonable manhole
    "max_depth_ft": 40.0,           # deepest reasonable manhole
    "min_diameter_in": 2,           # smallest pipe (service lateral)
    "max_diameter_in": 180,         # largest pipe (15ft trunk)
    "min_pipe_length_ft": 0.1,     # essentially zero-length
    "max_pipe_length_ft": 5000,     # very long segment
    "min_slope": -0.50,             # heavily adverse (likely error)
    "max_slope": 0.50,              # 50% grade (likely error)
}


class ValidationIssue:
    """Represents a single validation issue found in the data."""

    SEVERITY_ERROR = "ERROR"
    SEVERITY_WARNING = "WARNING"
    SEVERITY_INFO = "INFO"

    def __init__(self, feature_type, feature_id, field, issue_type, severity, message, value=None):
        self.feature_type = feature_type
        self.feature_id = feature_id
        self.field = field
        self.issue_type = issue_type
        self.severity = severity
        self.message = message
        self.value = value

    def to_dict(self):
        return {
            "feature_type": self.feature_type,
            "feature_id": self.feature_id,
            "field": self.field,
            "issue_type": self.issue_type,
            "severity": self.severity,
            "message": self.message,
            "value": self.value,
        }

    def __repr__(self):
        return f"[{self.severity}] {self.feature_type} {self.feature_id}: {self.message}"


def _safe_float(val):
    """Try to convert a value to float, return None if not possible."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def validate_pipes(records, thresholds=None):
    """
    Validate pipe records.

    Checks:
    - Required fields populated (us_invert, ds_invert, diameter)
    - Elevation values within reasonable range
    - Diameter within reasonable range
    - Length > 0
    - Slope calculation and adverse slope detection
    - Upstream invert vs downstream invert logic
    """
    t = thresholds or DEFAULT_THRESHOLDS
    issues = []

    for rec in records:
        pid = rec.get("pipe_id", "UNKNOWN")

        # Check required numeric fields
        us_inv = _safe_float(rec.get("us_invert"))
        ds_inv = _safe_float(rec.get("ds_invert"))
        diam = _safe_float(rec.get("diameter"))
        length = _safe_float(rec.get("length"))

        # Null checks
        if us_inv is None:
            issues.append(ValidationIssue(
                "pipes", pid, "us_invert", "NULL_VALUE", "ERROR",
                f"Upstream invert is null or non-numeric", rec.get("us_invert")))

        if ds_inv is None:
            issues.append(ValidationIssue(
                "pipes", pid, "ds_invert", "NULL_VALUE", "ERROR",
                f"Downstream invert is null or non-numeric", rec.get("ds_invert")))

        if diam is None:
            issues.append(ValidationIssue(
                "pipes", pid, "diameter", "NULL_VALUE", "ERROR",
                f"Diameter is null or non-numeric", rec.get("diameter")))

        # Elevation range checks
        if us_inv is not None:
            if us_inv < t["min_elevation_ft"] or us_inv > t["max_elevation_ft"]:
                issues.append(ValidationIssue(
                    "pipes", pid, "us_invert", "OUT_OF_RANGE", "WARNING",
                    f"Upstream invert {us_inv} outside expected range "
                    f"[{t['min_elevation_ft']}, {t['max_elevation_ft']}]", us_inv))

        if ds_inv is not None:
            if ds_inv < t["min_elevation_ft"] or ds_inv > t["max_elevation_ft"]:
                issues.append(ValidationIssue(
                    "pipes", pid, "ds_invert", "OUT_OF_RANGE", "WARNING",
                    f"Downstream invert {ds_inv} outside expected range "
                    f"[{t['min_elevation_ft']}, {t['max_elevation_ft']}]", ds_inv))

        # Diameter range checks
        if diam is not None:
            if diam < t["min_diameter_in"]:
                issues.append(ValidationIssue(
                    "pipes", pid, "diameter", "OUT_OF_RANGE", "WARNING",
                    f"Diameter {diam}\" is below minimum {t['min_diameter_in']}\"", diam))
            if diam > t["max_diameter_in"]:
                issues.append(ValidationIssue(
                    "pipes", pid, "diameter", "OUT_OF_RANGE", "WARNING",
                    f"Diameter {diam}\" exceeds maximum {t['max_diameter_in']}\"", diam))

        # Length checks
        if length is not None:
            if length <= 0:
                issues.append(ValidationIssue(
                    "pipes", pid, "length", "INVALID_VALUE", "ERROR",
                    f"Pipe length is zero or negative: {length}", length))
            elif length < t["min_pipe_length_ft"]:
                issues.append(ValidationIssue(
                    "pipes", pid, "length", "SUSPICIOUS_VALUE", "WARNING",
                    f"Pipe length {length} ft is suspiciously short", length))
            elif length > t["max_pipe_length_ft"]:
                issues.append(ValidationIssue(
                    "pipes", pid, "length", "SUSPICIOUS_VALUE", "WARNING",
                    f"Pipe length {length} ft is suspiciously long", length))

        # Slope / adverse slope check
        if us_inv is not None and ds_inv is not None and length is not None and length > 0:
            slope = (us_inv - ds_inv) / length
            if slope < 0:
                severity = "ERROR" if slope < t["min_slope"] else "WARNING"
                issues.append(ValidationIssue(
                    "pipes", pid, "slope", "ADVERSE_SLOPE", severity,
                    f"Adverse slope detected: {slope:.6f} ft/ft "
                    f"(US={us_inv}, DS={ds_inv}, L={length})", slope))

        # Geometry check
        if rec.get("geometry") is None:
            issues.append(ValidationIssue(
                "pipes", pid, "geometry", "NULL_GEOMETRY", "ERROR",
                f"Pipe has null geometry"))

    return issues


def validate_junctions(records, thresholds=None):
    """
    Validate junction/manhole records.

    Checks:
    - Required fields populated
    - Invert and rim elevations within range
    - Depth calculation and reasonableness
    - Rim must be higher than invert
    """
    t = thresholds or DEFAULT_THRESHOLDS
    issues = []

    for rec in records:
        jid = rec.get("junction_id", "UNKNOWN")

        inv_elev = _safe_float(rec.get("invert_elev"))
        rim_elev = _safe_float(rec.get("rim_elev"))
        depth = _safe_float(rec.get("depth"))

        if inv_elev is None:
            issues.append(ValidationIssue(
                "junctions", jid, "invert_elev", "NULL_VALUE", "ERROR",
                f"Junction invert elevation is null or non-numeric"))

        if inv_elev is not None:
            if inv_elev < t["min_elevation_ft"] or inv_elev > t["max_elevation_ft"]:
                issues.append(ValidationIssue(
                    "junctions", jid, "invert_elev", "OUT_OF_RANGE", "WARNING",
                    f"Invert elevation {inv_elev} outside expected range", inv_elev))

        if rim_elev is not None:
            if rim_elev < t["min_elevation_ft"] or rim_elev > t["max_elevation_ft"]:
                issues.append(ValidationIssue(
                    "junctions", jid, "rim_elev", "OUT_OF_RANGE", "WARNING",
                    f"Rim elevation {rim_elev} outside expected range", rim_elev))

        # Rim should be above invert
        if inv_elev is not None and rim_elev is not None:
            if rim_elev < inv_elev:
                issues.append(ValidationIssue(
                    "junctions", jid, "rim_elev", "RIM_BELOW_INVERT", "ERROR",
                    f"Rim elevation ({rim_elev}) is below invert ({inv_elev})", rim_elev))

            # Depth check
            calc_depth = rim_elev - inv_elev
            if calc_depth < t["min_depth_ft"]:
                issues.append(ValidationIssue(
                    "junctions", jid, "depth", "SHALLOW_STRUCTURE", "WARNING",
                    f"Structure depth {calc_depth:.1f} ft is very shallow "
                    f"(rim={rim_elev}, inv={inv_elev})", calc_depth))
            if calc_depth > t["max_depth_ft"]:
                issues.append(ValidationIssue(
                    "junctions", jid, "depth", "DEEP_STRUCTURE", "WARNING",
                    f"Structure depth {calc_depth:.1f} ft is unusually deep "
                    f"(rim={rim_elev}, inv={inv_elev})", calc_depth))

        if rec.get("geometry") is None:
            issues.append(ValidationIssue(
                "junctions", jid, "geometry", "NULL_GEOMETRY", "ERROR",
                f"Junction has null geometry"))

    return issues


def validate_pumps(records, thresholds=None):
    """Validate pump/lift station records."""
    t = thresholds or DEFAULT_THRESHOLDS
    issues = []

    for rec in records:
        pid = rec.get("station_id", "UNKNOWN")

        inlet_inv = _safe_float(rec.get("inlet_invert"))
        if inlet_inv is None:
            issues.append(ValidationIssue(
                "pumps", pid, "inlet_invert", "NULL_VALUE", "WARNING",
                f"Pump inlet invert is null or non-numeric"))

        if rec.get("geometry") is None:
            issues.append(ValidationIssue(
                "pumps", pid, "geometry", "NULL_GEOMETRY", "ERROR",
                f"Pump has null geometry"))

    return issues


def validate_storage(records, thresholds=None):
    """Validate storage facility records."""
    t = thresholds or DEFAULT_THRESHOLDS
    issues = []

    for rec in records:
        sid = rec.get("tank_id", "UNKNOWN")

        base = _safe_float(rec.get("base_elev"))
        max_e = _safe_float(rec.get("max_elev"))

        if base is None:
            issues.append(ValidationIssue(
                "storage", sid, "base_elev", "NULL_VALUE", "WARNING",
                f"Storage base elevation is null or non-numeric"))

        if base is not None and max_e is not None:
            if max_e <= base:
                issues.append(ValidationIssue(
                    "storage", sid, "max_elev", "MAX_BELOW_BASE", "ERROR",
                    f"Max elevation ({max_e}) <= base elevation ({base})", max_e))

        if rec.get("geometry") is None:
            issues.append(ValidationIssue(
                "storage", sid, "geometry", "NULL_GEOMETRY", "ERROR",
                f"Storage feature has null geometry"))

    return issues


def validate_all(ingestion_results, thresholds=None):
    """
    Run all validation checks on ingestion results.

    Parameters
    ----------
    ingestion_results : dict
        Output from ingest.ingest_all().
    thresholds : dict, optional
        Custom thresholds to override defaults.

    Returns
    -------
    dict
        'issues': list[ValidationIssue]
        'summary': dict with counts by type and severity
    """
    all_issues = []

    validators = {
        "pipes": validate_pipes,
        "junctions": validate_junctions,
        "pumps": validate_pumps,
        "storage": validate_storage,
    }

    for ftype, validator in validators.items():
        if ftype in ingestion_results and ingestion_results[ftype].get("records"):
            issues = validator(ingestion_results[ftype]["records"], thresholds)
            all_issues.extend(issues)

    # Build summary
    summary = {
        "total_issues": len(all_issues),
        "by_severity": {},
        "by_type": {},
        "by_feature_type": {},
    }
    for issue in all_issues:
        summary["by_severity"][issue.severity] = summary["by_severity"].get(issue.severity, 0) + 1
        summary["by_type"][issue.issue_type] = summary["by_type"].get(issue.issue_type, 0) + 1
        summary["by_feature_type"][issue.feature_type] = summary["by_feature_type"].get(issue.feature_type, 0) + 1

    return {"issues": all_issues, "summary": summary}
