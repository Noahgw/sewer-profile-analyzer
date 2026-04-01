"""
profile_analyzer.py — Analyze sewer network profiles for issues.

Walks the directed graph from upstream to downstream, checking:
1. Adverse slopes (pipe flows uphill)
2. Invert mismatches at junctions
3. Diameter decreases along flow path (without pump)
4. Rim-to-invert depth anomalies
5. Missing/null critical data

Returns a structured report of all issues found.
"""


class ProfileIssue:
    """Represents a single profile issue detected during analysis."""

    # Issue types
    ADVERSE_SLOPE = "ADVERSE_SLOPE"
    INVERT_MISMATCH = "INVERT_MISMATCH"
    DIAMETER_DECREASE = "DIAMETER_DECREASE"
    SHALLOW_STRUCTURE = "SHALLOW_STRUCTURE"
    DEEP_STRUCTURE = "DEEP_STRUCTURE"
    NULL_INVERT = "NULL_INVERT"
    NULL_DIAMETER = "NULL_DIAMETER"
    DEAD_END = "DEAD_END"
    ORPHAN_NODE = "ORPHAN_NODE"

    # Severity
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

    def __init__(self, issue_type, severity, feature_id, location_desc,
                 message, details=None, coordinates=None):
        self.issue_type = issue_type
        self.severity = severity
        self.feature_id = feature_id
        self.location_desc = location_desc
        self.message = message
        self.details = details or {}
        self.coordinates = coordinates

    def to_dict(self):
        return {
            "issue_type": self.issue_type,
            "severity": self.severity,
            "feature_id": self.feature_id,
            "location": self.location_desc,
            "message": self.message,
            "details": self.details,
            "coordinates": self.coordinates,
        }

    def __repr__(self):
        return f"[{self.severity}] {self.issue_type} at {self.feature_id}: {self.message}"


# Default analysis thresholds (metric)
DEFAULT_PROFILE_THRESHOLDS = {
    "invert_mismatch_tolerance_m": 0.01,  # allowable difference at junctions (metres)
    "min_structure_depth_m": 0.6,         # ~2 ft
    "max_structure_depth_m": 10.0,        # ~30 ft
    "adverse_slope_severity_threshold": -0.01,  # steeper adverse = HIGH severity
}


def _safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _get_node_attr(G, node_id, attr, has_networkx=False):
    """Get a node attribute from the graph."""
    if has_networkx:
        return G.nodes[node_id].get(attr)
    return G._nodes[node_id].get(attr)


def _get_edge_attr(G, u, v, has_networkx=False):
    """Get edge attributes from the graph."""
    if has_networkx:
        return G.get_edge_data(u, v) or {}
    return G.get_edge_data(u, v) or {}


def _is_forcemain(data):
    """Check if a pipe is a force main based on its attributes."""
    fm = data.get("force_main")
    if fm is None:
        return False
    if isinstance(fm, bool):
        return fm
    if isinstance(fm, str):
        return fm.strip().upper() in ("TRUE", "YES", "1", "Y", "FM")
    try:
        return bool(fm)
    except (ValueError, TypeError):
        return False


def analyze_adverse_slopes(G, has_networkx=False, thresholds=None):
    """
    Check every pipe for adverse slope (downstream invert higher than upstream).
    Skips force mains — adverse slope is expected for pressurized pipes.

    Returns list of ProfileIssue.
    """
    t = thresholds or DEFAULT_PROFILE_THRESHOLDS
    issues = []

    for u, v, data in (G.edges(data=True)):
        pid = data.get("pipe_id", f"{u}->{v}")
        us_inv = _safe_float(data.get("us_invert"))
        ds_inv = _safe_float(data.get("ds_invert"))
        length = _safe_float(data.get("length"))

        if us_inv is None or ds_inv is None:
            issues.append(ProfileIssue(
                ProfileIssue.NULL_INVERT, ProfileIssue.HIGH, pid,
                f"Pipe {pid} ({u} -> {v})",
                f"Missing invert elevation data (US={us_inv}, DS={ds_inv})",
                {"us_node": u, "ds_node": v, "us_invert": us_inv, "ds_invert": ds_inv},
            ))
            continue

        # Skip adverse slope check for force mains
        if _is_forcemain(data):
            continue

        if ds_inv > us_inv:
            slope = None
            if length and length > 0:
                slope = (us_inv - ds_inv) / length

            severity = ProfileIssue.HIGH if (slope and slope < t["adverse_slope_severity_threshold"]) else ProfileIssue.MEDIUM

            slope_str = f", slope = {slope:.6f} m/m" if slope else ""
            issues.append(ProfileIssue(
                ProfileIssue.ADVERSE_SLOPE, severity, pid,
                f"Pipe {pid} ({u} -> {v})",
                f"Adverse slope: US invert {us_inv} < DS invert {ds_inv} "
                f"(rise = {ds_inv - us_inv:.2f} m{slope_str})",
                {"us_node": u, "ds_node": v, "us_invert": us_inv,
                 "ds_invert": ds_inv, "slope": slope, "length": length},
            ))

    return issues


def analyze_invert_mismatches(G, has_networkx=False, thresholds=None):
    """
    Check invert continuity at junctions.

    At each junction, the incoming pipe's ds_invert should approximately equal
    the junction's invert_elev, and the outgoing pipe's us_invert should also match.
    """
    t = thresholds or DEFAULT_PROFILE_THRESHOLDS
    tol = t.get("invert_mismatch_tolerance_m", t.get("invert_mismatch_tolerance_ft", 0.01))
    issues = []

    for nid in G.nodes:
        node_attrs = G.nodes[nid] if has_networkx else G._nodes[nid]
        node_type = node_attrs.get("node_type", "junction")
        if node_type not in ("junction",):
            continue

        junc_inv = _safe_float(node_attrs.get("invert_elev"))
        if junc_inv is None:
            continue

        # Check incoming pipes
        for pred in G.predecessors(nid):
            edge_data = _get_edge_attr(G, pred, nid, has_networkx)
            ds_inv = _safe_float(edge_data.get("ds_invert"))
            pid = edge_data.get("pipe_id", f"{pred}->{nid}")

            if ds_inv is not None and abs(ds_inv - junc_inv) > tol:
                issues.append(ProfileIssue(
                    ProfileIssue.INVERT_MISMATCH, ProfileIssue.MEDIUM, nid,
                    f"Junction {nid} (incoming pipe {pid})",
                    f"Incoming pipe ds_invert ({ds_inv}) != junction invert ({junc_inv}), "
                    f"diff = {abs(ds_inv - junc_inv):.3f} m",
                    {"junction_id": nid, "pipe_id": pid, "pipe_ds_invert": ds_inv,
                     "junction_invert": junc_inv, "difference": abs(ds_inv - junc_inv)},
                ))

        # Check outgoing pipes
        for succ in G.successors(nid):
            edge_data = _get_edge_attr(G, nid, succ, has_networkx)
            us_inv = _safe_float(edge_data.get("us_invert"))
            pid = edge_data.get("pipe_id", f"{nid}->{succ}")

            if us_inv is not None and abs(us_inv - junc_inv) > tol:
                issues.append(ProfileIssue(
                    ProfileIssue.INVERT_MISMATCH, ProfileIssue.MEDIUM, nid,
                    f"Junction {nid} (outgoing pipe {pid})",
                    f"Outgoing pipe us_invert ({us_inv}) != junction invert ({junc_inv}), "
                    f"diff = {abs(us_inv - junc_inv):.3f} m",
                    {"junction_id": nid, "pipe_id": pid, "pipe_us_invert": us_inv,
                     "junction_invert": junc_inv, "difference": abs(us_inv - junc_inv)},
                ))

    return issues


def analyze_diameter_continuity(G, has_networkx=False):
    """
    Walk downstream and flag locations where pipe diameter decreases
    without a pump station in between.

    A decrease in diameter along the flow path (without a pump) typically
    indicates a data error or design issue — flow accumulates downstream,
    so pipes should generally stay the same size or get larger.
    """
    issues = []

    for nid in G.nodes:
        node_attrs = G.nodes[nid] if has_networkx else G._nodes[nid]
        node_type = node_attrs.get("node_type", "junction")

        # Skip pump/storage nodes — diameter changes are expected there
        if node_type in ("pump", "storage"):
            continue

        # Get incoming pipe diameters
        incoming_diameters = []
        for pred in G.predecessors(nid):
            edge_data = _get_edge_attr(G, pred, nid, has_networkx)
            d = _safe_float(edge_data.get("diameter"))
            if d is not None:
                incoming_diameters.append((edge_data.get("pipe_id", f"{pred}->{nid}"), d))

        if not incoming_diameters:
            continue

        max_incoming = max(incoming_diameters, key=lambda x: x[1])

        # Check outgoing pipes
        for succ in G.successors(nid):
            edge_data = _get_edge_attr(G, nid, succ, has_networkx)
            out_d = _safe_float(edge_data.get("diameter"))
            out_pid = edge_data.get("pipe_id", f"{nid}->{succ}")

            if out_d is not None and out_d < max_incoming[1]:
                issues.append(ProfileIssue(
                    ProfileIssue.DIAMETER_DECREASE, ProfileIssue.MEDIUM, nid,
                    f"Junction {nid} (pipe {max_incoming[0]} -> {out_pid})",
                    f"Diameter decreases from {max_incoming[1]}\" to {out_d}\" at {nid} "
                    f"without a pump station",
                    {"junction_id": nid, "upstream_pipe": max_incoming[0],
                     "upstream_diameter": max_incoming[1], "downstream_pipe": out_pid,
                     "downstream_diameter": out_d},
                ))

    return issues


def analyze_structure_depths(G, has_networkx=False, thresholds=None):
    """Check junction rim-to-invert depths for anomalies."""
    t = thresholds or DEFAULT_PROFILE_THRESHOLDS
    min_depth = t.get("min_structure_depth_m", t.get("min_structure_depth_ft", 0.6))
    max_depth = t.get("max_structure_depth_m", t.get("max_structure_depth_ft", 10.0))
    issues = []

    for nid in G.nodes:
        node_attrs = G.nodes[nid] if has_networkx else G._nodes[nid]
        if node_attrs.get("node_type") != "junction":
            continue

        rim = _safe_float(node_attrs.get("rim_elev"))
        inv = _safe_float(node_attrs.get("invert_elev"))

        if rim is None or inv is None:
            continue

        depth = rim - inv

        if depth < min_depth:
            issues.append(ProfileIssue(
                ProfileIssue.SHALLOW_STRUCTURE, ProfileIssue.LOW, nid,
                f"Junction {nid}",
                f"Structure depth {depth:.1f} m is below minimum {min_depth} m "
                f"(rim={rim}, inv={inv})",
                {"junction_id": nid, "rim": rim, "invert": inv, "depth": depth},
                coordinates=node_attrs.get("coords"),
            ))

        if depth > max_depth:
            issues.append(ProfileIssue(
                ProfileIssue.DEEP_STRUCTURE, ProfileIssue.LOW, nid,
                f"Junction {nid}",
                f"Structure depth {depth:.1f} m exceeds maximum {max_depth} m "
                f"(rim={rim}, inv={inv})",
                {"junction_id": nid, "rim": rim, "invert": inv, "depth": depth},
                coordinates=node_attrs.get("coords"),
            ))

    return issues


def analyze_null_diameters(G, has_networkx=False):
    """Flag pipes with null/missing diameter."""
    issues = []
    for u, v, data in G.edges(data=True):
        pid = data.get("pipe_id", f"{u}->{v}")
        if _safe_float(data.get("diameter")) is None:
            issues.append(ProfileIssue(
                ProfileIssue.NULL_DIAMETER, ProfileIssue.HIGH, pid,
                f"Pipe {pid} ({u} -> {v})",
                f"Pipe diameter is null or missing",
                {"us_node": u, "ds_node": v},
            ))
    return issues


def analyze_connectivity(network_result):
    """
    Convert connectivity warnings from network builder into ProfileIssues.
    """
    issues = []
    stats = network_result.get("stats", {})

    for nid in stats.get("orphan_nodes", []):
        issues.append(ProfileIssue(
            ProfileIssue.ORPHAN_NODE, ProfileIssue.MEDIUM, nid,
            f"Node {nid}",
            f"Node has no pipe connections (orphan)",
        ))

    for nid in stats.get("dead_end_nodes", []):
        issues.append(ProfileIssue(
            ProfileIssue.DEAD_END, ProfileIssue.LOW, nid,
            f"Node {nid}",
            f"Node has incoming pipe(s) but no outgoing pipe (dead end)",
        ))

    return issues


def trace_profile(G, start_node, has_networkx=False):
    """
    Trace the flow path from a starting node downstream to the terminus.

    Follows the first outgoing edge at each node (for simple linear paths).
    For branching networks, follows the edge with the largest downstream diameter.

    Returns
    -------
    list of dicts: sequence of nodes and connecting pipes along the profile.
    """
    path = []
    visited = set()
    current = start_node

    while current and current not in visited:
        visited.add(current)
        node_attrs = G.nodes[current] if has_networkx else G._nodes[current]

        entry = {
            "node_id": current,
            "node_type": node_attrs.get("node_type", "unknown"),
            "rim_elev": _safe_float(node_attrs.get("rim_elev")),
            "invert_elev": _safe_float(node_attrs.get("invert_elev")),
            "coords": node_attrs.get("coords"),
            "pipe_to_next": None,
        }

        successors = G.successors(current)
        if not successors:
            path.append(entry)
            break

        # Pick the main downstream path (largest diameter or first available)
        best_succ = None
        best_diam = -1
        for s in successors:
            edge_data = _get_edge_attr(G, current, s, has_networkx)
            d = _safe_float(edge_data.get("diameter")) or 0
            if d > best_diam:
                best_diam = d
                best_succ = s

        if best_succ is None:
            best_succ = successors[0]

        edge_data = _get_edge_attr(G, current, best_succ, has_networkx)
        entry["pipe_to_next"] = {
            "pipe_id": edge_data.get("pipe_id"),
            "us_invert": _safe_float(edge_data.get("us_invert")),
            "ds_invert": _safe_float(edge_data.get("ds_invert")),
            "diameter": _safe_float(edge_data.get("diameter")),
            "length": _safe_float(edge_data.get("length")),
            "material": edge_data.get("material"),
            "to_node": best_succ,
        }

        path.append(entry)
        current = best_succ

    return path


def run_full_analysis(network_result, thresholds=None):
    """
    Run all profile analysis checks on a built network.

    Parameters
    ----------
    network_result : dict
        Output from network_builder.build_network().
    thresholds : dict, optional
        Override default thresholds.

    Returns
    -------
    dict:
        'issues': list[ProfileIssue]
        'summary': dict with counts by type and severity
    """
    G = network_result["graph"]
    has_nx = hasattr(G, 'degree')  # networkx graphs have .degree

    all_issues = []

    # Run all analyzers
    all_issues.extend(analyze_adverse_slopes(G, has_nx, thresholds))
    all_issues.extend(analyze_invert_mismatches(G, has_nx, thresholds))
    all_issues.extend(analyze_diameter_continuity(G, has_nx))
    all_issues.extend(analyze_structure_depths(G, has_nx, thresholds))
    all_issues.extend(analyze_null_diameters(G, has_nx))
    all_issues.extend(analyze_connectivity(network_result))

    # Build summary
    summary = {
        "total_issues": len(all_issues),
        "by_type": {},
        "by_severity": {},
    }
    for issue in all_issues:
        summary["by_type"][issue.issue_type] = summary["by_type"].get(issue.issue_type, 0) + 1
        summary["by_severity"][issue.severity] = summary["by_severity"].get(issue.severity, 0) + 1

    return {
        "issues": all_issues,
        "summary": summary,
        "network_stats": network_result.get("stats", {}),
    }
