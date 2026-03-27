"""
fix_toolkit.py — Interactive fix engine with edit ledger.

Provides named fix strategies for each issue type, an append-only
edit ledger with group-based undo, and cascade logic for propagating
invert changes to neighboring pipes.
"""

import uuid
import time
from dataclasses import dataclass, field, asdict
from typing import List, Optional


# ── Default minimum slope (ft/ft) for fix calculations ──
MIN_SLOPE = 0.005


# ════════════════════════════════════════════════════════════
# EDIT LEDGER
# ════════════════════════════════════════════════════════════

@dataclass
class LedgerEntry:
    feature_id: str
    feature_type: str       # "pipe" or "junction"
    field: str              # "us_invert", "ds_invert", "invert_elev", etc.
    old_value: float
    new_value: float
    reason: str
    strategy: str
    group_id: str = ""
    entry_id: str = ""
    timestamp: float = 0.0

    def __post_init__(self):
        if not self.entry_id:
            self.entry_id = uuid.uuid4().hex[:8]
        if not self.timestamp:
            self.timestamp = time.time()


def apply_group(ledger: list, entries: List[LedgerEntry]) -> None:
    """Append a group of related entries to the ledger."""
    group_id = uuid.uuid4().hex[:8]
    for e in entries:
        e.group_id = group_id
    ledger.extend(entries)


def undo_last_group(ledger: list) -> List[LedgerEntry]:
    """Remove and return all entries from the most recent group."""
    if not ledger:
        return []
    last_group = ledger[-1].group_id
    removed = []
    while ledger and ledger[-1].group_id == last_group:
        removed.append(ledger.pop())
    return removed


def get_current_value(ledger: list, feature_id: str, field_name: str, original_value):
    """Walk the ledger forward, return the latest value for (feature_id, field)."""
    val = original_value
    for e in ledger:
        if e.feature_id == str(feature_id) and e.field == field_name:
            val = e.new_value
    return val


def get_all_edits(ledger: list) -> dict:
    """Return {(feature_id, field): new_value} for all current edits."""
    edits = {}
    for e in ledger:
        edits[(e.feature_id, e.field)] = e.new_value
    return edits


def ledger_summary(ledger: list) -> dict:
    """Return summary stats about the ledger."""
    groups = set(e.group_id for e in ledger)
    features = set(e.feature_id for e in ledger)
    return {
        "total_edits": len(ledger),
        "total_fixes": len(groups),
        "features_affected": len(features),
    }


# ════════════════════════════════════════════════════════════
# GRAPH HELPERS
# ════════════════════════════════════════════════════════════

def _get_node(G, nid):
    """Get node attributes dict."""
    if hasattr(G, 'nodes') and callable(getattr(G.nodes, '__getitem__', None)):
        return G.nodes[nid]
    return G._nodes.get(nid, {})


def _get_edge(G, u, v):
    """Get edge attributes dict."""
    return G.get_edge_data(u, v) or {}


def _safe_float(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _find_pipe_edge(G, pipe_id):
    """Find the (u, v, data) tuple for a given pipe_id."""
    for u, v, data in G.edges(data=True):
        if str(data.get("pipe_id", "")) == str(pipe_id):
            return u, v, data
    return None, None, None


# ════════════════════════════════════════════════════════════
# FIX STRATEGIES
# ════════════════════════════════════════════════════════════

# ── ADVERSE SLOPE ──

def flip_inverts(issue, G, ledger):
    """Swap upstream and downstream inverts (data entry error fix)."""
    u, v, data = _find_pipe_edge(G, issue.feature_id)
    if data is None:
        return []

    pid = str(issue.feature_id)
    us = get_current_value(ledger, pid, "us_invert", _safe_float(data.get("us_invert")))
    ds = get_current_value(ledger, pid, "ds_invert", _safe_float(data.get("ds_invert")))

    if us is None or ds is None:
        return []

    return [
        LedgerEntry(pid, "pipe", "us_invert", us, ds,
                    "Flip inverts (swap US/DS)", "flip_inverts"),
        LedgerEntry(pid, "pipe", "ds_invert", ds, us,
                    "Flip inverts (swap US/DS)", "flip_inverts"),
    ]


def linear_interpolate(issue, G, ledger):
    """Interpolate inverts between upstream pipe's DS invert and downstream pipe's US invert."""
    u, v, data = _find_pipe_edge(G, issue.feature_id)
    if data is None:
        return []

    pid = str(issue.feature_id)

    # Find upstream elevation (predecessor pipe's ds_invert or junction invert)
    upstream_elev = None
    for pred in G.predecessors(u):
        pred_edge = _get_edge(G, pred, u)
        upstream_elev = get_current_value(
            ledger, str(pred_edge.get("pipe_id", "")), "ds_invert",
            _safe_float(pred_edge.get("ds_invert"))
        )
        if upstream_elev is not None:
            break
    if upstream_elev is None:
        u_node = _get_node(G, u)
        upstream_elev = get_current_value(
            ledger, str(u), "invert_elev",
            _safe_float(u_node.get("invert_elev"))
        )

    # Find downstream elevation (successor pipe's us_invert or junction invert)
    downstream_elev = None
    for succ in G.successors(v):
        succ_edge = _get_edge(G, v, succ)
        downstream_elev = get_current_value(
            ledger, str(succ_edge.get("pipe_id", "")), "us_invert",
            _safe_float(succ_edge.get("us_invert"))
        )
        if downstream_elev is not None:
            break
    if downstream_elev is None:
        v_node = _get_node(G, v)
        downstream_elev = get_current_value(
            ledger, str(v), "invert_elev",
            _safe_float(v_node.get("invert_elev"))
        )

    if upstream_elev is None or downstream_elev is None:
        return []

    us_old = get_current_value(ledger, pid, "us_invert", _safe_float(data.get("us_invert")))
    ds_old = get_current_value(ledger, pid, "ds_invert", _safe_float(data.get("ds_invert")))

    new_us = round(upstream_elev, 3)
    new_ds = round(downstream_elev, 3)

    # If still adverse after interpolation, force minimum slope
    if new_ds >= new_us:
        length = _safe_float(data.get("length")) or 100
        new_ds = round(new_us - MIN_SLOPE * length, 3)

    entries = []
    if us_old != new_us:
        entries.append(LedgerEntry(pid, "pipe", "us_invert", us_old, new_us,
                                   "Linear interpolate from neighbors", "linear_interpolate"))
    if ds_old != new_ds:
        entries.append(LedgerEntry(pid, "pipe", "ds_invert", ds_old, new_ds,
                                   "Linear interpolate from neighbors", "linear_interpolate"))
    return entries


def min_slope_from_upstream(issue, G, ledger):
    """Set DS invert using min slope from US invert, cascade downstream."""
    u, v, data = _find_pipe_edge(G, issue.feature_id)
    if data is None:
        return []

    pid = str(issue.feature_id)
    us_inv = get_current_value(ledger, pid, "us_invert", _safe_float(data.get("us_invert")))
    ds_inv_old = get_current_value(ledger, pid, "ds_invert", _safe_float(data.get("ds_invert")))
    length = _safe_float(data.get("length")) or 100

    if us_inv is None:
        return []

    new_ds = round(us_inv - MIN_SLOPE * length, 3)
    entries = []
    if ds_inv_old != new_ds:
        entries.append(LedgerEntry(pid, "pipe", "ds_invert", ds_inv_old, new_ds,
                                   "Min slope from upstream", "min_slope_from_upstream"))

    # Cascade downstream
    cascade = _cascade_downstream(v, new_ds, G, ledger + entries)
    entries.extend(cascade)
    return entries


def min_slope_to_downstream(issue, G, ledger):
    """Set US invert using min slope to DS invert, cascade upstream."""
    u, v, data = _find_pipe_edge(G, issue.feature_id)
    if data is None:
        return []

    pid = str(issue.feature_id)
    ds_inv = get_current_value(ledger, pid, "ds_invert", _safe_float(data.get("ds_invert")))
    us_inv_old = get_current_value(ledger, pid, "us_invert", _safe_float(data.get("us_invert")))
    length = _safe_float(data.get("length")) or 100

    if ds_inv is None:
        return []

    new_us = round(ds_inv + MIN_SLOPE * length, 3)
    entries = []
    if us_inv_old != new_us:
        entries.append(LedgerEntry(pid, "pipe", "us_invert", us_inv_old, new_us,
                                   "Min slope to downstream", "min_slope_to_downstream"))

    # Cascade upstream
    cascade = _cascade_upstream(u, new_us, G, ledger + entries)
    entries.extend(cascade)
    return entries


def _cascade_downstream(start_node, elev_at_node, G, ledger, max_depth=20):
    """Propagate invert changes downstream using minimum slope."""
    entries = []
    queue = [(start_node, elev_at_node, 0)]
    visited = set()

    while queue:
        node, node_elev, depth = queue.pop(0)
        if depth >= max_depth or node in visited:
            continue
        visited.add(node)

        for succ in G.successors(node):
            edge = _get_edge(G, node, succ)
            pid = str(edge.get("pipe_id", ""))
            length = _safe_float(edge.get("length")) or 100

            current_us = get_current_value(ledger + entries, pid, "us_invert",
                                           _safe_float(edge.get("us_invert")))
            current_ds = get_current_value(ledger + entries, pid, "ds_invert",
                                           _safe_float(edge.get("ds_invert")))

            # Adjust US invert to match the incoming elevation
            new_us = round(node_elev, 3)
            if current_us is not None and current_us != new_us:
                entries.append(LedgerEntry(pid, "pipe", "us_invert", current_us, new_us,
                                           f"Cascade: match upstream node", "min_slope_from_upstream"))

            # Check if DS invert needs adjustment
            if current_ds is not None and current_ds >= new_us:
                new_ds = round(new_us - MIN_SLOPE * length, 3)
                entries.append(LedgerEntry(pid, "pipe", "ds_invert", current_ds, new_ds,
                                           f"Cascade: min slope from {pid} US", "min_slope_from_upstream"))
                queue.append((succ, new_ds, depth + 1))

    return entries


def _cascade_upstream(start_node, elev_at_node, G, ledger, max_depth=20):
    """Propagate invert changes upstream using minimum slope."""
    entries = []
    queue = [(start_node, elev_at_node, 0)]
    visited = set()

    while queue:
        node, node_elev, depth = queue.pop(0)
        if depth >= max_depth or node in visited:
            continue
        visited.add(node)

        for pred in G.predecessors(node):
            edge = _get_edge(G, pred, node)
            pid = str(edge.get("pipe_id", ""))
            length = _safe_float(edge.get("length")) or 100

            current_ds = get_current_value(ledger + entries, pid, "ds_invert",
                                           _safe_float(edge.get("ds_invert")))
            current_us = get_current_value(ledger + entries, pid, "us_invert",
                                           _safe_float(edge.get("us_invert")))

            # Adjust DS invert to match the outgoing elevation
            new_ds = round(node_elev, 3)
            if current_ds is not None and current_ds != new_ds:
                entries.append(LedgerEntry(pid, "pipe", "ds_invert", current_ds, new_ds,
                                           f"Cascade: match downstream node", "min_slope_to_downstream"))

            # Check if US invert needs adjustment
            if current_us is not None and current_us <= new_ds:
                new_us = round(new_ds + MIN_SLOPE * length, 3)
                entries.append(LedgerEntry(pid, "pipe", "us_invert", current_us, new_us,
                                           f"Cascade: min slope to {pid} DS", "min_slope_to_downstream"))
                queue.append((pred, new_us, depth + 1))

    return entries


# ── INVERT MISMATCH ──

def adjust_pipe_to_junction(issue, G, ledger):
    """Adjust the pipe's invert at the junction to match the junction's invert_elev."""
    details = issue.details or {}
    pipe_id = str(details.get("pipe_id", issue.feature_id))
    node_id = str(details.get("node_id", ""))
    end = details.get("end", "")  # "upstream" or "downstream"

    u, v, data = _find_pipe_edge(G, pipe_id)
    if data is None:
        return []

    # Get junction invert
    junc_inv = None
    if node_id:
        node_data = _get_node(G, node_id)
        junc_inv = get_current_value(ledger, node_id, "invert_elev",
                                     _safe_float(node_data.get("invert_elev")))

    if junc_inv is None:
        return []

    # Determine which end of the pipe connects to this junction
    if end == "downstream" or str(v) == node_id:
        field = "ds_invert"
    else:
        field = "us_invert"

    old_val = get_current_value(ledger, pipe_id, field, _safe_float(data.get(field)))
    if old_val == junc_inv:
        return []

    return [LedgerEntry(pipe_id, "pipe", field, old_val, round(junc_inv, 3),
                        f"Adjust pipe to match junction {node_id}", "adjust_pipe_to_junction")]


def adjust_junction_to_pipe(issue, G, ledger):
    """Adjust the junction's invert_elev to match the pipe's invert at that end."""
    details = issue.details or {}
    pipe_id = str(details.get("pipe_id", issue.feature_id))
    node_id = str(details.get("node_id", ""))
    end = details.get("end", "")

    u, v, data = _find_pipe_edge(G, pipe_id)
    if data is None:
        return []

    # Get the pipe's invert at the junction end
    if end == "downstream" or str(v) == node_id:
        pipe_inv = get_current_value(ledger, pipe_id, "ds_invert",
                                     _safe_float(data.get("ds_invert")))
    else:
        pipe_inv = get_current_value(ledger, pipe_id, "us_invert",
                                     _safe_float(data.get("us_invert")))

    if pipe_inv is None or not node_id:
        return []

    node_data = _get_node(G, node_id)
    old_inv = get_current_value(ledger, node_id, "invert_elev",
                                _safe_float(node_data.get("invert_elev")))
    if old_inv == pipe_inv:
        return []

    return [LedgerEntry(node_id, "junction", "invert_elev", old_inv, round(pipe_inv, 3),
                        f"Adjust junction to match pipe {pipe_id}", "adjust_junction_to_pipe")]


# ── NULL INVERT (pipes) ──

def null_invert_from_junction(issue, G, ledger):
    """Set missing pipe invert(s) from the connected junction's invert_elev."""
    u, v, data = _find_pipe_edge(G, issue.feature_id)
    if data is None:
        return []

    pid = str(issue.feature_id)
    details = issue.details or {}
    us_inv = get_current_value(ledger, pid, "us_invert", _safe_float(details.get("us_invert")))
    ds_inv = get_current_value(ledger, pid, "ds_invert", _safe_float(details.get("ds_invert")))
    entries = []

    if us_inv is None:
        u_node = _get_node(G, u)
        junc_inv = get_current_value(ledger, str(u), "invert_elev",
                                     _safe_float(u_node.get("invert_elev")))
        if junc_inv is not None:
            entries.append(LedgerEntry(pid, "pipe", "us_invert", None, round(junc_inv, 3),
                                       f"Set from junction {u} invert", "null_invert_from_junction"))

    if ds_inv is None:
        v_node = _get_node(G, v)
        junc_inv = get_current_value(ledger, str(v), "invert_elev",
                                     _safe_float(v_node.get("invert_elev")))
        if junc_inv is not None:
            entries.append(LedgerEntry(pid, "pipe", "ds_invert", None, round(junc_inv, 3),
                                       f"Set from junction {v} invert", "null_invert_from_junction"))

    return entries


def null_invert_from_neighbor_pipe(issue, G, ledger):
    """Set missing pipe invert(s) from the connected upstream/downstream pipe."""
    u, v, data = _find_pipe_edge(G, issue.feature_id)
    if data is None:
        return []

    pid = str(issue.feature_id)
    details = issue.details or {}
    us_inv = get_current_value(ledger, pid, "us_invert", _safe_float(details.get("us_invert")))
    ds_inv = get_current_value(ledger, pid, "ds_invert", _safe_float(details.get("ds_invert")))
    entries = []

    # Fill US invert from upstream pipe's DS invert
    if us_inv is None:
        for pred in G.predecessors(u):
            pred_edge = _get_edge(G, pred, u)
            pred_pid = str(pred_edge.get("pipe_id", ""))
            pred_ds = get_current_value(ledger, pred_pid, "ds_invert",
                                        _safe_float(pred_edge.get("ds_invert")))
            if pred_ds is not None:
                entries.append(LedgerEntry(pid, "pipe", "us_invert", None, round(pred_ds, 3),
                                           f"Set from upstream pipe {pred_pid} DS invert",
                                           "null_invert_from_neighbor_pipe"))
                break

    # Fill DS invert from downstream pipe's US invert
    if ds_inv is None:
        for succ in G.successors(v):
            succ_edge = _get_edge(G, v, succ)
            succ_pid = str(succ_edge.get("pipe_id", ""))
            succ_us = get_current_value(ledger, succ_pid, "us_invert",
                                        _safe_float(succ_edge.get("us_invert")))
            if succ_us is not None:
                entries.append(LedgerEntry(pid, "pipe", "ds_invert", None, round(succ_us, 3),
                                           f"Set from downstream pipe {succ_pid} US invert",
                                           "null_invert_from_neighbor_pipe"))
                break

    return entries


def null_invert_interpolate(issue, G, ledger):
    """Interpolate missing invert(s) using the known invert + min slope."""
    u, v, data = _find_pipe_edge(G, issue.feature_id)
    if data is None:
        return []

    pid = str(issue.feature_id)
    details = issue.details or {}
    us_inv = get_current_value(ledger, pid, "us_invert", _safe_float(details.get("us_invert")))
    ds_inv = get_current_value(ledger, pid, "ds_invert", _safe_float(details.get("ds_invert")))
    length = _safe_float(data.get("length")) or 100
    entries = []

    if us_inv is not None and ds_inv is None:
        # Have US, missing DS — project downstream using min slope
        new_ds = round(us_inv - MIN_SLOPE * length, 3)
        entries.append(LedgerEntry(pid, "pipe", "ds_invert", None, new_ds,
                                   f"Min slope ({MIN_SLOPE}) from US invert",
                                   "null_invert_interpolate"))

    elif ds_inv is not None and us_inv is None:
        # Have DS, missing US — project upstream using min slope
        new_us = round(ds_inv + MIN_SLOPE * length, 3)
        entries.append(LedgerEntry(pid, "pipe", "us_invert", None, new_us,
                                   f"Min slope ({MIN_SLOPE}) to DS invert",
                                   "null_invert_interpolate"))

    elif us_inv is None and ds_inv is None:
        # Both missing — try junctions first, then neighbor pipes
        u_node = _get_node(G, u)
        v_node = _get_node(G, v)
        u_inv = get_current_value(ledger, str(u), "invert_elev",
                                  _safe_float(u_node.get("invert_elev")))
        v_inv = get_current_value(ledger, str(v), "invert_elev",
                                  _safe_float(v_node.get("invert_elev")))

        if u_inv is not None:
            entries.append(LedgerEntry(pid, "pipe", "us_invert", None, round(u_inv, 3),
                                       f"Set from junction {u} invert", "null_invert_interpolate"))
            new_ds = round(u_inv - MIN_SLOPE * length, 3)
            entries.append(LedgerEntry(pid, "pipe", "ds_invert", None, new_ds,
                                       f"Min slope ({MIN_SLOPE}) from junction {u}",
                                       "null_invert_interpolate"))
        elif v_inv is not None:
            new_us = round(v_inv + MIN_SLOPE * length, 3)
            entries.append(LedgerEntry(pid, "pipe", "us_invert", None, new_us,
                                       f"Min slope ({MIN_SLOPE}) to junction {v}",
                                       "null_invert_interpolate"))
            entries.append(LedgerEntry(pid, "pipe", "ds_invert", None, round(v_inv, 3),
                                       f"Set from junction {v} invert", "null_invert_interpolate"))

    return entries


# ── NULL JUNCTION INVERT ──

def junction_invert_from_lowest_pipe(issue, G, ledger):
    """Set junction invert_elev to the lowest connected pipe invert."""
    # feature_id could be a junction ID or pipe ID — check details
    details = issue.details or {}
    # For NULL_INVERT the feature is a pipe, but we may need to handle
    # junction fixes too. This strategy works on junction nodes directly.
    node_id = str(details.get("us_node", "") or details.get("ds_node", "") or issue.feature_id)

    node_data = _get_node(G, node_id)
    if not node_data:
        return []

    current_inv = get_current_value(ledger, node_id, "invert_elev",
                                    _safe_float(node_data.get("invert_elev")))

    # Collect all connected pipe inverts at this node
    inverts = []

    # Incoming pipes — their DS invert connects here
    for pred in G.predecessors(node_id):
        edge = _get_edge(G, pred, node_id)
        pid = str(edge.get("pipe_id", ""))
        ds = get_current_value(ledger, pid, "ds_invert", _safe_float(edge.get("ds_invert")))
        if ds is not None:
            inverts.append(ds)

    # Outgoing pipes — their US invert connects here
    for succ in G.successors(node_id):
        edge = _get_edge(G, node_id, succ)
        pid = str(edge.get("pipe_id", ""))
        us = get_current_value(ledger, pid, "us_invert", _safe_float(edge.get("us_invert")))
        if us is not None:
            inverts.append(us)

    if not inverts:
        return []

    lowest = round(min(inverts), 3)
    if current_inv == lowest:
        return []

    return [LedgerEntry(node_id, "junction", "invert_elev", current_inv, lowest,
                        f"Set to lowest connected pipe invert ({lowest})",
                        "junction_invert_from_lowest_pipe")]


# ════════════════════════════════════════════════════════════
# STRATEGY REGISTRY
# ════════════════════════════════════════════════════════════

STRATEGIES = {
    "ADVERSE_SLOPE": [
        ("flip_inverts", "Flip Inverts", flip_inverts),
        ("linear_interpolate", "Linear Interpolate", linear_interpolate),
        ("min_slope_from_upstream", "Min Slope from Upstream", min_slope_from_upstream),
        ("min_slope_to_downstream", "Min Slope to Downstream", min_slope_to_downstream),
    ],
    "INVERT_MISMATCH": [
        ("adjust_pipe_to_junction", "Adjust Pipe to Match Junction", adjust_pipe_to_junction),
        ("adjust_junction_to_pipe", "Adjust Junction to Match Pipe", adjust_junction_to_pipe),
    ],
    "NULL_INVERT": [
        ("null_invert_from_junction", "From Connected Junction", null_invert_from_junction),
        ("null_invert_from_neighbor_pipe", "From Neighbor Pipe", null_invert_from_neighbor_pipe),
        ("null_invert_interpolate", "Interpolate (Min Slope)", null_invert_interpolate),
        ("junction_invert_from_lowest_pipe", "Set Junction to Lowest Pipe", junction_invert_from_lowest_pipe),
    ],
}


def get_strategies(issue_type: str):
    """Return list of (key, display_name, fn) tuples for an issue type."""
    return STRATEGIES.get(issue_type, [])


def compute_fix(strategy_key: str, issue, G, ledger) -> List[LedgerEntry]:
    """Look up a strategy by key and compute proposed edits."""
    for issue_type, strategies in STRATEGIES.items():
        for key, name, fn in strategies:
            if key == strategy_key:
                return fn(issue, G, ledger)
    return []
